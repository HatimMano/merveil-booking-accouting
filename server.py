"""
Flask HTTP server for Google Cloud Run.

Triggered via HTTP POST (Cloud Scheduler or any HTTP client).

Expected request body (JSON):
    {
        "folder_id": "1abc...xyz",   // Google Drive folder containing the source files
        "date":      "2025-10-03",   // Processing date (YYYY-MM-DD) or "AUTO"
        "ota":       "booking"       // "booking" or "airbnb"
    }

On success it returns:
    {
        "status": "ok",
        "reservations": 83,
        "warnings": 8,
        "blocking": 0,
        "balance_ok": true,
        "pennylane_entry_id": 12345       // booking — unique PennyLane entry id
        // OR
        "pennylane_batches_posted": 40    // airbnb — number of payout batches posted
    }
"""

import logging
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path

from flask import Flask, jsonify, request

sys.path.insert(0, str(Path(__file__).parent))

from config.mapping_loader import load_mapping, load_airbnb_mapping
from config.settings import (
    DRIVE_FOLDER_BOOKING,
    DRIVE_FOLDER_AIRBNB,
    AIRBNB_JOURNAL_CODE,
    AIRBNB_ACCOUNT_BANK,
    AIRBNB_ACCOUNT_CLIENT,
    AIRBNB_ACCOUNT_SUPPLIER,
    AIRBNB_ACCOUNT_CANCELLATION_FEE,
)
from parsers.booking import BookingExcelParser
from parsers.airbnb import AirbnbParser
from accounting.entries import generate_entries
from drive.client import DriveClient
from pennylane.client import PennyLaneClient
from validators.anomalies import (
    Severity,
    check_balance,
    check_duplicate_reservations,
    validate_reservation_amounts,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

_BOOKING_MAPPING_PATH = Path(__file__).parent / "config" / "mapping" / "CodeAppart_Compta.csv"
_AIRBNB_MAPPING_PATH  = Path(__file__).parent / "config" / "mapping" / "AirbnbLogement_Compta.csv"


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


# ---------------------------------------------------------------------------
# Pipeline endpoint
# ---------------------------------------------------------------------------

@app.route("/process", methods=["POST"])
def process():
    body = request.get_json(force=True, silent=True) or {}
    ota       = body.get("ota", "booking")
    test_mode = bool(body.get("test", False))
    dry_run   = bool(body.get("dry_run", False))
    date_str  = body.get("date")

    if ota not in ("booking", "airbnb"):
        return jsonify({"error": f"Unsupported OTA '{ota}'. Use 'booking' or 'airbnb'."}), 400

    default_folder = DRIVE_FOLDER_BOOKING if ota == "booking" else DRIVE_FOLDER_AIRBNB
    folder_id = body.get("folder_id") or default_folder

    if not date_str or date_str == "AUTO":
        import zoneinfo
        date_str = datetime.now(zoneinfo.ZoneInfo("Europe/Paris")).strftime("%Y-%m-%d")

    try:
        processing_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        return jsonify({"error": f"Invalid date format '{date_str}'. Expected YYYY-MM-DD"}), 400

    logger.info("Processing request: folder_id=%s  date=%s  ota=%s", folder_id, date_str, ota)

    try:
        if ota == "booking":
            result = _run_booking_pipeline(folder_id, processing_date, date_str, test_mode, dry_run)
        else:
            result = _run_airbnb_pipeline(folder_id, processing_date, date_str, test_mode, dry_run)
    except Exception as exc:
        logger.exception("Pipeline failed: %s", exc)
        return jsonify({"error": str(exc)}), 500

    return jsonify(result), 200


# ---------------------------------------------------------------------------
# Booking pipeline
# ---------------------------------------------------------------------------

def _run_booking_pipeline(folder_id: str, processing_date, date_str: str, test_mode: bool = False, dry_run: bool = False) -> dict:
    drive = DriveClient()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)

        # Step 1: download the Booking .xlsx from Drive (exactly 1 file expected)
        xlsx_files = drive.list_excel_files(folder_id)
        if not xlsx_files:
            return {"status": "skipped", "reason": "No .xlsx file found in the Drive folder."}
        if len(xlsx_files) > 1:
            names = ", ".join(f["name"] for f in xlsx_files)
            return {
                "status": "error",
                "reason": f"{len(xlsx_files)} fichiers xlsx trouvés ({names}) — déposez un seul fichier à la fois.",
            }

        xlsx_meta = xlsx_files[0]
        xlsx_file_id = xlsx_meta["id"]
        local_name = xlsx_meta["name"] if xlsx_meta["name"].endswith(".xlsx") else xlsx_meta["name"] + ".xlsx"
        local_xlsx = tmp / local_name
        drive.download_file(xlsx_meta["id"], local_xlsx, mime_type=xlsx_meta.get("mimeType"))
        logger.info("Downloaded Booking file: %s (type: %s)", xlsx_meta["name"], xlsx_meta.get("mimeType"))

        # Step 2: load mapping
        mapping = load_mapping(_BOOKING_MAPPING_PATH)

        # Step 3: parse into payout batches
        parser = BookingExcelParser()
        batches, anomalies = parser.parse_into_batches(local_xlsx)
        all_reservations = [r for b in batches for r in b.reservations]
        anomalies.extend(check_duplicate_reservations(all_reservations))

        if not batches:
            return {"status": "skipped", "reason": "No payout batches found in the Booking file."}

        # Step 4: generate entries per batch
        per_batch_entries = []
        all_processed = []
        for batch in batches:
            batch_entries, batch_processed, entry_anomalies = generate_entries(
                batch.reservations, processing_date, mapping,
                per_reservation_fees=True,
            )
            anomalies.extend(entry_anomalies)
            per_batch_entries.append(batch_entries)
            all_processed.extend(batch_processed)

        # Step 5: per-reservation validation
        for r in all_processed:
            anomalies.extend(validate_reservation_amounts(r))

        # Step 6: global balance check
        balance_ok, anomalies = _check_global_balance(all_processed, anomalies)

        blocking = [a for a in anomalies if a.severity == Severity.BLOCKING]
        warnings = [a for a in anomalies if a.severity == Severity.WARNING]

        if blocking:
            logger.error("%d blocking anomaly/ies — PennyLane NOT posted.", len(blocking))
            if not dry_run and not test_mode:
                _archive_run(drive, folder_id, date_str, [xlsx_file_id], anomalies, "booking")
            return {
                "status":           "blocked",
                "reservations":     len(all_processed),
                "blocking":         len(blocking),
                "warnings":         len(warnings),
                "balance_ok":       balance_ok,
                "blocking_details": [a.message for a in blocking],
            }

        # Step 7: post each payout batch to PennyLane (skipped in dry_run)
        if dry_run:
            total_entries = sum(len(b) for b in per_batch_entries)
            logger.info("dry_run=True — PennyLane NOT posted. %d batches / %d entries ready.", len(per_batch_entries), total_entries)
            return {
                "status":       "dry_run",
                "reservations": len(all_processed),
                "warnings":     len(warnings),
                "blocking":     0,
                "balance_ok":   balance_ok,
                "batches":      len(per_batch_entries),
                "entries":      total_entries,
            }
        if test_mode:
            for batch in per_batch_entries:
                if batch:
                    batch[0].label = "[TEST] " + batch[0].label
        pl_results = _get_pennylane_client().post_batches(per_batch_entries)
        logger.info(
            "Done — %d reservations, %d warnings, balance_ok=%s, %d batches posted to PennyLane",
            len(all_processed), len(warnings), balance_ok, len(pl_results),
        )
        _archive_run(drive, folder_id, date_str, [xlsx_file_id], warnings, "booking")
        return {
            "status":                   "ok",
            "reservations":             len(all_processed),
            "warnings":                 len(warnings),
            "blocking":                 0,
            "balance_ok":               balance_ok,
            "pennylane_batches_posted": len(pl_results),
        }


# ---------------------------------------------------------------------------
# Airbnb pipeline
# ---------------------------------------------------------------------------

def _run_airbnb_pipeline(folder_id: str, processing_date, date_str: str, test_mode: bool = False, dry_run: bool = False) -> dict:
    drive = DriveClient()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)

        # Step 1: download the Airbnb .xlsx from Drive (exactly 1 file expected in root)
        xlsx_files = drive.list_excel_files(folder_id)
        if not xlsx_files:
            return {"status": "skipped", "reason": "No .xlsx file found in the Drive folder."}
        if len(xlsx_files) > 1:
            names = ", ".join(f["name"] for f in xlsx_files)
            return {
                "status": "error",
                "reason": f"{len(xlsx_files)} fichiers xlsx trouvés dans le dossier ({names}) — déposez un seul fichier à la fois.",
            }

        xlsx_meta = xlsx_files[0]
        xlsx_file_id = xlsx_meta["id"]
        local_name = xlsx_meta["name"] if xlsx_meta["name"].endswith(".xlsx") else xlsx_meta["name"] + ".xlsx"
        local_xlsx = tmp / local_name
        drive.download_file(xlsx_meta["id"], local_xlsx, mime_type=xlsx_meta.get("mimeType"))
        logger.info("Downloaded Airbnb file: %s (type: %s)", xlsx_meta["name"], xlsx_meta.get("mimeType"))

        # Step 2: load Airbnb mapping
        mapping = load_airbnb_mapping(_AIRBNB_MAPPING_PATH)

        # Step 3: parse into payout batches
        parser = AirbnbParser()
        batches, anomalies = parser.parse_into_batches(local_xlsx)

        # Enrich NON_EUR anomalies with code_comptable + PennyLane label
        for a in anomalies:
            if a.type == "NON_EUR_CURRENCY":
                logement = a.details.get("logement", "")
                code = mapping.get(logement, logement)
                checkout = a.details.get("checkout_date", "")
                voyageur = a.details.get("voyageur", "")
                row_type = a.details.get("row_type", "")
                ref = a.reservation_ref or ""
                a.details["code_comptable"] = code
                a.details["label_pennylane"] = (
                    f"{code} - AIRBNB - CO : {checkout} - {voyageur} - {row_type} - {ref}"
                )

        if not batches:
            import openpyxl as _xl
            _wb = _xl.load_workbook(local_xlsx, data_only=True)
            _rows = list(_wb.active.iter_rows(values_only=True))
            _header = next((r for r in _rows if r[1] == "Type"), None)
            _first_data = _rows[(_rows.index(_header) + 2)] if _header else None
            return {
                "status": "skipped",
                "reason": "No payout batches found in the Airbnb file.",
                "anomalies_count": len(anomalies),
                "anomalies_sample": [{"type": a.type, "severity": a.severity, "message": a.message} for a in anomalies[:3]],
                "header": list(_header) if _header else None,
                "first_data_row": [str(c) for c in _first_data] if _first_data else None,
            }

        # Step 4: generate entries per batch (keep per-batch for PennyLane posting)
        per_batch_entries = []
        all_processed = []
        for batch in batches:
            batch_entries, batch_processed, entry_anomalies = generate_entries(
                batch.reservations,
                processing_date,
                mapping,
                journal_code=AIRBNB_JOURNAL_CODE,
                account_bank=AIRBNB_ACCOUNT_BANK,
                account_client=AIRBNB_ACCOUNT_CLIENT,
                account_supplier=AIRBNB_ACCOUNT_SUPPLIER,
                account_cancellation_fee=AIRBNB_ACCOUNT_CANCELLATION_FEE,
                ota_label="AIRBNB",
            )
            anomalies.extend(entry_anomalies)
            per_batch_entries.append(batch_entries)
            all_processed.extend(batch_processed)

        # Step 5: per-reservation validation
        for r in all_processed:
            anomalies.extend(validate_reservation_amounts(r))

        # Step 6: global balance check
        balance_ok, anomalies = _check_global_balance(all_processed, anomalies)

        blocking = [a for a in anomalies if a.severity == Severity.BLOCKING]
        warnings = [a for a in anomalies if a.severity == Severity.WARNING]

        if blocking:
            logger.error("%d blocking anomaly/ies — PennyLane NOT posted.", len(blocking))
            if not dry_run and not test_mode:
                _archive_run(drive, folder_id, date_str, [xlsx_file_id], anomalies, "airbnb")
            return {
                "status":           "blocked",
                "reservations":     len(all_processed),
                "blocking":         len(blocking),
                "warnings":         len(warnings),
                "balance_ok":       balance_ok,
                "blocking_details": [a.message for a in blocking],
            }

        # Step 7: post each payout batch to PennyLane (skipped in dry_run)
        if dry_run:
            total_entries = sum(len(b) for b in per_batch_entries)
            logger.info("dry_run=True — PennyLane NOT posted. %d batches / %d entries ready.", len(per_batch_entries), total_entries)
            return {
                "status":       "dry_run",
                "reservations": len(all_processed),
                "warnings":     len(warnings),
                "blocking":     0,
                "balance_ok":   balance_ok,
                "batches":      len(per_batch_entries),
                "entries":      total_entries,
            }
        if test_mode:
            for batch in per_batch_entries:
                if batch:
                    batch[0].label = "[TEST] " + batch[0].label
        pl_results = _get_pennylane_client().post_batches(per_batch_entries)
        logger.info(
            "Done — %d reservations, %d warnings, balance_ok=%s, %d batches posted to PennyLane",
            len(all_processed), len(warnings), balance_ok, len(pl_results),
        )
        _archive_run(drive, folder_id, date_str, [xlsx_file_id], warnings, "airbnb")
        return {
            "status":                   "ok",
            "reservations":             len(all_processed),
            "warnings":                 len(warnings),
            "blocking":                 0,
            "balance_ok":               balance_ok,
            "pennylane_batches_posted": len(pl_results),
        }


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _archive_run(drive, folder_id: str, date_str: str, file_ids: list, anomalies: list, ota: str) -> None:
    """Create Archive subfolder, move source files into it, create anomaly sheet if needed."""
    try:
        archive_id = drive.get_or_create_folder(folder_id, f"Archive {date_str}")
    except Exception as exc:
        logger.warning("Could not create archive folder: %s", exc)
        return
    for fid in file_ids:
        try:
            drive.move_file(fid, archive_id, folder_id)
        except Exception as exc:
            logger.warning("Could not move file %s to archive: %s", fid, exc)
    if anomalies:
        _post_anomaly_sheet(drive, archive_id, anomalies, ota)


def _post_anomaly_sheet(drive, folder_id: str, anomalies: list, ota: str) -> None:
    """Create/replace an anomaly Google Sheet in *folder_id*."""
    header = [
        "Sévérité", "Type", "Référence réservation",
        "Libellé PennyLane", "Montant", "Devise",
        "Message", "Fichier source",
    ]
    rows = [header] + [
        [
            a.severity,
            a.type,
            a.reservation_ref or "",
            a.details.get("label_pennylane", ""),
            a.details.get("montant", ""),
            a.details.get("currency", ""),
            a.message,
            a.source_file,
        ]
        for a in anomalies
    ]
    sheet_name = f"Anomalies {ota.upper()}"
    try:
        drive.create_anomaly_sheet(folder_id, sheet_name, rows)
        logger.info("Anomaly sheet created: '%s' (%d row(s))", sheet_name, len(rows) - 1)
    except Exception as exc:
        logger.warning("Could not create anomaly sheet: %s", exc)


def _get_pennylane_client() -> PennyLaneClient:
    token = os.environ.get("PENNYLANE_TOKEN")
    if not token:
        raise ValueError("PENNYLANE_TOKEN environment variable not set.")
    return PennyLaneClient(token=token)


def _check_global_balance(processed, anomalies):
    """Run the global balance check and return (balance_ok, updated_anomalies)."""
    if not processed:
        return True, anomalies
    total_net            = sum(r.net            for r in processed)
    total_amount         = sum(r.amount         for r in processed)
    total_commission     = sum(r.commission     for r in processed)
    total_payment_charge = sum(r.payment_charge for r in processed)
    total_city_tax       = sum(r.city_tax       for r in processed)
    balance_anomaly = check_balance(
        total_net, total_amount, total_commission,
        total_payment_charge, total_city_tax,
    )
    if balance_anomaly:
        anomalies.append(balance_anomaly)
    return balance_anomaly is None, anomalies


# ---------------------------------------------------------------------------
# Entry point (local dev / Cloud Run)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
