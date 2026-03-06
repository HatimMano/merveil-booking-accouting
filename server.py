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
        "excel_file_id": "1xyz..."
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
    AIRBNB_JOURNAL_CODE,
    AIRBNB_ACCOUNT_BANK,
    AIRBNB_ACCOUNT_CLIENT,
    AIRBNB_ACCOUNT_SUPPLIER,
)
from parsers.booking import BookingParser
from parsers.airbnb import AirbnbParser
from accounting.entries import generate_entries
from accounting.excel import create_excel_workbook
from drive.client import DriveClient
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
    folder_id        = body.get("folder_id")
    output_folder_id = body.get("output_folder_id") or folder_id
    date_str         = body.get("date")
    ota              = body.get("ota", "booking")

    if not folder_id:
        return jsonify({"error": "Missing 'folder_id' in request body"}), 400

    if ota not in ("booking", "airbnb"):
        return jsonify({"error": f"Unsupported OTA '{ota}'. Use 'booking' or 'airbnb'."}), 400

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
            result = _run_booking_pipeline(folder_id, output_folder_id, processing_date, date_str)
        else:
            result = _run_airbnb_pipeline(folder_id, output_folder_id, processing_date, date_str)
    except Exception as exc:
        logger.exception("Pipeline failed: %s", exc)
        return jsonify({"error": str(exc)}), 500

    return jsonify(result), 200


# ---------------------------------------------------------------------------
# Booking pipeline
# ---------------------------------------------------------------------------

def _run_booking_pipeline(folder_id: str, output_folder_id: str, processing_date, date_str: str) -> dict:
    drive = DriveClient()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        csv_dir = tmp / "csv_input"

        # Step 1: download CSVs from Drive
        local_csvs = drive.download_all_csvs(folder_id, csv_dir)
        if not local_csvs:
            return {"status": "skipped", "reason": "No CSV files found in the Drive folder."}

        # Step 2: load mapping
        mapping = load_mapping(_BOOKING_MAPPING_PATH)

        # Step 3: parse
        parser = BookingParser()
        reservations, anomalies = parser.parse_directory(csv_dir)
        anomalies.extend(check_duplicate_reservations(reservations))

        # Step 4: generate entries
        entries, processed, entry_anomalies = generate_entries(
            reservations, processing_date, mapping
        )
        anomalies.extend(entry_anomalies)

        # Step 5: per-reservation validation
        for r in processed:
            anomalies.extend(validate_reservation_amounts(r))

        # Step 6: global balance check
        balance_ok, anomalies = _check_global_balance(processed, anomalies)

        blocking = [a for a in anomalies if a.severity == Severity.BLOCKING]
        warnings = [a for a in anomalies if a.severity == Severity.WARNING]

        if blocking:
            logger.error("%d blocking anomaly/ies — Excel NOT generated.", len(blocking))
            return {
                "status":           "blocked",
                "reservations":     len(processed),
                "blocking":         len(blocking),
                "warnings":         len(warnings),
                "balance_ok":       balance_ok,
                "blocking_details": [a.message for a in blocking],
            }

        # Step 7: create Excel and upload
        excel_filename = f"booking_{date_str}.xlsx"
        excel_path     = tmp / excel_filename
        create_excel_workbook(entries, anomalies, processed, excel_path)

        dated_folder_id = drive.get_or_create_folder(output_folder_id, date_str)
        file_id = drive.upload_excel(excel_path, dated_folder_id, excel_filename)

        logger.info(
            "Done — %d reservations, %d warnings, balance_ok=%s, Excel id=%s",
            len(processed), len(warnings), balance_ok, file_id,
        )
        return {
            "status":         "ok",
            "reservations":   len(processed),
            "warnings":       len(warnings),
            "blocking":       0,
            "balance_ok":     balance_ok,
            "excel_file_id":  file_id,
            "excel_filename": excel_filename,
        }


# ---------------------------------------------------------------------------
# Airbnb pipeline
# ---------------------------------------------------------------------------

def _run_airbnb_pipeline(folder_id: str, output_folder_id: str, processing_date, date_str: str) -> dict:
    drive = DriveClient()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)

        # Step 1: download the Airbnb .xlsx from Drive
        xlsx_files = drive.list_excel_files(folder_id)
        if not xlsx_files:
            return {"status": "skipped", "reason": "No .xlsx file found in the Drive folder."}

        xlsx_meta = xlsx_files[0]  # most recent (list_excel_files orders by modifiedTime desc)
        # Always save locally as .xlsx (export handles Google Sheets → xlsx conversion)
        local_name = xlsx_meta["name"] if xlsx_meta["name"].endswith(".xlsx") else xlsx_meta["name"] + ".xlsx"
        local_xlsx = tmp / local_name
        drive.download_file(xlsx_meta["id"], local_xlsx, mime_type=xlsx_meta.get("mimeType"))
        logger.info("Downloaded Airbnb file: %s (type: %s)", xlsx_meta["name"], xlsx_meta.get("mimeType"))

        # Step 2: load Airbnb mapping
        mapping = load_airbnb_mapping(_AIRBNB_MAPPING_PATH)

        # Step 3: parse into payout batches
        parser = AirbnbParser()
        batches, anomalies = parser.parse_into_batches(local_xlsx)

        if not batches:
            return {"status": "skipped", "reason": "No payout batches found in the Airbnb file."}

        # Step 4: generate entries per batch
        all_entries = []
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
                ota_label="AIRBNB",
            )
            anomalies.extend(entry_anomalies)
            all_entries.extend(batch_entries)
            all_processed.extend(batch_processed)

        # Step 5: per-reservation validation
        for r in all_processed:
            anomalies.extend(validate_reservation_amounts(r))

        # Step 6: global balance check
        balance_ok, anomalies = _check_global_balance(all_processed, anomalies)

        blocking = [a for a in anomalies if a.severity == Severity.BLOCKING]
        warnings = [a for a in anomalies if a.severity == Severity.WARNING]

        if blocking:
            logger.error("%d blocking anomaly/ies — Excel NOT generated.", len(blocking))
            return {
                "status":           "blocked",
                "reservations":     len(all_processed),
                "blocking":         len(blocking),
                "warnings":         len(warnings),
                "balance_ok":       balance_ok,
                "blocking_details": [a.message for a in blocking],
            }

        # Step 7: create Excel and upload
        excel_filename = f"airbnb_{date_str}.xlsx"
        excel_path     = tmp / excel_filename
        create_excel_workbook(all_entries, anomalies, all_processed, excel_path)

        dated_folder_id = drive.get_or_create_folder(output_folder_id, date_str)
        file_id = drive.upload_excel(excel_path, dated_folder_id, excel_filename)

        logger.info(
            "Done — %d reservations, %d warnings, balance_ok=%s, Excel id=%s",
            len(all_processed), len(warnings), balance_ok, file_id,
        )
        return {
            "status":         "ok",
            "reservations":   len(all_processed),
            "warnings":       len(warnings),
            "blocking":       0,
            "balance_ok":     balance_ok,
            "excel_file_id":  file_id,
            "excel_filename": excel_filename,
        }


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

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
