"""Orchestrator générique du pipeline compta.

Fonction unique `run_pipeline(source, ...)` qui consomme n'importe quelle `Source`
(Booking Drive, Airbnb Drive, futur Mews Bills, futur Mews Payments) et :
  1. Fetch les batches via la source
  2. Enrichit les anomalies (hook source-spécifique optionnel)
  3. Lookup BQ pour récupérer les `bill_number` Mews (chantier 2)
  4. Génère les écritures comptables équilibrées par batch
  5. Valide montants + balance globale
  6. Bloque sur anomalie bloquante (rien posté)
  7. Poste dans PennyLane (sauf dry_run / bq_only)
  8. Trace dans BigQuery `pennylane.raw_postings`
  9. Archive le fichier source dans Drive (si Source basée Drive)

Le orchestrator n'a aucune logique OTA-spécifique : tout passe par les
`Source.entries_kwargs` et les hooks `enrich_anomalies()`.
"""

import logging
import os
from datetime import date
from typing import Any

from accounting.entries import generate_entries
from bigquery.postings import build_synthetic_results, write_postings
from drive.client import DriveClient
from lookups.mews import lookup_bills
from pennylane.client import PennyLaneClient
from sources.base import Source
from validators.anomalies import Severity, check_balance, validate_reservation_amounts

logger = logging.getLogger(__name__)


def run_pipeline(
    source: Source,
    processing_date: date,
    drive_client: DriveClient,
    test_mode: bool = False,
    dry_run: bool = False,
    bq_only: bool = False,
    run_id: str = "",
) -> dict[str, Any]:
    """Orchestre le pipeline complet pour une source donnée. Retourne le résumé du run."""

    # ── Step 1: fetch via la Source ────────────────────────────────────────
    src_result = source.fetch(processing_date)
    if not src_result.batches:
        return {"status": "skipped", "reason": f"No batches found in {source.name} source."}

    # ── Step 1.5: enrichissement source-spécifique des anomalies ───────────
    source.enrich_anomalies(src_result)
    anomalies = src_result.anomalies

    # ── Step 2: BQ lookup → numéro de facture Mews (chantier 2) ────────────
    # Le gross (= amount + city_tax_negatif) sert au matching par proximité
    # de montant — évite les bills annexes (cf cas Rachel Ward 24618).
    all_targets = [
        (r.reference_number, float(r.amount + r.city_tax))
        for b in src_result.batches for r in b.reservations
    ]
    try:
        bill_lookup = lookup_bills(all_targets)
    except Exception as exc:
        logger.warning("Bill lookup failed (non-fatal): %s", exc)
        bill_lookup = {}

    # ── Step 3: génération des écritures par batch ─────────────────────────
    per_batch_entries = []
    all_processed = []
    for batch in src_result.batches:
        batch_entries, batch_processed, entry_anomalies = generate_entries(
            batch.reservations,
            processing_date,
            src_result.mapping,
            bill_lookup=bill_lookup,
            **source.entries_kwargs,
        )
        anomalies.extend(entry_anomalies)
        per_batch_entries.append(batch_entries)
        all_processed.extend(batch_processed)

    # ── Step 4: validation montants par réservation ────────────────────────
    for r in all_processed:
        anomalies.extend(validate_reservation_amounts(r))

    # ── Step 5: balance check global ───────────────────────────────────────
    balance_ok, anomalies = _check_global_balance(all_processed, anomalies)

    blocking = [a for a in anomalies if a.severity == Severity.BLOCKING]
    warnings = [a for a in anomalies if a.severity == Severity.WARNING]

    file_date = max(b.payout_date for b in src_result.batches).strftime("%Y-%m-%d")

    # ── Step 6: blocage si anomalie bloquante ──────────────────────────────
    if blocking:
        logger.error("%d blocking anomaly/ies — PennyLane NOT posted.", len(blocking))
        if not dry_run and not test_mode:
            _archive_run(
                drive_client, src_result.drive_folder_id, file_date,
                src_result.archive_file_ids, anomalies, source.name,
            )
        return {
            "status":           "blocked",
            "reservations":     len(all_processed),
            "blocking":         len(blocking),
            "warnings":         len(warnings),
            "balance_ok":       balance_ok,
            "blocking_details": [a.message for a in blocking],
        }

    # ── Step 7: dry_run (validation sans POST ni archive) ──────────────────
    if dry_run:
        total_entries = sum(len(b) for b in per_batch_entries)
        logger.info(
            "dry_run=True — PennyLane NOT posted. %d batches / %d entries ready.",
            len(per_batch_entries), total_entries,
        )
        return {
            "status":       "dry_run",
            "reservations": len(all_processed),
            "warnings":     len(warnings),
            "blocking":     0,
            "balance_ok":   balance_ok,
            "batches":      len(per_batch_entries),
            "entries":      total_entries,
        }

    # ── Step 8: préfixe [TEST] si test_mode ────────────────────────────────
    if test_mode:
        for batch in per_batch_entries:
            if batch:
                batch[0].label = "[TEST] " + batch[0].label

    # ── Step 9: post PennyLane (ou synthetic results si bq_only) ───────────
    if bq_only:
        pl_results = build_synthetic_results(per_batch_entries)
        logger.info("bq_only=True — PennyLane skipped, writing trace to BQ only.")
    else:
        pl_results = _get_pennylane_client().post_batches(per_batch_entries)
        logger.info(
            "Done — %d reservations, %d warnings, balance_ok=%s, %d batches posted to PennyLane",
            len(all_processed), len(warnings), balance_ok, len(pl_results),
        )

    # ── Step 10: BQ trace (non-fatal si plante) ────────────────────────────
    bq_rows_written = 0
    try:
        bq_rows_written = write_postings(
            run_id=run_id,
            ota=source.name,
            source_file=src_result.source_file,
            per_batch_entries=per_batch_entries,
            pl_results=pl_results,
            test_mode=test_mode,
            bq_only=bq_only,
        )
    except Exception as exc:
        logger.exception("BQ trace write failed (non-fatal): %s", exc)

    # ── Step 11: archive Drive (seulement si Source basée Drive) ───────────
    archive_date = f"{file_date} [BQ-ONLY]" if bq_only else file_date
    _archive_run(
        drive_client, src_result.drive_folder_id, archive_date,
        src_result.archive_file_ids, warnings, source.name,
    )

    return {
        "status":                   "ok",
        "reservations":             len(all_processed),
        "warnings":                 len(warnings),
        "blocking":                 0,
        "balance_ok":               balance_ok,
        "pennylane_batches_posted": 0 if bq_only else len(pl_results),
        "bq_rows_written":          bq_rows_written,
        "bq_only":                  bq_only,
    }


# ── Helpers internes ───────────────────────────────────────────────────────

def _archive_run(drive, folder_id: str, date_str: str, file_ids: list, anomalies: list, source_name: str) -> None:
    """Crée le sous-dossier Archive, déplace les fichiers source, crée la sheet d'anomalies.

    No-op si `folder_id` est vide (= Source basée BigQuery, pas Drive).
    """
    if not folder_id:
        return  # Source non-Drive (ex: Mews Bills depuis BQ) — pas d'archive
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
        _post_anomaly_sheet(drive, archive_id, anomalies, source_name)


def _post_anomaly_sheet(drive, folder_id: str, anomalies: list, source_name: str) -> None:
    """Crée/remplace une Google Sheet d'anomalies dans `folder_id`."""
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
    sheet_name = f"Anomalies {source_name.upper()}"
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
