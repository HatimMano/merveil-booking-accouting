"""Orchestrator générique du pipeline compta.

Fonction unique `run_pipeline(source, ...)` qui consomme n'importe quelle `Source`
(Booking Drive, Airbnb Drive, futur Mews Bills, futur Mews Payments) et :
  1. Fetch les batches via la source
  2. Enrichit les anomalies (hook source-spécifique optionnel)
  3. Lookup BQ pour récupérer les `bill_number` Mews (chantier 2)
  4. Génère les écritures comptables équilibrées par batch
  5. Valide montants + balance globale
  6. Bloque sur anomalie bloquante (rien posté)
  7. Poste dans PennyLane batch par batch, journalisé write-ahead dans
     `pennylane.posting_journal` (intent → POST → trace → posted) : un replay
     skippe les batches `posted`, se bloque sur un `intent` orphelin
  8. Trace dans BigQuery `pennylane.raw_postings` (par batch, fatal)
  9. Archive le fichier source dans Drive (si Source basée Drive)

Le orchestrator n'a aucune logique OTA-spécifique : tout passe par les
`Source.entries_kwargs` et les hooks `enrich_anomalies()`.
"""

import logging
import os
import time
from datetime import date
from pathlib import Path
from typing import Any

from accounting.entries import generate_entries
from bigquery.journal import PHASE_INTENT, PHASE_POSTED, read_journal, run_mode, write_phase
from bigquery.postings import build_synthetic_results, write_postings
from config.mapping_loader import _normalize_key, load_apartment_comptable_map
from drive.client import DriveClient
from lookups.mews import lookup_bills, resolve_apartments_by_channel
from pennylane.client import PennyLaneClient
from sources.base import Source
from validators.anomalies import Anomaly, Severity, check_balance, validate_reservation_amounts

logger = logging.getLogger(__name__)

# Pivot apartment_code Mews (complet) → code_comptable, pour le fallback Mews
# quand un libellé d'annonce OTA est absent du mapping (annonce renommée).
_APARTMENT_CODE_MAP_PATH = Path(__file__).parent / "config" / "mapping" / "Mapping_appart_code.csv"


def run_pipeline(
    source: Source,
    processing_date: date,
    drive_client: DriveClient,
    test_mode: bool = False,
    dry_run: bool = False,
    bq_only: bool = False,
    run_id: str = "",
    force: bool = False,
) -> dict[str, Any]:
    """Orchestre le pipeline complet pour une source donnée. Retourne le résumé du run.

    `force=True` ignore le journal en LECTURE (tout est re-posté) — à utiliser
    uniquement après nettoyage manuel dans Pennylane d'un état incertain.
    Les phases restent écrites au journal même en force.
    """

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

    # ── Step 2.5: fallback Mews pour les libellés absents du mapping ────────
    # Annonce OTA renommée → ref_appart introuvable au mapping. On résout via
    # le code de confirmation (stable) → Mews → apartment_code → code_comptable.
    # Non-fatal : en cas d'échec, on retombe sur le BLOCKING habituel.
    mews_fallback: dict[str, str] = {}
    try:
        unresolved = [
            r.reference_number
            for b in src_result.batches for r in b.reservations
            if src_result.mapping.get(r.ref_appart) is None
            and src_result.mapping.get(_normalize_key(r.ref_appart)) is None
            and r.reference_number
        ]
        if unresolved:
            full_to_comptable = load_apartment_comptable_map(_APARTMENT_CODE_MAP_PATH)
            mews_fallback = resolve_apartments_by_channel(unresolved, full_to_comptable)
    except Exception as exc:
        logger.warning("Mews apartment fallback failed (non-fatal): %s", exc)
        mews_fallback = {}

    # ── Step 3: génération des écritures par batch ─────────────────────────
    per_batch_entries = []
    all_processed = []
    for batch in src_result.batches:
        batch_entries, batch_processed, entry_anomalies = generate_entries(
            batch.reservations,
            processing_date,
            src_result.mapping,
            bill_lookup=bill_lookup,
            mews_fallback=mews_fallback,
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

    # ── Step 9-10: POST + trace, batch par batch, journalisés (write-ahead) ─
    # Séquence par batch : intent (fatal) → POST → trace raw_postings (fatal)
    # → posted (fatal). `posted` ⇒ tracé. Un `intent` sans `posted` au replay
    # = POST au résultat inconnu → run bloqué, rien n'est re-posté aveuglément.
    mode = run_mode(test_mode, bq_only)
    batch_keys = _batch_keys(src_result.batches)

    if force:
        logger.warning("force=True — journal ignoré en lecture : TOUS les batches seront (re)postés.")
        journal_state: dict[str, str] = {}
    else:
        journal_state = read_journal(
            ota=source.name,
            source_file=src_result.source_file,
            file_hash=src_result.file_hash,
            mode=mode,
        )

    uncertain = [k for k in batch_keys if journal_state.get(k) == PHASE_INTENT]
    if uncertain:
        return _handle_uncertain_journal(
            drive_client, src_result, source.name, uncertain, mode,
        )

    client = None if bq_only else _get_pennylane_client()
    posted_count = 0
    skipped_count = 0
    bq_rows_written = 0
    total = len(per_batch_entries)

    for i, batch in enumerate(per_batch_entries):
        key = batch_keys[i]
        if journal_state.get(key) == PHASE_POSTED:
            logger.info("Batch %d/%d (key=%s) déjà posté — SKIP (journal).", i + 1, total, key)
            skipped_count += 1
            continue

        label = batch[0].label if batch else None
        journal_kwargs = dict(
            ota=source.name,
            source_file=src_result.source_file,
            file_hash=src_result.file_hash,
            batch_key=key,
            batch_index=i,
            mode=mode,
            run_id=run_id,
            label=label,
            service_version=os.environ.get("K_REVISION") or os.environ.get("GIT_SHA"),
        )

        write_phase(phase=PHASE_INTENT, **journal_kwargs)

        if bq_only:
            result = build_synthetic_results([batch])[0]
        else:
            logger.info("Posting batch %d/%d (%d lines, key=%s)...", i + 1, total, len(batch), key)
            result = client.post_ledger_entry(batch)

        bq_rows_written += write_postings(
            run_id=run_id,
            ota=source.name,
            source_file=src_result.source_file,
            per_batch_entries=[batch],
            pl_results=[result],
            test_mode=test_mode,
            bq_only=bq_only,
            first_batch_index=i,
        )

        write_phase(
            phase=PHASE_POSTED,
            ledger_entry_id=result.get("ledger_entry_id"),
            **journal_kwargs,
        )
        posted_count += 1

        if not bq_only and i < total - 1:
            time.sleep(0.5)

    logger.info(
        "Done — %d reservations, %d warnings, balance_ok=%s, %d batch(es) posted, %d skipped (journal)",
        len(all_processed), len(warnings), balance_ok, posted_count, skipped_count,
    )

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
        "pennylane_batches_posted": 0 if bq_only else posted_count,
        "batches_skipped_journal":  skipped_count,
        "bq_rows_written":          bq_rows_written,
        "bq_only":                  bq_only,
    }


# ── Helpers internes ───────────────────────────────────────────────────────

def _batch_keys(batches: list) -> list[str]:
    """Clé sémantique de chaque batch pour le journal d'idempotence.

    Booking : `payout_id` ; Airbnb : `payout_reference`. Fallback positionnel
    si une future source n'a ni l'un ni l'autre (moins robuste à un
    changement d'ordre du parser — préférer un identifiant métier).
    """
    keys = []
    for i, b in enumerate(batches):
        key = getattr(b, "payout_id", None) or getattr(b, "payout_reference", None) or f"index-{i}"
        keys.append(str(key))
    if len(set(keys)) != len(keys):
        # Deux batches avec la même clé → le skip du journal deviendrait ambigu.
        raise ValueError(f"Batch keys non uniques ({keys}) — journal d'idempotence impossible.")
    return keys


def _handle_uncertain_journal(drive, src_result, source_name: str, uncertain: list, mode: str) -> dict:
    """Un `intent` sans `posted` = un POST Pennylane au résultat inconnu.

    On ne re-poste RIEN (risque de doublon comptable) et on n'archive PAS le
    fichier (il doit rester pour le re-run post-résolution). Résolution
    manuelle documentée dans bigquery/journal.py (vérifier dans Pennylane par
    date+label, puis INSERT `posted` manuel OU nettoyage + re-run force=true).
    """
    msg = (
        f"{len(uncertain)} batch(es) en état INCERTAIN dans le journal "
        f"(intent sans posted) : {uncertain}. Un POST Pennylane a peut-être "
        f"abouti sans trace (crash mi-run). RIEN n'a été posté sur ce run. "
        f"Vérifier dans Pennylane (date + label, cf. pennylane.posting_journal) "
        f"puis résoudre — procédure dans bigquery/journal.py."
    )
    logger.error("JOURNAL BLOQUANT [%s/%s] : %s", source_name, src_result.source_file, msg)
    anomaly = Anomaly(
        type="JOURNAL_UNCERTAIN_STATE",
        severity=Severity.BLOCKING,
        message=msg,
        source_file=src_result.source_file,
        reservation_ref=None,
        details={"uncertain_batch_keys": uncertain, "mode": mode},
    )
    # Sheet d'anomalie dans le dossier RACINE (pas d'archive : le fichier reste).
    if src_result.drive_folder_id:
        _post_anomaly_sheet(drive, src_result.drive_folder_id, [anomaly], source_name)
    return {
        "status":            "journal_blocked",
        "uncertain_batches": uncertain,
        "message":           msg,
    }


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
