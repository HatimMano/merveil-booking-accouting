"""BigQuery writer for the pennylane.raw_postings trace table.

Append-only. 1 row = 1 ledger_entry_line PennyLane (or planned, if bq_only).

Pivot for downstream reconciliation with mews_raw.raw_bills.
"""

import logging
import os
from datetime import datetime, timezone
from decimal import Decimal
from typing import List, Optional

from google.cloud import bigquery

from accounting.entries import AccountingEntry

logger = logging.getLogger(__name__)

_PROJECT = "merveil-data-warehouse"
_TABLE = f"{_PROJECT}.pennylane.raw_postings"


def _to_str_amount(d: Optional[Decimal]) -> Optional[str]:
    """BQ NUMERIC accepts strings — preserves precision better than float."""
    return f"{d:.2f}" if d is not None else None


def _build_rows(
    *,
    run_id: str,
    ota: str,
    source_file: Optional[str],
    posted_at: datetime,
    test_mode: bool,
    bq_only: bool,
    service_version: Optional[str],
    per_batch_entries: List[List[AccountingEntry]],
    pl_results: List[dict],
    first_batch_index: int = 0,
) -> List[dict]:
    """
    Flatten batches × entries × PennyLane line ids into BQ rows.

    `per_batch_entries[i]` aligns with `pl_results[i]["ledger_entry_lines"][j]`
    in chantier 1 we trust position-based alignment (pennylane.client matches
    entries[i] -> api response lines[i]).
    """
    if len(per_batch_entries) != len(pl_results):
        raise ValueError(
            f"Mismatch: {len(per_batch_entries)} batches vs {len(pl_results)} results"
        )

    rows: List[dict] = []
    for batch_index, (entries, result) in enumerate(zip(per_batch_entries, pl_results), start=first_batch_index):
        ledger_entry_id = result.get("ledger_entry_id")
        line_results = result.get("ledger_entry_lines") or [{}] * len(entries)

        for entry_index, (e, line_info) in enumerate(zip(entries, line_results)):
            rows.append({
                "posted_at":            posted_at.isoformat(),
                "run_id":               run_id,
                "ota":                  ota,
                "journal_code":         e.journal,
                "processing_date":      e.date.isoformat(),
                "payout_date":          e.payout_date.isoformat() if e.payout_date else None,
                "source_file":          source_file,
                "batch_index":          batch_index,
                "entry_index":          entry_index,
                "ledger_entry_id":      ledger_entry_id,
                "ledger_entry_line_id": line_info.get("ledger_entry_line_id"),
                "account_code":         e.account,
                "ledger_account_id":    line_info.get("ledger_account_id"),
                "label":                e.label,
                "debit":                _to_str_amount(e.debit),
                "credit":               _to_str_amount(e.credit),
                "ota_reservation_ref":  e.ota_reservation_ref,
                "ref_appart":           e.ref_appart,
                "code_comptable":       e.code_comptable,
                "ref_piece":            e.ref_piece or None,
                "bill_id_mews":         e.bill_id_mews,
                "test_mode":            test_mode,
                "bq_only":              bq_only,
                "service_version":      service_version,
            })
    return rows


def write_postings(
    *,
    run_id: str,
    ota: str,
    source_file: Optional[str],
    per_batch_entries: List[List[AccountingEntry]],
    pl_results: List[dict],
    test_mode: bool = False,
    bq_only: bool = False,
    first_batch_index: int = 0,
) -> int:
    """
    Insert entries into pennylane.raw_postings (append-only).

    Depuis 2026-07-02 l'orchestrateur appelle cette fonction batch par batch,
    juste après chaque POST Pennylane (`first_batch_index` = position réelle
    du batch dans le run, pour garder la sémantique de `batch_index`).

    Returns the number of rows inserted. Raises on insert errors so the caller
    can decide whether to alert / retry.

    In bq_only mode, pl_results should be a list of synthetic results with
    ledger_entry_id=None and ledger_entry_line_id=None for each entry.
    """
    if not per_batch_entries:
        logger.info("write_postings: no batches to write.")
        return 0

    posted_at = datetime.now(timezone.utc)
    service_version = os.environ.get("K_REVISION") or os.environ.get("GIT_SHA")

    rows = _build_rows(
        run_id=run_id,
        ota=ota,
        source_file=source_file,
        posted_at=posted_at,
        test_mode=test_mode,
        bq_only=bq_only,
        service_version=service_version,
        per_batch_entries=per_batch_entries,
        pl_results=pl_results,
        first_batch_index=first_batch_index,
    )

    client = bigquery.Client(project=_PROJECT)
    errors = client.insert_rows_json(_TABLE, rows)
    if errors:
        logger.error("BQ insert errors: %s", errors)
        raise RuntimeError(f"BigQuery insert failed: {errors}")

    logger.info(
        "BQ trace: inserted %d rows in %s (run_id=%s, ota=%s, bq_only=%s, test_mode=%s)",
        len(rows), _TABLE, run_id, ota, bq_only, test_mode,
    )
    return len(rows)


def build_synthetic_results(per_batch_entries: List[List[AccountingEntry]]) -> List[dict]:
    """
    Build placeholder pl_results for bq_only mode (no PennyLane call made).

    Same shape as PennyLaneClient.post_ledger_entry() return value, but with
    NULL ids — the entries can still be traced in BQ for validation.
    """
    return [
        {
            "dry_run":            True,
            "ledger_entry_id":    None,
            "ledger_entry_lines": [
                {"ledger_entry_line_id": None, "ledger_account_id": None}
                for _ in entries
            ],
        }
        for entries in per_batch_entries
    ]
