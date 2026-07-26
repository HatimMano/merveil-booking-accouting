"""Write-ahead journal for PennyLane batch posts — pennylane.posting_journal.

Append-only. 2 rows per batch in the nominal case:
  phase='intent' — written BEFORE the PennyLane POST (fatal if it fails:
                   we never send money without a trace)
  phase='posted' — written AFTER the POST succeeded AND the raw_postings
                   trace was written

Replay contract (read_journal), for a given (ota, source_file, file_hash, mode):
  batch_key -> 'posted'  : batch fully done — the orchestrator SKIPS it
  batch_key -> 'intent'  : POST outcome unknown (crash between intent and
                           posted) — the orchestrator BLOCKS the whole run,
                           nothing is re-posted blindly

`batch_key` is the semantic payout identifier (Booking `payout_id`, Airbnb
`payout_reference`) — stable even if the parser changes batch ordering.
`file_hash` pins the file content: a corrected re-upload (same name, new
content) gets a fresh journal and is posted normally.

Manual resolution of an 'intent' orphan (after checking in PennyLane whether
the entry exists — date + label are in the journal row):
  -- entry EXISTS in PennyLane → mark it posted so the replay skips it:
  INSERT INTO `merveil-data-warehouse.pennylane.posting_journal`
    (logged_at, run_id, ota, source_file, file_hash, batch_key, batch_index,
     phase, mode, label)
  SELECT CURRENT_TIMESTAMP(), 'manual-resolution', ota, source_file, file_hash,
         batch_key, batch_index, 'posted', mode, label
  FROM `merveil-data-warehouse.pennylane.posting_journal`
  WHERE batch_key = '<key>' AND phase = 'intent';
  -- entry ABSENT from PennyLane → delete the entry manually if partial, then
  -- re-run with {"force": true} (re-posts everything for the file).
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Optional

from google.cloud import bigquery

logger = logging.getLogger(__name__)

_PROJECT = "merveil-data-warehouse"
_TABLE = f"{_PROJECT}.pennylane.posting_journal"

PHASE_INTENT = "intent"
PHASE_POSTED = "posted"


def run_mode(test_mode: bool, bq_only: bool) -> str:
    """Journal mode — skip logic only matches rows written in the same mode,
    so bq_only/test runs never shadow (or get shadowed by) live runs."""
    if bq_only:
        return "bq_only"
    if test_mode:
        return "test"
    return "live"


def read_journal(
    *, ota: str, source_file: str, file_hash: str, mode: str,
    client: Optional[bigquery.Client] = None,
) -> Dict[str, str]:
    """Return {batch_key: 'posted' | 'intent'} for this exact file content + mode.

    'posted' wins over 'intent' for the same key (nominal completed batch).
    Raises on any BQ error — the caller must not post blindly without journal.
    """
    client = client or bigquery.Client(project=_PROJECT)
    query = f"""
        SELECT batch_key, LOGICAL_OR(phase = 'posted') AS has_posted
        FROM `{_TABLE}`
        WHERE ota = @ota
          AND source_file = @source_file
          AND file_hash = @file_hash
          AND mode = @mode
        GROUP BY batch_key
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("ota", "STRING", ota),
        bigquery.ScalarQueryParameter("source_file", "STRING", source_file),
        bigquery.ScalarQueryParameter("file_hash", "STRING", file_hash),
        bigquery.ScalarQueryParameter("mode", "STRING", mode),
    ])
    rows = list(client.query(query, job_config=job_config).result())
    state = {r.batch_key: (PHASE_POSTED if r.has_posted else PHASE_INTENT) for r in rows}
    if state:
        logger.info(
            "Journal: %d batch(es) déjà tracé(s) pour %s/%s (hash=%s, mode=%s) : %s",
            len(state), ota, source_file, file_hash[:8], mode, state,
        )
    return state


def read_journal_by_keys(
    *, ota: str, mode: str, batch_keys: list,
    client: Optional[bigquery.Client] = None,
) -> Dict[str, str]:
    """Return {batch_key: 'posted' | 'intent'} pour cet OTA + mode, toutes
    sources confondues — pour les sources BQ à fenêtre glissante (Mews
    Payments) où `source_file`/`file_hash` changent à chaque run alors que le
    batch (un versement, immuable) a déjà été posté sous une fenêtre
    précédente. Un payout_id posté une fois est posté pour toujours.

    ⚠ Conséquence : une re-livraison du même payout avec des montants
    CORRIGÉS ne serait pas re-postée (la réco 9.7 la détecterait).
    """
    client = client or bigquery.Client(project=_PROJECT)
    keys = [str(k) for k in batch_keys]
    if not keys:
        return {}
    query = f"""
        SELECT batch_key, LOGICAL_OR(phase = 'posted') AS has_posted
        FROM `{_TABLE}`
        WHERE ota = @ota
          AND mode = @mode
          AND batch_key IN UNNEST(@batch_keys)
        GROUP BY batch_key
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("ota", "STRING", ota),
        bigquery.ScalarQueryParameter("mode", "STRING", mode),
        bigquery.ArrayQueryParameter("batch_keys", "STRING", keys),
    ])
    rows = list(client.query(query, job_config=job_config).result())
    state = {r.batch_key: (PHASE_POSTED if r.has_posted else PHASE_INTENT) for r in rows}
    if state:
        logger.info(
            "Journal (scope batch_key): %d batch(es) déjà tracé(s) pour %s (mode=%s) : %s",
            len(state), ota, mode, state,
        )
    return state


def write_phase(
    *, ota: str, source_file: str, file_hash: str, batch_key: str,
    batch_index: int, phase: str, mode: str, run_id: str,
    ledger_entry_id: Optional[str] = None, label: Optional[str] = None,
    service_version: Optional[str] = None,
    client: Optional[bigquery.Client] = None,
) -> None:
    """Append one phase row. Raises on insert errors (fatal by design)."""
    client = client or bigquery.Client(project=_PROJECT)
    row = {
        "logged_at":       datetime.now(timezone.utc).isoformat(),
        "run_id":          run_id,
        "ota":             ota,
        "source_file":     source_file,
        "file_hash":       file_hash,
        "batch_key":       batch_key,
        "batch_index":     batch_index,
        "phase":           phase,
        "mode":            mode,
        "ledger_entry_id": str(ledger_entry_id) if ledger_entry_id is not None else None,
        "label":           label,
        "service_version": service_version,
    }
    errors = client.insert_rows_json(_TABLE, [row])
    if errors:
        raise RuntimeError(f"posting_journal insert failed ({phase}, batch_key={batch_key}): {errors}")
