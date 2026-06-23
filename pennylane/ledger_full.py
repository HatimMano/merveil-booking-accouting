"""Pull PennyLane grand livre COMPLET (tous comptes) → BQ `pennylane.raw_ledger_lines`.

Lot A du chantier "Pennylane → BQ" (cf. Archides CLAUDE.md). Contrairement à
`grand_livre.py` (3 comptes 6041/6042/60472, mensuel, branché sur les modèles
compta), ce module aspire TOUTES les lignes du grand livre en daily incrémental.

Mécanique incrémentale (l'API v2 ignore les filtres de date mais respecte
`sort=-date`) :
  - on tire en DESC depuis le futur,
  - on garde toutes les lignes `date >= from_date`,
  - early-stop dès qu'on passe sous `from_date` (overlap glissant),
  - MERGE sur `ledger_entry_line_id` → les corrections antidatées dans la fenêtre
    d'overlap sont rejouées (0 doublon).

⚠️ Ancre du curseur = `today - OVERLAP_DAYS`, PAS `MAX(date)` : des écritures sont
datées dans le futur (prévisions / versements), elles gonfleraient le curseur et
feraient rater les écritures récentes. Les écritures futures restent captées
puisqu'elles arrivent en tête du scan DESC.

Le nom de compte (`ledger_account.label`) et le libellé d'écriture parente ne sont
PAS embarqués dans la ligne → enrichissement `get_account`/`get_entry` volontairement
DÉFÉRÉ (N+1 trop coûteux sur le ledger complet ; non requis pour la validation).
À brancher en Phase 2 (re-pointage des modèles compta) via une référence
`ledger_accounts` séparée.

Usage CLI :
  PENNYLANE_TOKEN=xxx python -m pennylane.ledger_full                      # daily incr (today-45j)
  PENNYLANE_TOKEN=xxx python -m pennylane.ledger_full --overlap-days 60
  PENNYLANE_TOKEN=xxx python -m pennylane.ledger_full --from-date 2025-01-01   # bootstrap
  PENNYLANE_TOKEN=xxx python -m pennylane.ledger_full --dry-run
"""

import argparse
import logging
import os
from datetime import datetime, timedelta, timezone

from google.cloud import bigquery

from .grand_livre import PennylaneGLClient

logger = logging.getLogger(__name__)

BQ_PROJECT = "merveil-data-warehouse"
BQ_DATASET = "pennylane"
BQ_TABLE = "raw_ledger_lines"
BQ_STAGING = "_ledger_lines_staging"

OVERLAP_DAYS = 45          # fenêtre re-scannée chaque jour (corrections antidatées)
EARLY_STOP_BUFFER = 200    # lignes consécutives sous from_date avant d'arrêter


def transform_line(line: dict, ingested_at: str) -> dict:
    la = line.get("ledger_account") or {}
    lettered = (line.get("lettered_ledger_entry_lines") or {}).get("ids") or []
    return {
        "ledger_entry_line_id": int(line["id"]),
        "ledger_entry_id": int((line.get("ledger_entry") or {}).get("id") or 0) or None,
        "date": line.get("date"),
        "journal_id": int((line.get("journal") or {}).get("id") or 0) or None,
        "account_id": int(la.get("id") or 0) or None,
        "account_number": str(la.get("number") or "") or None,
        "label": line.get("label"),
        "debit": str(line.get("debit") or "0"),
        "credit": str(line.get("credit") or "0"),
        "lettered_line_ids": [int(x) for x in lettered],
        "created_at": line.get("created_at"),
        "updated_at": line.get("updated_at"),
        "ingested_at": ingested_at,
    }


def bq_schema() -> list[bigquery.SchemaField]:
    return [
        bigquery.SchemaField("ledger_entry_line_id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("ledger_entry_id", "INT64"),
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("journal_id", "INT64"),
        bigquery.SchemaField("account_id", "INT64"),
        bigquery.SchemaField("account_number", "STRING"),
        bigquery.SchemaField("label", "STRING"),
        bigquery.SchemaField("debit", "NUMERIC"),
        bigquery.SchemaField("credit", "NUMERIC"),
        bigquery.SchemaField("lettered_line_ids", "INT64", mode="REPEATED"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
    ]


def ensure_target_table(bq: bigquery.Client) -> None:
    table_ref = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    try:
        bq.get_table(table_ref)
        return
    except Exception:
        pass
    table = bigquery.Table(table_ref, schema=bq_schema())
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.MONTH, field="date"
    )
    table.clustering_fields = ["account_number"]
    bq.create_table(table)
    logger.info("Created table %s", table_ref)


def merge_to_bq(rows: list[dict], bq: bigquery.Client) -> dict:
    if not rows:
        return {"merged": 0, "skipped_empty": True}

    ensure_target_table(bq)
    staging = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_STAGING}"
    target = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=bq_schema(),
    )
    bq.load_table_from_json(rows, staging, job_config=job_config).result()

    cols = [f.name for f in bq_schema()]
    set_clause = ",\n      ".join(f"{c} = S.{c}" for c in cols if c != "ledger_entry_line_id")
    insert_cols = ", ".join(cols)
    insert_vals = ", ".join(f"S.{c}" for c in cols)
    merge_sql = f"""
    MERGE `{target}` T
    USING `{staging}` S
    ON T.ledger_entry_line_id = S.ledger_entry_line_id
    WHEN MATCHED THEN UPDATE SET
      {set_clause}
    WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """
    bq.query(merge_sql).result()
    bq.query(f"DROP TABLE `{staging}`").result()
    return {"merged": len(rows)}


def run(from_date: str | None = None, overlap_days: int = OVERLAP_DAYS,
        dry_run: bool = False) -> dict:
    token = os.environ.get("PENNYLANE_TOKEN")
    if not token:
        raise SystemExit("PENNYLANE_TOKEN env var required")

    if from_date is None:
        from_date = (datetime.now(timezone.utc).date() - timedelta(days=overlap_days)).isoformat()

    client = PennylaneGLClient(token)
    ingested_at = datetime.now(timezone.utc).isoformat()

    rows = []
    total = 0
    consecutive_below = 0
    for line in client.iter_ledger_lines(from_date, "9999-12-31"):
        total += 1
        d = line.get("date") or ""
        if d and d < from_date:
            consecutive_below += 1
            if consecutive_below >= EARLY_STOP_BUFFER:
                logger.info("early-stop at scan=%d : %d lignes consécutives date<%s",
                            total, EARLY_STOP_BUFFER, from_date)
                break
            continue
        consecutive_below = 0
        if not d or d < from_date:
            continue
        rows.append(transform_line(line, ingested_at))

    logger.info("from=%s : scanned=%d, kept=%d", from_date, total, len(rows))

    if dry_run:
        logger.info("DRY RUN — no BQ write")
        return {"from": from_date, "scanned": total, "kept": len(rows), "dry_run": True}

    bq = bigquery.Client(project=BQ_PROJECT)
    res = merge_to_bq(rows, bq)
    return {"from": from_date, "scanned": total, "kept": len(rows), **res}


def cli():
    parser = argparse.ArgumentParser()
    parser.add_argument("--from-date", help="YYYY-MM-DD (bootstrap/backfill ; sinon today-overlap)")
    parser.add_argument("--overlap-days", type=int, default=OVERLAP_DAYS)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    result = run(from_date=args.from_date, overlap_days=args.overlap_days, dry_run=args.dry_run)
    print(result)


if __name__ == "__main__":
    cli()
