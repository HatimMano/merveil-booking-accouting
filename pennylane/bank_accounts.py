"""Pull PennyLane soldes bancaires → BQ `pennylane.raw_bank_accounts` (snapshot append-only).

Brique trésorerie. L'endpoint `/bank_accounts` expose le **solde réel** par compte
(Pennylane est connecté aux banques) + le nom + le lien `ledger_account` (= jointure
directe vers les transactions `raw_ledger_lines`, Lot A).

Snapshot **append-only** (1 ligne / compte / run) → solde courant = dernier snapshot,
historique = trend dans le temps + base pour l'alerte seuil. ~5 comptes, daily.

Usage CLI :
  PENNYLANE_TOKEN=xxx python -m pennylane.bank_accounts
  PENNYLANE_TOKEN=xxx python -m pennylane.bank_accounts --dry-run
"""

import argparse
import logging
import os
from datetime import datetime, timezone

from google.cloud import bigquery

from .grand_livre import PENNYLANE_BASE, PennylaneGLClient

logger = logging.getLogger(__name__)

BQ_PROJECT = "merveil-data-warehouse"
BQ_DATASET = "pennylane"
BQ_TABLE = "raw_bank_accounts"


def iter_bank_accounts(client: PennylaneGLClient):
    cursor = None
    while True:
        params = {"limit": 100}
        if cursor:
            params["cursor"] = cursor
        d = client._get(f"{PENNYLANE_BASE}/bank_accounts", params=params)
        for item in d.get("items", []):
            yield item
        if not d.get("has_more"):
            return
        cursor = d.get("next_cursor")
        if not cursor:
            return


def transform(acc: dict, snapshot_at: str) -> dict:
    la = acc.get("ledger_account") or {}
    return {
        "snapshot_at": snapshot_at,
        "account_id": int(acc["id"]),
        "name": acc.get("name"),
        "currency": acc.get("currency"),
        "balance": str(acc.get("balance") or "0"),
        "ledger_account_id": int(la.get("id") or 0) or None,
        "bank_establishment_id": int((acc.get("bank_establishment") or {}).get("id") or 0) or None,
        "updated_at": acc.get("updated_at"),
    }


def bq_schema() -> list[bigquery.SchemaField]:
    return [
        bigquery.SchemaField("snapshot_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("account_id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("currency", "STRING"),
        bigquery.SchemaField("balance", "NUMERIC"),
        bigquery.SchemaField("ledger_account_id", "INT64"),
        bigquery.SchemaField("bank_establishment_id", "INT64"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
    ]


def ensure_table(bq: bigquery.Client) -> None:
    ref = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    try:
        bq.get_table(ref)
        return
    except Exception:
        pass
    t = bigquery.Table(ref, schema=bq_schema())
    t.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY, field="snapshot_at"
    )
    t.clustering_fields = ["account_id"]
    bq.create_table(t)
    logger.info("Created table %s", ref)


def run(dry_run: bool = False) -> dict:
    token = os.environ.get("PENNYLANE_TOKEN")
    if not token:
        raise SystemExit("PENNYLANE_TOKEN env var required")

    client = PennylaneGLClient(token)
    snapshot_at = datetime.now(timezone.utc).isoformat()
    rows = [transform(a, snapshot_at) for a in iter_bank_accounts(client)]
    logger.info("fetched %d bank accounts", len(rows))

    if dry_run:
        return {"fetched": len(rows), "rows": rows, "dry_run": True}

    bq = bigquery.Client(project=BQ_PROJECT)
    ensure_table(bq)
    bq.load_table_from_json(
        rows,
        f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}",
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=bq_schema(),
        ),
    ).result()
    return {"appended": len(rows)}


def cli():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    print(run(dry_run=args.dry_run))


if __name__ == "__main__":
    cli()
