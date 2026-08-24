"""Pull PennyLane transactions bancaires → BQ `pennylane.raw_transactions`.

Chantier « Pennylane max data » (cf. Archides/docs/plans/pennylane-max-data-2026-08-21.md).
LE manque n°1 identifié par les sondes du 21-24/08 : les mouvements bancaires réels
(« combien on a payé à Leroy Merlin ? »), avec la ventilation analytique INLINE au
niveau transaction (contrairement aux factures où c'est un N+1 — cf. Lot 2).

Mécanique — DIFFÉRENTE des Lots A/B (pas de sort=-date + early-stop ici) :
  - `/transactions` n'accepte `sort` que sur `id`, MAIS supporte un filtre serveur
    `date` avec la syntaxe LISTE (découverte Phase 0 du 24/08) :
        filter=[{"field":"date","operator":"gteq","value":"2026-08-01"}]
    Vérifié réellement appliqué (fenêtre 7j → 239 lignes, pas 26 933).
  - On tire donc la fenêtre voulue directement côté serveur, cursor jusqu'au bout.
  - MERGE sur `id` → les rapprochements tardifs (supplier/customer matchés après
    coup, catégorisation analytique) sont rejoués dans la fenêtre d'overlap.

⚠️ `updated_at` n'est PAS filtrable (400) → l'overlap glissant garde tout son sens :
une transaction rapprochée/catégorisée après coup n'est re-capturée que si sa DATE
retombe dans la fenêtre re-scannée. OVERLAP_DAYS=45, aligné sur le ledger (Lot A).

Volumétrie mesurée le 24/08 : 26 933 transactions au total (2024-01-03 → today),
backfill intégral ~2 min d'API. Le daily 45j ≈ 15-20 pages.

Usage CLI :
  PENNYLANE_TOKEN=xxx python -m pennylane.transactions                        # daily incr (today-45j)
  PENNYLANE_TOKEN=xxx python -m pennylane.transactions --from-date 2024-01-01 # backfill intégral
  PENNYLANE_TOKEN=xxx python -m pennylane.transactions --dry-run
"""

import argparse
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Iterator

from google.cloud import bigquery

from .grand_livre import PENNYLANE_BASE, PennylaneGLClient

logger = logging.getLogger(__name__)

BQ_PROJECT = "merveil-data-warehouse"
BQ_DATASET = "pennylane"
BQ_TABLE = "raw_transactions"

OVERLAP_DAYS = 45  # fenêtre re-scannée chaque jour (rapprochements/catégorisations tardifs)


def iter_transactions(client: PennylaneGLClient, from_date: str,
                      to_date: str | None = None) -> Iterator[dict]:
    """Itère les transactions de la fenêtre [from_date, to_date] via filtre serveur."""
    filt = [{"field": "date", "operator": "gteq", "value": from_date}]
    if to_date:
        filt.append({"field": "date", "operator": "lteq", "value": to_date})
    filter_json = json.dumps(filt)

    cursor = None
    page = 0
    while True:
        params = {"limit": 100, "filter": filter_json}
        if cursor:
            params["cursor"] = cursor
        d = client._get(f"{PENNYLANE_BASE}/transactions", params=params)
        for item in d.get("items", []):
            yield item
        page += 1
        if page % 20 == 0:
            logger.info("paginated %d pages", page)
        if not d.get("has_more"):
            return
        cursor = d.get("next_cursor")
        if not cursor:
            return


def _num(v) -> str | None:
    return str(v) if v is not None else None


def _nested_id(obj) -> int | None:
    return int(obj["id"]) if obj and obj.get("id") is not None else None


def transform_transaction(tx: dict, ingested_at: str) -> dict:
    pro = tx.get("pro_account_expense") or {}
    emp = pro.get("employee") or {}
    emp_name = " ".join(p for p in (emp.get("first_name"), emp.get("last_name")) if p) or None
    return {
        "id": int(tx["id"]),
        "date": tx.get("date"),
        "amount": _num(tx.get("amount")) or "0",
        "fee": _num(tx.get("fee")),
        "currency": tx.get("currency"),
        "currency_amount": _num(tx.get("currency_amount")),
        "currency_fee": _num(tx.get("currency_fee")),
        "label": tx.get("label"),
        "bank_account_id": _nested_id(tx.get("bank_account")),
        "journal_id": _nested_id(tx.get("journal")),
        "supplier_id": _nested_id(tx.get("supplier")),
        "customer_id": _nested_id(tx.get("customer")),
        "outstanding_balance": _num(tx.get("outstanding_balance")),
        "interbank_code": tx.get("interbank_code"),
        "attachment_required": bool(tx.get("attachment_required")),
        "pro_account_employee": emp_name,
        "pro_account_card": pro.get("card_masked_number"),
        "categories": [
            {
                "id": _nested_id(c),
                "label": c.get("label"),
                "weight": _num(c.get("weight")),
                "analytical_code": c.get("analytical_code"),
                "category_group_id": _nested_id(c.get("category_group")),
            }
            for c in (tx.get("categories") or [])
        ],
        "archived_at": tx.get("archived_at"),
        "created_at": tx.get("created_at"),
        "updated_at": tx.get("updated_at"),
        "ingested_at": ingested_at,
        "_raw_payload": json.dumps(tx, ensure_ascii=False),
    }


def bq_schema() -> list[bigquery.SchemaField]:
    return [
        bigquery.SchemaField("id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("amount", "NUMERIC"),
        bigquery.SchemaField("fee", "NUMERIC"),
        bigquery.SchemaField("currency", "STRING"),
        bigquery.SchemaField("currency_amount", "NUMERIC"),
        bigquery.SchemaField("currency_fee", "NUMERIC"),
        bigquery.SchemaField("label", "STRING"),
        bigquery.SchemaField("bank_account_id", "INT64"),
        bigquery.SchemaField("journal_id", "INT64"),
        bigquery.SchemaField("supplier_id", "INT64"),
        bigquery.SchemaField("customer_id", "INT64"),
        bigquery.SchemaField("outstanding_balance", "NUMERIC"),
        bigquery.SchemaField("interbank_code", "STRING"),
        bigquery.SchemaField("attachment_required", "BOOL"),
        bigquery.SchemaField("pro_account_employee", "STRING"),
        bigquery.SchemaField("pro_account_card", "STRING"),
        bigquery.SchemaField(
            "categories", "RECORD", mode="REPEATED",
            fields=[
                bigquery.SchemaField("id", "INT64"),
                bigquery.SchemaField("label", "STRING"),
                bigquery.SchemaField("weight", "NUMERIC"),
                bigquery.SchemaField("analytical_code", "STRING"),
                bigquery.SchemaField("category_group_id", "INT64"),
            ],
        ),
        bigquery.SchemaField("archived_at", "TIMESTAMP"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("_raw_payload", "STRING"),
    ]


def ensure_target_table(bq: bigquery.Client) -> None:
    table_ref = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    try:
        bq.get_table(table_ref)
        return
    except Exception:
        pass
    t = bigquery.Table(table_ref, schema=bq_schema())
    t.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.MONTH, field="date"
    )
    t.clustering_fields = ["bank_account_id"]
    bq.create_table(t)
    logger.info("Created table %s", table_ref)


def merge_to_bq(rows: list[dict], bq: bigquery.Client) -> dict:
    if not rows:
        return {"merged": 0, "skipped_empty": True}

    ensure_target_table(bq)
    staging = f"{BQ_PROJECT}.{BQ_DATASET}._{BQ_TABLE}_staging"
    target = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=bq_schema(),
    )
    bq.load_table_from_json(rows, staging, job_config=job_config).result()

    cols = [f.name for f in bq_schema()]
    set_clause = ",\n      ".join(f"{c} = S.{c}" for c in cols if c != "id")
    insert_cols = ", ".join(cols)
    insert_vals = ", ".join(f"S.{c}" for c in cols)
    merge_sql = f"""
    MERGE `{target}` T
    USING `{staging}` S
    ON T.id = S.id
    WHEN MATCHED THEN UPDATE SET
      {set_clause}
    WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """
    bq.query(merge_sql).result()
    bq.query(f"DROP TABLE `{staging}`").result()
    return {"merged": len(rows)}


def run(from_date: str | None = None, to_date: str | None = None,
        overlap_days: int = OVERLAP_DAYS, dry_run: bool = False) -> dict:
    token = os.environ.get("PENNYLANE_TOKEN")
    if not token:
        raise SystemExit("PENNYLANE_TOKEN env var required")

    if from_date is None:
        from_date = (datetime.now(timezone.utc).date() - timedelta(days=overlap_days)).isoformat()

    client = PennylaneGLClient(token)
    ingested_at = datetime.now(timezone.utc).isoformat()

    rows = [transform_transaction(tx, ingested_at)
            for tx in iter_transactions(client, from_date, to_date)]
    logger.info("from=%s to=%s : fetched=%d", from_date, to_date or "∞", len(rows))

    if dry_run:
        sample = {k: v for k, v in rows[0].items() if k != "_raw_payload"} if rows else None
        return {"from": from_date, "to": to_date, "fetched": len(rows),
                "dry_run": True, "sample": sample}

    bq = bigquery.Client(project=BQ_PROJECT)
    res = merge_to_bq(rows, bq)
    return {"from": from_date, "to": to_date, "fetched": len(rows), **res}


def cli():
    parser = argparse.ArgumentParser()
    parser.add_argument("--from-date", help="YYYY-MM-DD (backfill ; sinon today-overlap)")
    parser.add_argument("--to-date", help="YYYY-MM-DD (borne haute optionnelle)")
    parser.add_argument("--overlap-days", type=int, default=OVERLAP_DAYS)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    result = run(from_date=args.from_date, to_date=args.to_date,
                 overlap_days=args.overlap_days, dry_run=args.dry_run)
    print(result)


if __name__ == "__main__":
    cli()
