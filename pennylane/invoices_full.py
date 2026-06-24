"""Pull PennyLane factures (clients + fournisseurs) → BQ `pennylane.raw_{customer,supplier}_invoices`.

Lot B du chantier "Pennylane → BQ" (cf. Archides CLAUDE.md). Frère de `ledger_full.py` :
même mécanique incrémentale, mais sur les endpoints `/customer_invoices` et
`/supplier_invoices`. Objectif : la page Finances passe du prisme PMS (Mews, DSO
faux) au prisme comptable réel (DSO/impayés/lettrage vrais), via le pivot
`ledger_entry_id` qui joint sur `raw_ledger_lines` (Lot A).

Mécanique incrémentale (l'API v2 n'autorise `sort` que sur `id`/`date`, PAS
`updated_at`) :
  - on tire en `sort=-date` DESC,
  - on garde toutes les factures `date >= from_date`,
  - early-stop dès qu'on passe sous `from_date` (overlap glissant),
  - MERGE sur `id` → les transitions de statut (impayé→payé, deadline) dans la
    fenêtre d'overlap sont rejouées (0 doublon).

⚠️ `OVERLAP_DAYS = 90` (vs 45 pour le ledger) : un paiement décale plus dans le
temps que l'écriture comptable, il faut une fenêtre plus large pour capter le
passage "payé". Les paiements au-delà de la fenêtre (rares) sont ratés — même
limite assumée que `ledger_full`.

Champs nested (`invoice_lines`, `payments`, `matched_transactions`) volontairement
DÉFÉRÉS : non requis pour DSO/impayés/lettrage (qui s'appuient sur les en-têtes +
`ledger_entry_id`). À brancher si besoin.

Usage CLI :
  PENNYLANE_TOKEN=xxx python -m pennylane.invoices_full                       # daily incr (today-90j), les 2 types
  PENNYLANE_TOKEN=xxx python -m pennylane.invoices_full --kind customer
  PENNYLANE_TOKEN=xxx python -m pennylane.invoices_full --from-date 2024-07-01   # bootstrap
  PENNYLANE_TOKEN=xxx python -m pennylane.invoices_full --dry-run
"""

import argparse
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Iterator

from google.cloud import bigquery

from .grand_livre import PENNYLANE_BASE, PennylaneGLClient

logger = logging.getLogger(__name__)

BQ_PROJECT = "merveil-data-warehouse"
BQ_DATASET = "pennylane"

OVERLAP_DAYS = 90          # fenêtre re-scannée chaque jour (transitions de statut)
EARLY_STOP_BUFFER = 200    # factures consécutives sous from_date avant d'arrêter

# kind -> (endpoint, table, party_field)
KINDS = {
    "customer": ("customer_invoices", "raw_customer_invoices", "customer"),
    "supplier": ("supplier_invoices", "raw_supplier_invoices", "supplier"),
}


def iter_invoices(client: PennylaneGLClient, endpoint: str) -> Iterator[dict]:
    """Itère les factures en ordre date DESC (early-stop friendly).

    L'API v2 n'accepte `sort` que sur `id`/`date` — on prend `-date` pour pouvoir
    arrêter dès qu'on sort de la fenêtre.
    """
    cursor = None
    page = 0
    while True:
        params = {"limit": 100, "sort": "-date"}
        if cursor:
            params["cursor"] = cursor
        d = client._get(f"{PENNYLANE_BASE}/{endpoint}", params=params)
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


def transform_invoice(inv: dict, party_field: str, ingested_at: str) -> dict:
    party = inv.get(party_field) or {}
    return {
        "id": int(inv["id"]),
        "ledger_entry_id": int((inv.get("ledger_entry") or {}).get("id") or 0) or None,
        "party_id": int(party.get("id") or 0) or None,
        "invoice_number": inv.get("invoice_number"),
        "date": inv.get("date"),
        "deadline": inv.get("deadline"),
        "amount": str(inv.get("amount") or "0"),
        "tax": str(inv.get("tax") or "0"),
        "currency": inv.get("currency"),
        "currency_amount": str(inv.get("currency_amount") or "0"),
        "status": inv.get("status") or inv.get("payment_status"),
        "paid": bool(inv.get("paid")),
        "remaining_amount_with_tax": str(inv.get("remaining_amount_with_tax") or "0"),
        "external_reference": inv.get("external_reference"),
        "label": inv.get("label"),
        "created_at": inv.get("created_at"),
        "updated_at": inv.get("updated_at"),
        "archived_at": inv.get("archived_at"),
        "ingested_at": ingested_at,
    }


def bq_schema() -> list[bigquery.SchemaField]:
    return [
        bigquery.SchemaField("id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("ledger_entry_id", "INT64"),
        bigquery.SchemaField("party_id", "INT64"),
        bigquery.SchemaField("invoice_number", "STRING"),
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("deadline", "DATE"),
        bigquery.SchemaField("amount", "NUMERIC"),
        bigquery.SchemaField("tax", "NUMERIC"),
        bigquery.SchemaField("currency", "STRING"),
        bigquery.SchemaField("currency_amount", "NUMERIC"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("paid", "BOOL"),
        bigquery.SchemaField("remaining_amount_with_tax", "NUMERIC"),
        bigquery.SchemaField("external_reference", "STRING"),
        bigquery.SchemaField("label", "STRING"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
        bigquery.SchemaField("archived_at", "TIMESTAMP"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
    ]


def ensure_target_table(bq: bigquery.Client, table: str) -> None:
    table_ref = f"{BQ_PROJECT}.{BQ_DATASET}.{table}"
    try:
        bq.get_table(table_ref)
        return
    except Exception:
        pass
    t = bigquery.Table(table_ref, schema=bq_schema())
    t.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.MONTH, field="date"
    )
    t.clustering_fields = ["paid"]
    bq.create_table(t)
    logger.info("Created table %s", table_ref)


def merge_to_bq(rows: list[dict], table: str, bq: bigquery.Client) -> dict:
    if not rows:
        return {"merged": 0, "skipped_empty": True}

    ensure_target_table(bq, table)
    staging = f"{BQ_PROJECT}.{BQ_DATASET}._{table}_staging"
    target = f"{BQ_PROJECT}.{BQ_DATASET}.{table}"

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


def run_kind(kind: str, from_date: str, client: PennylaneGLClient,
             ingested_at: str, dry_run: bool, bq: bigquery.Client | None) -> dict:
    endpoint, table, party_field = KINDS[kind]

    rows = []
    total = 0
    consecutive_below = 0
    for inv in iter_invoices(client, endpoint):
        total += 1
        d = inv.get("date") or ""
        if d and d < from_date:
            consecutive_below += 1
            if consecutive_below >= EARLY_STOP_BUFFER:
                logger.info("[%s] early-stop at scan=%d : %d consécutifs date<%s",
                            kind, total, EARLY_STOP_BUFFER, from_date)
                break
            continue
        consecutive_below = 0
        if not d or d < from_date:
            continue
        rows.append(transform_invoice(inv, party_field, ingested_at))

    logger.info("[%s] from=%s : scanned=%d, kept=%d", kind, from_date, total, len(rows))

    if dry_run:
        return {"kind": kind, "scanned": total, "kept": len(rows), "dry_run": True}

    res = merge_to_bq(rows, table, bq)
    return {"kind": kind, "scanned": total, "kept": len(rows), **res}


def run(kinds: list[str], from_date: str | None = None,
        overlap_days: int = OVERLAP_DAYS, dry_run: bool = False) -> dict:
    token = os.environ.get("PENNYLANE_TOKEN")
    if not token:
        raise SystemExit("PENNYLANE_TOKEN env var required")

    if from_date is None:
        from_date = (datetime.now(timezone.utc).date() - timedelta(days=overlap_days)).isoformat()

    client = PennylaneGLClient(token)
    ingested_at = datetime.now(timezone.utc).isoformat()
    bq = None if dry_run else bigquery.Client(project=BQ_PROJECT)

    results = [run_kind(k, from_date, client, ingested_at, dry_run, bq) for k in kinds]
    return {"from": from_date, "results": results}


def cli():
    parser = argparse.ArgumentParser()
    parser.add_argument("--kind", choices=["customer", "supplier", "both"], default="both")
    parser.add_argument("--from-date", help="YYYY-MM-DD (bootstrap/backfill ; sinon today-overlap)")
    parser.add_argument("--overlap-days", type=int, default=OVERLAP_DAYS)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    kinds = ["customer", "supplier"] if args.kind == "both" else [args.kind]
    result = run(kinds=kinds, from_date=args.from_date,
                 overlap_days=args.overlap_days, dry_run=args.dry_run)
    print(result)


if __name__ == "__main__":
    cli()
