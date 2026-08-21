"""Pull les RÉFÉRENTIELS Pennylane → BQ `pennylane.raw_{suppliers,customers,ledger_accounts}`.

Lot 1 du chantier contrôle de gestion (demande Mickael 21/08). Le grand livre (Lot A)
et les factures (Lot B) sont en base depuis fin juin, mais **nus** : une ligne d'écriture
ne porte qu'un `account_number` et une facture qu'un `party_id`. Sans les libellés, le
grand livre est illisible pour qui n'a pas le plan comptable en tête.

Ces 3 endpoints sont des référentiels : petits (~1 à 2 k lignes), sans notion de date.
On tire donc TOUT à chaque run et on MERGE sur `id` — pas d'incrémental, pas de curseur.

⚠️ Le MERGE ne supprime jamais : un tiers supprimé côté Pennylane reste en base, ce qui
est voulu — les écritures anciennes qui le référencent doivent rester joignables.

⚠️ Ces référentiels ne portent PAS le code analytique. Dans l'API v2, la ventilation
analytique est accrochée à la FACTURE (`/supplier_invoices/{id}/categories`), jamais à la
ligne d'écriture (`ledger_entry_lines.categories` est vide sur 100 % des lignes scannées
en août 2026). C'est le lot 2, et il attend que la saisie côté Pennylane se densifie
(~1 facture fournisseur sur 3 catégorisée au 21/08).

Usage CLI :
  PENNYLANE_TOKEN=xxx python -m pennylane.references
  PENNYLANE_TOKEN=xxx python -m pennylane.references --kind suppliers
  PENNYLANE_TOKEN=xxx python -m pennylane.references --dry-run
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

SF = bigquery.SchemaField


def _addr(obj: dict, key: str) -> dict:
    a = obj.get(key) or {}
    return {
        "address": a.get("address") or None,
        "postal_code": a.get("postal_code") or None,
        "city": a.get("city") or None,
        "country_alpha2": a.get("country_alpha2") or None,
    }


def _ledger_account_id(obj: dict):
    return int((obj.get("ledger_account") or {}).get("id") or 0) or None


def transform_supplier(s: dict, ingested_at: str) -> dict:
    return {
        "id": int(s["id"]),
        "name": s.get("name"),
        "reg_no": s.get("reg_no") or None,
        "vat_number": s.get("vat_number") or None,
        "iban": s.get("iban") or None,
        "ledger_account_id": _ledger_account_id(s),
        "emails": [e for e in (s.get("emails") or []) if e],
        "payment_method": s.get("supplier_payment_method"),
        "due_date_delay": s.get("supplier_due_date_delay"),
        "due_date_rule": s.get("supplier_due_date_rule"),
        "external_reference": s.get("external_reference") or None,
        **_addr(s, "postal_address"),
        "created_at": s.get("created_at"),
        "updated_at": s.get("updated_at"),
        "ingested_at": ingested_at,
    }


def transform_customer(c: dict, ingested_at: str) -> dict:
    return {
        "id": int(c["id"]),
        "name": c.get("name"),
        "customer_type": c.get("customer_type"),
        "reg_no": c.get("reg_no") or None,
        "vat_number": c.get("vat_number") or None,
        "phone": c.get("phone") or None,
        "payment_conditions": c.get("payment_conditions"),
        "ledger_account_id": _ledger_account_id(c),
        "emails": [e for e in (c.get("emails") or []) if e],
        "external_reference": c.get("external_reference") or None,
        **_addr(c, "billing_address"),
        "created_at": c.get("created_at"),
        "updated_at": c.get("updated_at"),
        "ingested_at": ingested_at,
    }


def transform_ledger_account(a: dict, ingested_at: str) -> dict:
    return {
        "id": int(a["id"]),
        "number": str(a.get("number") or "") or None,
        "label": a.get("label"),
        "type": a.get("type"),
        "vat_rate": a.get("vat_rate"),
        "country_alpha2": a.get("country_alpha2"),
        "enabled": a.get("enabled"),
        "letterable": a.get("letterable"),
        "created_at": a.get("created_at"),
        "updated_at": a.get("updated_at"),
        "ingested_at": ingested_at,
    }


_ADDR_FIELDS = [
    SF("address", "STRING"), SF("postal_code", "STRING"),
    SF("city", "STRING"), SF("country_alpha2", "STRING"),
]

SUPPLIER_SCHEMA = [
    SF("id", "INT64", mode="REQUIRED"), SF("name", "STRING"),
    SF("reg_no", "STRING"), SF("vat_number", "STRING"), SF("iban", "STRING"),
    SF("ledger_account_id", "INT64"), SF("emails", "STRING", mode="REPEATED"),
    SF("payment_method", "STRING"), SF("due_date_delay", "INT64"),
    SF("due_date_rule", "STRING"), SF("external_reference", "STRING"),
    *_ADDR_FIELDS,
    SF("created_at", "TIMESTAMP"), SF("updated_at", "TIMESTAMP"),
    SF("ingested_at", "TIMESTAMP", mode="REQUIRED"),
]

CUSTOMER_SCHEMA = [
    SF("id", "INT64", mode="REQUIRED"), SF("name", "STRING"),
    SF("customer_type", "STRING"), SF("reg_no", "STRING"),
    SF("vat_number", "STRING"), SF("phone", "STRING"),
    SF("payment_conditions", "STRING"), SF("ledger_account_id", "INT64"),
    SF("emails", "STRING", mode="REPEATED"), SF("external_reference", "STRING"),
    *_ADDR_FIELDS,
    SF("created_at", "TIMESTAMP"), SF("updated_at", "TIMESTAMP"),
    SF("ingested_at", "TIMESTAMP", mode="REQUIRED"),
]

LEDGER_ACCOUNT_SCHEMA = [
    SF("id", "INT64", mode="REQUIRED"), SF("number", "STRING"),
    SF("label", "STRING"), SF("type", "STRING"), SF("vat_rate", "STRING"),
    SF("country_alpha2", "STRING"), SF("enabled", "BOOL"),
    SF("letterable", "BOOL"),
    SF("created_at", "TIMESTAMP"), SF("updated_at", "TIMESTAMP"),
    SF("ingested_at", "TIMESTAMP", mode="REQUIRED"),
]

# kind -> (endpoint, table, transform, schema, clustering)
KINDS = {
    "suppliers": ("/suppliers", "raw_suppliers", transform_supplier, SUPPLIER_SCHEMA, ["name"]),
    "customers": ("/customers", "raw_customers", transform_customer, CUSTOMER_SCHEMA, ["name"]),
    "ledger_accounts": ("/ledger_accounts", "raw_ledger_accounts", transform_ledger_account,
                        LEDGER_ACCOUNT_SCHEMA, ["number"]),
}


def iter_all(client: PennylaneGLClient, endpoint: str):
    cursor = None
    while True:
        params = {"limit": 100}
        if cursor:
            params["cursor"] = cursor
        d = client._get(f"{PENNYLANE_BASE}{endpoint}", params=params)
        items = d.get("items", [])
        for item in items:
            yield item
        cursor = d.get("next_cursor")
        if not d.get("has_more") or not cursor or not items:
            return


def ensure_table(bq: bigquery.Client, table: str, schema, clustering) -> None:
    ref = f"{BQ_PROJECT}.{BQ_DATASET}.{table}"
    try:
        bq.get_table(ref)
        return
    except Exception:
        pass
    t = bigquery.Table(ref, schema=schema)
    t.clustering_fields = clustering
    bq.create_table(t)
    logger.info("Created table %s", ref)


def merge_to_bq(rows: list[dict], table: str, schema, clustering, bq: bigquery.Client) -> dict:
    if not rows:
        return {"merged": 0, "skipped_empty": True}

    ensure_table(bq, table, schema, clustering)
    staging = f"{BQ_PROJECT}.{BQ_DATASET}._{table}_staging"
    target = f"{BQ_PROJECT}.{BQ_DATASET}.{table}"

    bq.load_table_from_json(
        rows, staging,
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, schema=schema,
        ),
    ).result()

    cols = [f.name for f in schema]
    set_clause = ",\n      ".join(f"{c} = S.{c}" for c in cols if c != "id")
    merge_sql = f"""
    MERGE `{target}` T
    USING `{staging}` S
    ON T.id = S.id
    WHEN MATCHED THEN UPDATE SET
      {set_clause}
    WHEN NOT MATCHED THEN INSERT ({", ".join(cols)}) VALUES ({", ".join("S." + c for c in cols)})
    """
    bq.query(merge_sql).result()
    bq.query(f"DROP TABLE `{staging}`").result()
    return {"merged": len(rows)}


def run_kind(kind: str, client: PennylaneGLClient, ingested_at: str,
             dry_run: bool, bq: bigquery.Client | None) -> dict:
    endpoint, table, transform, schema, clustering = KINDS[kind]
    rows = [transform(item, ingested_at) for item in iter_all(client, endpoint)]
    logger.info("[%s] fetched=%d", kind, len(rows))

    if dry_run:
        return {"kind": kind, "fetched": len(rows), "sample": rows[:2], "dry_run": True}
    return {"kind": kind, "fetched": len(rows), **merge_to_bq(rows, table, schema, clustering, bq)}


def run(kinds: list[str], dry_run: bool = False) -> dict:
    token = os.environ.get("PENNYLANE_TOKEN")
    if not token:
        raise SystemExit("PENNYLANE_TOKEN env var required")

    client = PennylaneGLClient(token)
    ingested_at = datetime.now(timezone.utc).isoformat()
    bq = None if dry_run else bigquery.Client(project=BQ_PROJECT)
    return {"results": [run_kind(k, client, ingested_at, dry_run, bq) for k in kinds]}


def cli():
    parser = argparse.ArgumentParser()
    parser.add_argument("--kind", choices=[*KINDS, "all"], default="all")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    kinds = list(KINDS) if args.kind == "all" else [args.kind]
    print(run(kinds, dry_run=args.dry_run))


if __name__ == "__main__":
    cli()
