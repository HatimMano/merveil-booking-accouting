"""Pull les EN-TÊTES d'écriture Pennylane → BQ `pennylane.raw_ledger_entries`.

Lot 3 du chantier « max data ». Le Lot A (`ledger_full.py`) ingère les LIGNES
d'écriture (`/ledger_entry_lines`), qui ne portent qu'un compte et un montant.
L'en-tête (`/ledger_entries`) porte ce qui manque pour lire une écriture :
`piece_number`, `invoice_number`, `due_date`, `status` (⭐ `validation_needed`
observé), les catégories analytiques, et surtout la présence d'un **justificatif**.

⭐ `attachment` et `ledger_attachment_filename` sont **inline dans le list** — sondé
le 25/08 : aucun N+1 nécessaire (6 écritures sur 100 en portaient un en août). C'est
ce qui rend cette brique quasi gratuite alors qu'elle alimente directement le chantier
« détection d'erreurs de saisie » : écriture sans justificatif, écriture jamais validée.

Mécanique : identique à `transactions.py` — filtre serveur `date` en syntaxe LISTE
(`updated_at` n'est PAS filtrable), overlap glissant 45 j aligné sur le Lot A pour
re-capter les corrections antidatées, MERGE sur `id`.

Usage CLI :
  PENNYLANE_TOKEN=xxx python -m pennylane.ledger_entries                        # daily (45j)
  PENNYLANE_TOKEN=xxx python -m pennylane.ledger_entries --from-date 2024-01-01 # backfill
  PENNYLANE_TOKEN=xxx python -m pennylane.ledger_entries --dry-run
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
BQ_TABLE = "raw_ledger_entries"

OVERLAP_DAYS = 45  # aligné sur ledger_full.py (Lot A) — même univers d'écritures


def iter_entries(client: PennylaneGLClient, from_date: str,
                 to_date: str | None = None) -> Iterator[dict]:
    filt = [{"field": "date", "operator": "gteq", "value": from_date}]
    if to_date:
        filt.append({"field": "date", "operator": "lteq", "value": to_date})
    filter_json = json.dumps(filt)

    cursor, page = None, 0
    while True:
        params = {"limit": 100, "filter": filter_json}
        if cursor:
            params["cursor"] = cursor
        d = client._get(f"{PENNYLANE_BASE}/ledger_entries", params=params)
        items = d.get("items", [])
        for item in items:
            yield item
        page += 1
        if page % 50 == 0:
            logger.info("paginated %d pages", page)
        cursor = d.get("next_cursor")
        if not d.get("has_more") or not cursor or not items:
            return


def _nested_id(obj) -> int | None:
    return int(obj["id"]) if obj and obj.get("id") is not None else None


def transform_entry(e: dict, ingested_at: str) -> dict:
    att = e.get("attachment")
    # `attachment` est tantôt un objet (avec url/filename), tantôt un booléen selon
    # le contexte — on ne garde que le fait qu'il existe + le nom de fichier, jamais
    # l'URL (signée, expire vite : même piège que `public_file_url`, cf. Phase 4).
    return {
        "id": int(e["id"]),
        "date": e.get("date"),
        "label": e.get("label"),
        "piece_number": e.get("piece_number") or None,
        "invoice_number": e.get("invoice_number") or None,
        "due_date": e.get("due_date"),
        "status": e.get("status"),
        "journal_id": e.get("journal_id") or _nested_id(e.get("journal")),
        "has_attachment": bool(att),
        "attachment_filename": e.get("ledger_attachment_filename")
                               or (att.get("filename") if isinstance(att, dict) else None),
        "categories": [
            {
                "id": _nested_id(c),
                "label": c.get("label"),
                "analytical_code": c.get("analytical_code"),
                "category_group_id": _nested_id(c.get("category_group")),
            }
            for c in (e.get("categories") or [])
        ],
        "created_at": e.get("created_at"),
        "updated_at": e.get("updated_at"),
        "ingested_at": ingested_at,
        "_raw_payload": json.dumps(e, ensure_ascii=False),
    }


def bq_schema() -> list[bigquery.SchemaField]:
    SF = bigquery.SchemaField
    return [
        SF("id", "INT64", mode="REQUIRED"),
        SF("date", "DATE"),
        SF("label", "STRING"),
        SF("piece_number", "STRING"),
        SF("invoice_number", "STRING"),
        SF("due_date", "DATE"),
        SF("status", "STRING"),
        SF("journal_id", "INT64"),
        SF("has_attachment", "BOOL"),
        SF("attachment_filename", "STRING"),
        SF("categories", "RECORD", mode="REPEATED", fields=[
            SF("id", "INT64"), SF("label", "STRING"),
            SF("analytical_code", "STRING"), SF("category_group_id", "INT64"),
        ]),
        SF("created_at", "TIMESTAMP"),
        SF("updated_at", "TIMESTAMP"),
        SF("ingested_at", "TIMESTAMP", mode="REQUIRED"),
        SF("_raw_payload", "STRING"),
    ]


def ensure_target_table(bq: bigquery.Client) -> None:
    ref = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    try:
        bq.get_table(ref)
        return
    except Exception:
        pass
    t = bigquery.Table(ref, schema=bq_schema())
    t.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.MONTH, field="date"
    )
    t.clustering_fields = ["journal_id", "status"]
    bq.create_table(t)
    logger.info("Created table %s", ref)


def merge_to_bq(rows: list[dict], bq: bigquery.Client) -> dict:
    if not rows:
        return {"merged": 0, "skipped_empty": True}

    ensure_target_table(bq)
    staging = f"{BQ_PROJECT}.{BQ_DATASET}._{BQ_TABLE}_staging"
    target = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    bq.load_table_from_json(
        rows, staging,
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, schema=bq_schema(),
        ),
    ).result()

    cols = [f.name for f in bq_schema()]
    set_clause = ",\n      ".join(f"{c} = S.{c}" for c in cols if c != "id")
    bq.query(f"""
    MERGE `{target}` T
    USING `{staging}` S
    ON T.id = S.id
    WHEN MATCHED THEN UPDATE SET
      {set_clause}
    WHEN NOT MATCHED THEN INSERT ({", ".join(cols)}) VALUES ({", ".join("S." + c for c in cols)})
    """).result()
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

    rows = [transform_entry(e, ingested_at) for e in iter_entries(client, from_date, to_date)]
    n_att = sum(1 for r in rows if r["has_attachment"])
    logger.info("from=%s to=%s : fetched=%d (avec justificatif : %d)",
                from_date, to_date or "∞", len(rows), n_att)

    if dry_run:
        sample = {k: v for k, v in rows[0].items() if k != "_raw_payload"} if rows else None
        return {"from": from_date, "fetched": len(rows), "with_attachment": n_att,
                "dry_run": True, "sample": sample}

    bq = bigquery.Client(project=BQ_PROJECT)
    return {"from": from_date, "fetched": len(rows), "with_attachment": n_att,
            **merge_to_bq(rows, bq)}


def cli():
    parser = argparse.ArgumentParser()
    parser.add_argument("--from-date", help="YYYY-MM-DD (backfill ; sinon today-overlap)")
    parser.add_argument("--to-date", help="YYYY-MM-DD (borne haute optionnelle)")
    parser.add_argument("--overlap-days", type=int, default=OVERLAP_DAYS)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    print(run(from_date=args.from_date, to_date=args.to_date,
              overlap_days=args.overlap_days, dry_run=args.dry_run))


if __name__ == "__main__":
    cli()
