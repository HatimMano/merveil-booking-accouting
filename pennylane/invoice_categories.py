"""Pull la VENTILATION ANALYTIQUE par facture → BQ `pennylane.raw_invoice_categories`.

Lot 2 du chantier contrôle de gestion (cf. ADR 2026-08-21). Dans l'API v2 le code
analytique est accroché à la FACTURE (`/{supplier,customer}_invoices/{id}/categories`),
jamais à la ligne d'écriture — d'où 1 appel API PAR facture, la seule volumétrie
coûteuse du chantier. Couverture réelle de la saisie : 98-100 % des factures
comptabilisées (mesure corrigée du 21/08 — le « 26-38 % » était un artefact de sonde).

Mécanique :
  - la liste des factures à sonder vient de BQ (`raw_{supplier,customer}_invoices`),
    fenêtre `--days N` sur `date` (défaut 90, aligné sur l'overlap du Lot B) ou
    `--all` pour le backfill ;
  - sondes avec retry sur 429 (backoff), petit pool de threads ;
  - ⚠️ une facture dont la sonde ÉCHOUE après retries sort du lot : elle n'est ni
    écrite ni purgée (sinon on effacerait ses assignations existantes sur un
    incident réseau). Une erreur ne va jamais au dénominateur.
  - écriture par remplacement : staging (1 ligne par facture sondée, catégorie
    NULL si la facture n'en porte pas) → DELETE des factures sondées dans la
    cible → INSERT des assignations. Une catégorie retirée côté Pennylane
    disparaît donc au run suivant.

Usage CLI :
  PENNYLANE_TOKEN=xxx python -m pennylane.invoice_categories              # daily (90j)
  PENNYLANE_TOKEN=xxx python -m pennylane.invoice_categories --days 30
  PENNYLANE_TOKEN=xxx python -m pennylane.invoice_categories --all        # backfill
  PENNYLANE_TOKEN=xxx python -m pennylane.invoice_categories --kind supplier --dry-run
"""

import argparse
import concurrent.futures as cf
import logging
import os
import threading
import time
from datetime import datetime, timezone

from google.cloud import bigquery

from .grand_livre import PENNYLANE_BASE, PennylaneGLClient

logger = logging.getLogger(__name__)

BQ_PROJECT = "merveil-data-warehouse"
BQ_DATASET = "pennylane"
BQ_TABLE = "raw_invoice_categories"
BQ_STAGING = "_invoice_categories_staging"

# kind -> (endpoint API, table BQ source des ids)
KINDS = {
    "supplier": ("supplier_invoices", "raw_supplier_invoices"),
    "customer": ("customer_invoices", "raw_customer_invoices"),
}

WORKERS = 4          # sondes parallèles — modeste, l'API 429 vite
MAX_ATTEMPTS = 8     # retries backoff sur 429 / erreurs transitoires

SF = bigquery.SchemaField


def bq_schema() -> list[bigquery.SchemaField]:
    return [
        SF("invoice_kind", "STRING", mode="REQUIRED"),   # supplier | customer
        SF("invoice_id", "INT64", mode="REQUIRED"),
        SF("category_id", "INT64"),                      # NULL en staging = facture sans catégorie
        SF("category_label", "STRING"),                  # dénormalisé (résiste à une catégorie supprimée)
        SF("category_group_id", "INT64"),
        SF("weight", "NUMERIC"),
        SF("ingested_at", "TIMESTAMP", mode="REQUIRED"),
    ]


class _Prober:
    """Sonde thread-safe avec retry 429. Chaque worker a sa session."""

    def __init__(self, token: str):
        self._token = token
        self._local = threading.local()

    def _client(self) -> PennylaneGLClient:
        if not hasattr(self._local, "c"):
            self._local.c = PennylaneGLClient(self._token)
        return self._local.c

    def probe(self, endpoint: str, invoice_id: int):
        """→ liste d'items, ou None si échec définitif (facture à écarter du lot)."""
        for attempt in range(MAX_ATTEMPTS):
            try:
                d = self._client()._get(
                    f"{PENNYLANE_BASE}/{endpoint}/{invoice_id}/categories",
                    params={"limit": 100},
                )
                return d.get("items", [])
            except Exception as e:
                # Le client gère déjà les 429 avec backoff ; ici on rattrape les
                # épuisements de retries / erreurs transitoires résiduelles.
                if attempt == MAX_ATTEMPTS - 1:
                    logger.warning("probe KO %s/%s : %s", endpoint, invoice_id, str(e)[:120])
                    return None
                time.sleep(1.0 * (attempt + 1))
        return None


def invoice_ids(bq: bigquery.Client, kind: str, days: int | None) -> list[int]:
    _, table = KINDS[kind]
    where = "" if days is None else f"WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL {int(days)} DAY)"
    q = f"SELECT id FROM `{BQ_PROJECT}.{BQ_DATASET}.{table}` {where}"
    return [r.id for r in bq.query(q).result()]


def run_kind(kind: str, days: int | None, prober: _Prober, bq: bigquery.Client,
             ingested_at: str, dry_run: bool) -> dict:
    endpoint, _ = KINDS[kind]
    ids = invoice_ids(bq, kind, days)
    logger.info("[%s] %d factures à sonder (fenêtre=%s)", kind, len(ids), days or "ALL")

    rows, skipped, done = [], 0, 0
    lock = threading.Lock()

    def work(iid: int):
        nonlocal skipped, done
        items = prober.probe(endpoint, iid)
        with lock:
            done += 1
            if done % 500 == 0:
                logger.info("[%s] %d/%d sondées (%d KO)", kind, done, len(ids), skipped)
        if items is None:
            with lock:
                skipped += 1
            return
        out = []
        if items:
            for it in items:
                out.append({
                    "invoice_kind": kind,
                    "invoice_id": int(iid),
                    "category_id": int(it["id"]),
                    "category_label": it.get("label"),
                    "category_group_id": int((it.get("category_group") or {}).get("id") or 0) or None,
                    "weight": str(it.get("weight") or "1"),
                    "ingested_at": ingested_at,
                })
        else:
            # Facture sondée SANS catégorie : ligne sentinelle (category_id NULL)
            # pour que le DELETE purge d'éventuelles assignations retirées.
            out.append({
                "invoice_kind": kind, "invoice_id": int(iid),
                "category_id": None, "category_label": None,
                "category_group_id": None, "weight": None,
                "ingested_at": ingested_at,
            })
        with lock:
            rows.extend(out)

    with cf.ThreadPoolExecutor(WORKERS) as ex:
        list(ex.map(work, ids))

    n_assign = sum(1 for r in rows if r["category_id"] is not None)
    n_probed = len({(r["invoice_kind"], r["invoice_id"]) for r in rows})
    logger.info("[%s] sondées=%d, assignations=%d, échecs écartés=%d",
                kind, n_probed, n_assign, skipped)
    return {"kind": kind, "probed": n_probed, "assignments": n_assign,
            "skipped": skipped, "rows": rows}


def ensure_table(bq: bigquery.Client) -> None:
    ref = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    try:
        bq.get_table(ref)
        return
    except Exception:
        pass
    t = bigquery.Table(ref, schema=bq_schema())
    t.clustering_fields = ["invoice_kind", "invoice_id"]
    bq.create_table(t)
    logger.info("Created table %s", ref)


def replace_in_bq(rows: list[dict], bq: bigquery.Client) -> dict:
    if not rows:
        return {"replaced": 0, "skipped_empty": True}
    ensure_table(bq)
    staging = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_STAGING}"
    target = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    bq.load_table_from_json(
        rows, staging,
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, schema=bq_schema(),
        ),
    ).result()

    # Remplacement transactionnel par facture sondée : purge puis réécriture.
    bq.query(f"""
    BEGIN TRANSACTION;
    DELETE FROM `{target}` T
    WHERE EXISTS (SELECT 1 FROM `{staging}` S
                  WHERE S.invoice_kind = T.invoice_kind AND S.invoice_id = T.invoice_id);
    INSERT INTO `{target}`
    SELECT * FROM `{staging}` WHERE category_id IS NOT NULL;
    COMMIT TRANSACTION;
    """).result()
    bq.query(f"DROP TABLE `{staging}`").result()
    return {"replaced": len([r for r in rows if r["category_id"] is not None])}


def run(kinds: list[str], days: int | None, dry_run: bool = False) -> dict:
    token = os.environ.get("PENNYLANE_TOKEN")
    if not token:
        raise SystemExit("PENNYLANE_TOKEN env var required")

    prober = _Prober(token)
    bq = bigquery.Client(project=BQ_PROJECT)
    ingested_at = datetime.now(timezone.utc).isoformat()

    all_rows, results = [], []
    for k in kinds:
        res = run_kind(k, days, prober, bq, ingested_at, dry_run)
        rows = res.pop("rows")
        all_rows.extend(rows)
        results.append(res)

    if dry_run:
        return {"results": results, "dry_run": True}
    return {"results": results, **replace_in_bq(all_rows, bq)}


def cli():
    parser = argparse.ArgumentParser()
    parser.add_argument("--kind", choices=[*KINDS, "all"], default="all")
    parser.add_argument("--days", type=int, default=90,
                        help="fenêtre sur la date de facture (défaut 90j, aligné Lot B)")
    parser.add_argument("--all", action="store_true", help="backfill : toutes les factures")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    kinds = list(KINDS) if args.kind == "all" else [args.kind]
    print(run(kinds, days=None if args.all else args.days, dry_run=args.dry_run))


if __name__ == "__main__":
    cli()
