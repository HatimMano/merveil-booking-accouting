"""Pull le RAPPROCHEMENT facture → transaction bancaire → BQ
`pennylane.raw_invoice_matched_transactions`.

Lot 3 du chantier « max data ». C'est la brique qui répond à la question de Mickael
(« combien on a payé à Leroy Merlin ? ») avec sa **date de décaissement réelle**.

⭐ Pourquoi par la FACTURE et pas par la transaction (mesuré le 25/08) :
`raw_transactions.supplier_id` n'est rapproché que sur **13 % (2024) / 22 % (2025) /
27 % (2026)** des mouvements — 73 % des transactions 2026 ne portent aucun fournisseur.
La sous-ressource `/{kind}_invoices/{id}/matched_transactions` est remplie à **85 %**,
et elle sert la transaction **complète inline** (date, montant, libellé bancaire,
compte, `pro_account_expense` = employé + carte masquée). C'est donc le seul chemin
praticable, au prix d'un N+1.

⚠️ Périmètre : factures **FOURNISSEURS** uniquement. Les `customer_invoices` sont nos
propres factures, dont l'encaissement est déjà couvert par les flux 1 et 2 — les sonder
serait un N+1 pour de la redondance. Le paramètre `--kind` existe si ça change un jour.

⭐ Le point de design qui compte — la re-sonde est pilotée par BQ, PAS par une fenêtre :
une facture payée à 120 jours sort de l'overlap 90 j **avant** que son rapprochement
n'arrive, et `updated_at` n'est pas filtrable côté API. Le daily sonde donc :
  (a) toutes les factures de la fenêtre `--days` (défaut 90, aligné Lot B) ;
  (b) **plus** toutes celles qui n'ont TOUJOURS aucun rapprochement en base, dans une
      fenêtre de rattrapage `--recheck-days` (défaut 365).
Sans (b), le taux de 85 % s'éroderait silencieusement sur le stock.
⚠️ Le plafond (b) est assumé et **journalisé à chaque run** : une facture de plus d'un
an sans rapprochement n'est plus re-sondée (cf. log `abandonnées`). Ce n'est pas un
silence, c'est une borne — mais c'en est une.

⚠️ Comme pour `invoice_categories.py` : une facture dont la sonde ÉCHOUE après retries
sort du lot (ni écrite ni purgée), pour qu'un incident réseau n'efface pas des
rapprochements existants.

Usage CLI :
  PENNYLANE_TOKEN=xxx python -m pennylane.invoice_matched               # daily
  PENNYLANE_TOKEN=xxx python -m pennylane.invoice_matched --all         # backfill (~19k sondes)
  PENNYLANE_TOKEN=xxx python -m pennylane.invoice_matched --dry-run
"""

import argparse
import concurrent.futures as cf
import json
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
BQ_TABLE = "raw_invoice_matched_transactions"
BQ_STAGING = "_invoice_matched_staging"

KINDS = {
    "supplier": ("supplier_invoices", "raw_supplier_invoices"),
    "customer": ("customer_invoices", "raw_customer_invoices"),
}

WORKERS = 4          # même prudence que invoice_categories : l'API 429 vite (~5 sondes/s)
MAX_ATTEMPTS = 8

SF = bigquery.SchemaField


def bq_schema() -> list[bigquery.SchemaField]:
    return [
        SF("invoice_kind", "STRING", mode="REQUIRED"),
        SF("invoice_id", "INT64", mode="REQUIRED"),
        SF("transaction_id", "INT64"),          # NULL = sentinelle staging (purge)
        SF("date", "DATE"),                     # date du MOUVEMENT bancaire
        SF("amount", "NUMERIC"),
        SF("currency", "STRING"),
        SF("currency_amount", "NUMERIC"),
        SF("fee", "NUMERIC"),
        SF("label", "STRING"),                  # libellé bancaire brut
        SF("bank_account_id", "INT64"),
        SF("journal_id", "INT64"),
        SF("supplier_id", "INT64"),
        SF("customer_id", "INT64"),
        SF("outstanding_balance", "NUMERIC"),
        SF("attachment_required", "BOOL"),
        SF("pro_account_employee", "STRING"),   # qui a payé…
        SF("pro_account_card", "STRING"),       # …avec quelle carte
        SF("transaction_created_at", "TIMESTAMP"),
        SF("transaction_updated_at", "TIMESTAMP"),
        SF("ingested_at", "TIMESTAMP", mode="REQUIRED"),
        SF("_raw_payload", "STRING"),
    ]


def _num(v) -> str | None:
    return str(v) if v is not None else None


def _nested_id(obj) -> int | None:
    return int(obj["id"]) if obj and obj.get("id") is not None else None


def transform(kind: str, invoice_id: int, tx: dict, ingested_at: str) -> dict:
    pro = tx.get("pro_account_expense") or {}
    emp = pro.get("employee") or {}
    emp_name = " ".join(p for p in (emp.get("first_name"), emp.get("last_name")) if p) or None
    return {
        "invoice_kind": kind,
        "invoice_id": int(invoice_id),
        "transaction_id": int(tx["id"]),
        "date": tx.get("date"),
        "amount": _num(tx.get("amount")),
        "currency": tx.get("currency"),
        "currency_amount": _num(tx.get("currency_amount")),
        "fee": _num(tx.get("fee")),
        "label": tx.get("label"),
        "bank_account_id": _nested_id(tx.get("bank_account")),
        "journal_id": _nested_id(tx.get("journal")),
        "supplier_id": _nested_id(tx.get("supplier")),
        "customer_id": _nested_id(tx.get("customer")),
        "outstanding_balance": _num(tx.get("outstanding_balance")),
        "attachment_required": bool(tx.get("attachment_required")),
        "pro_account_employee": emp_name,
        "pro_account_card": pro.get("card_masked_number"),
        "transaction_created_at": tx.get("created_at"),
        "transaction_updated_at": tx.get("updated_at"),
        "ingested_at": ingested_at,
        "_raw_payload": json.dumps(tx, ensure_ascii=False),
    }


def sentinel(kind: str, invoice_id: int, ingested_at: str) -> dict:
    """Facture sondée SANS rapprochement : purge d'éventuels matchs retirés."""
    row = {f.name: None for f in bq_schema()}
    row.update(invoice_kind=kind, invoice_id=int(invoice_id), ingested_at=ingested_at)
    return row


class _Prober:
    def __init__(self, token: str):
        self._token = token
        self._local = threading.local()

    def _client(self) -> PennylaneGLClient:
        if not hasattr(self._local, "c"):
            self._local.c = PennylaneGLClient(self._token)
        return self._local.c

    def probe(self, endpoint: str, invoice_id: int):
        """→ liste d'items, ou None si échec définitif (facture écartée du lot)."""
        for attempt in range(MAX_ATTEMPTS):
            try:
                d = self._client()._get(
                    f"{PENNYLANE_BASE}/{endpoint}/{invoice_id}/matched_transactions",
                    params={"limit": 100},
                )
                return d.get("items", [])
            except Exception as e:
                if attempt == MAX_ATTEMPTS - 1:
                    logger.warning("probe KO %s/%s : %s", endpoint, invoice_id, str(e)[:120])
                    return None
                time.sleep(1.0 * (attempt + 1))
        return None


def invoice_ids(bq: bigquery.Client, kind: str, days: int | None,
                recheck_days: int) -> tuple[list[int], dict]:
    """(a) fenêtre récente + (b) rattrapage des factures encore sans rapprochement."""
    _, table = KINDS[kind]
    src = f"`{BQ_PROJECT}.{BQ_DATASET}.{table}`"
    tgt = f"`{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`"

    if days is None:                                    # backfill : tout
        q = f"SELECT id, 'backfill' AS bucket FROM {src}"
    else:
        # Le rattrapage n'a de sens que si la table cible existe déjà.
        try:
            bq.get_table(f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}")
            recheck = f"""
            UNION DISTINCT
            SELECT i.id, 'rattrapage' AS bucket FROM {src} i
            WHERE i.date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL {int(recheck_days)} DAY)
                             AND DATE_SUB(CURRENT_DATE(), INTERVAL {int(days)} DAY)
              AND NOT EXISTS (SELECT 1 FROM {tgt} m
                              WHERE m.invoice_kind = '{kind}' AND m.invoice_id = i.id)
            """
        except Exception:
            recheck = ""
        q = f"""
        SELECT id, 'fenetre' AS bucket FROM {src}
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL {int(days)} DAY)
        {recheck}
        """
    rows = list(bq.query(q).result())
    buckets = {}
    for r in rows:
        buckets[r.bucket] = buckets.get(r.bucket, 0) + 1
    return [r.id for r in rows], buckets


def count_abandoned(bq: bigquery.Client, kind: str, recheck_days: int) -> int:
    """Factures hors fenêtre de rattrapage et toujours sans rapprochement (borne assumée)."""
    _, table = KINDS[kind]
    try:
        bq.get_table(f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}")
    except Exception:
        return 0
    q = f"""
    SELECT COUNT(*) n FROM `{BQ_PROJECT}.{BQ_DATASET}.{table}` i
    WHERE i.date < DATE_SUB(CURRENT_DATE(), INTERVAL {int(recheck_days)} DAY)
      AND NOT EXISTS (SELECT 1 FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}` m
                      WHERE m.invoice_kind = '{kind}' AND m.invoice_id = i.id)
    """
    return list(bq.query(q).result())[0].n


def run_kind(kind: str, days: int | None, recheck_days: int, prober: _Prober,
             bq: bigquery.Client, ingested_at: str) -> dict:
    endpoint, _ = KINDS[kind]
    ids, buckets = invoice_ids(bq, kind, days, recheck_days)
    logger.info("[%s] %d factures à sonder %s", kind, len(ids), buckets)

    rows, skipped, done, matched = [], 0, 0, 0
    lock = threading.Lock()

    def work(iid: int):
        nonlocal skipped, done, matched
        items = prober.probe(endpoint, iid)
        with lock:
            done += 1
            if done % 500 == 0:
                logger.info("[%s] %d/%d sondées (%d KO)", kind, done, len(ids), skipped)
        if items is None:
            with lock:
                skipped += 1
            return
        out = ([transform(kind, iid, tx, ingested_at) for tx in items] if items
               else [sentinel(kind, iid, ingested_at)])
        with lock:
            if items:
                matched += 1
            rows.extend(out)

    with cf.ThreadPoolExecutor(WORKERS) as ex:
        list(ex.map(work, ids))

    probed = len({r["invoice_id"] for r in rows})
    n_tx = sum(1 for r in rows if r["transaction_id"] is not None)
    pct = round(100 * matched / probed, 1) if probed else None
    logger.info("[%s] sondées=%d, avec rapprochement=%d (%s%%), transactions=%d, échecs écartés=%d",
                kind, probed, matched, pct, n_tx, skipped)

    abandoned = count_abandoned(bq, kind, recheck_days) if days is not None else 0
    if abandoned:
        logger.info("[%s] ⚠ %d factures hors fenêtre de rattrapage (%dj) et sans rapprochement "
                    "— non re-sondées (borne assumée)", kind, abandoned, recheck_days)

    return {"kind": kind, "probed": probed, "with_match": matched, "match_pct": pct,
            "transactions": n_tx, "skipped": skipped, "abandoned": abandoned, "rows": rows}


def ensure_table(bq: bigquery.Client) -> None:
    ref = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    try:
        bq.get_table(ref)
        return
    except Exception:
        pass
    t = bigquery.Table(ref, schema=bq_schema())
    t.clustering_fields = ["invoice_kind", "invoice_id", "supplier_id"]
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

    bq.query(f"""
    BEGIN TRANSACTION;
    DELETE FROM `{target}` T
    WHERE EXISTS (SELECT 1 FROM `{staging}` S
                  WHERE S.invoice_kind = T.invoice_kind AND S.invoice_id = T.invoice_id);
    INSERT INTO `{target}`
    SELECT * FROM `{staging}` WHERE transaction_id IS NOT NULL;
    COMMIT TRANSACTION;
    """).result()
    bq.query(f"DROP TABLE `{staging}`").result()
    return {"replaced": len([r for r in rows if r["transaction_id"] is not None])}


def run(kinds: list[str], days: int | None, recheck_days: int = 365,
        dry_run: bool = False) -> dict:
    token = os.environ.get("PENNYLANE_TOKEN")
    if not token:
        raise SystemExit("PENNYLANE_TOKEN env var required")

    prober = _Prober(token)
    bq = bigquery.Client(project=BQ_PROJECT)
    ingested_at = datetime.now(timezone.utc).isoformat()

    all_rows, results = [], []
    for k in kinds:
        res = run_kind(k, days, recheck_days, prober, bq, ingested_at)
        all_rows.extend(res.pop("rows"))
        results.append(res)

    if dry_run:
        return {"results": results, "dry_run": True}
    return {"results": results, **replace_in_bq(all_rows, bq)}


def cli():
    parser = argparse.ArgumentParser()
    parser.add_argument("--kind", choices=[*KINDS, "all"], default="supplier",
                        help="supplier par défaut (les factures clients sont couvertes "
                             "par les flux 1/2 — cf. docstring)")
    parser.add_argument("--days", type=int, default=90,
                        help="fenêtre sur la date de facture (défaut 90j, aligné Lot B)")
    parser.add_argument("--recheck-days", type=int, default=365,
                        help="rattrapage des factures encore sans rapprochement (défaut 365j)")
    parser.add_argument("--all", action="store_true", help="backfill : toutes les factures")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    kinds = list(KINDS) if args.kind == "all" else [args.kind]
    print(run(kinds, days=None if args.all else args.days,
              recheck_days=args.recheck_days, dry_run=args.dry_run))


if __name__ == "__main__":
    cli()
