"""Télécharge les PDF de factures Pennylane → `gs://merveil-pennylane-invoices/`.

Phase 4a du chantier « max data ». Étape 1 de la chaîne d'extraction : ce job ne fait
QUE poser les fichiers dans GCS et tracer ce qu'il a posé. L'analyse Gemini est un job
séparé (`merveil-genai`, `bash deploy.sh invoices`) qui lit GCS — ainsi la SA
`merveil-genai-sa` n'a JAMAIS besoin du token Pennylane.

⚠️⚠️ LE PIÈGE CENTRAL — `public_file_url` EXPIRE en moins de ~25 minutes et Pennylane
en resigne une DIFFÉRENTE à chaque GET. L'URL stockée dans `raw_supplier_invoices` est
donc **périmée par construction** : elle atteste seulement qu'un PDF existe. Le fetcher
doit **re-sonder la facture par son id** juste avant de télécharger (2 appels/doc).
Ne jamais « optimiser » en réutilisant l'URL de BQ : ça renvoie des 400 silencieux.

Idempotent : si l'objet GCS existe déjà, on ne re-télécharge pas (skip sans appel API).
Un backfill interrompu se reprend donc sans coût. Trace dans `pennylane.raw_invoice_files`
(MERGE sur (invoice_kind, invoice_id)) → c'est elle qui pilote le job Gemini en aval.

⚠️ PÉRIMÈTRE BORNÉ À 3 MOIS par défaut (décision Hatim 25/08) : l'historique complet
= ~16 700 documents × 2 appels à ~5/s ≈ 4 h de fetch, pour un intérêt qui décroît avec
l'âge des factures. Le quotidien ne traite donc que ~30-40 nouveaux documents.
`--all` lève la borne si on veut reprendre l'historique — par tranches de préférence.

Usage CLI :
  PENNYLANE_TOKEN=xxx python -m pennylane.invoice_pdfs               # daily (90j)
  PENNYLANE_TOKEN=xxx python -m pennylane.invoice_pdfs --days 30
  PENNYLANE_TOKEN=xxx python -m pennylane.invoice_pdfs --all         # tout l'historique
  PENNYLANE_TOKEN=xxx python -m pennylane.invoice_pdfs --limit 50 --dry-run
"""

import argparse
import concurrent.futures as cf
import logging
import os
import threading
import time
from datetime import datetime, timezone

import requests
from google.cloud import bigquery, storage

from .grand_livre import PENNYLANE_BASE, PennylaneGLClient

logger = logging.getLogger(__name__)

BQ_PROJECT = "merveil-data-warehouse"
BQ_DATASET = "pennylane"
BQ_TABLE = "raw_invoice_files"
GCS_BUCKET = "merveil-pennylane-invoices"

KINDS = {
    "supplier": ("supplier_invoices", "raw_supplier_invoices"),
    "customer": ("customer_invoices", "raw_customer_invoices"),
}

WORKERS = 4
MAX_ATTEMPTS = 6
MAX_BYTES = 15_000_000   # au-delà, Gemini refuse l'inline — on trace et on écarte
DEFAULT_DAYS = 90        # périmètre par défaut : 3 mois (cf. to_fetch)

SF = bigquery.SchemaField


def bq_schema() -> list[bigquery.SchemaField]:
    return [
        SF("invoice_kind", "STRING", mode="REQUIRED"),
        SF("invoice_id", "INT64", mode="REQUIRED"),
        SF("gcs_uri", "STRING"),
        SF("mime_type", "STRING"),
        SF("file_bytes", "INT64"),
        SF("filename", "STRING"),
        SF("status", "STRING", mode="REQUIRED"),   # ok | skipped_existing | no_url | too_big | error
        SF("error", "STRING"),
        SF("fetched_at", "TIMESTAMP", mode="REQUIRED"),
    ]


def mime_of(data: bytes, filename: str | None) -> str:
    """Le magic number fait foi ; le nom de fichier n'est qu'un repli."""
    if data[:4] == b"%PDF":
        return "application/pdf"
    if data[:3] == b"\xff\xd8\xff":
        return "image/jpeg"
    if data[:8] == b"\x89PNG\r\n\x1a\n":
        return "image/png"
    fn = (filename or "").lower()
    for ext, m in ((".pdf", "application/pdf"), (".jpg", "image/jpeg"),
                   (".jpeg", "image/jpeg"), (".png", "image/png")):
        if fn.endswith(ext):
            return m
    return "application/pdf"


EXT_OF = {"application/pdf": "pdf", "image/jpeg": "jpg", "image/png": "png"}


def http_retry(fn, tries: int = MAX_ATTEMPTS, base: float = 2.0):
    delay = r = None
    for _ in range(tries):
        r = fn()
        if r.status_code in (429, 500, 502, 503, 504):
            delay = base if delay is None else delay * 2
            time.sleep(delay)
            continue
        return r
    return r


class _Fetcher:
    def __init__(self, token: str, bucket):
        self._token = token
        self._bucket = bucket
        self._local = threading.local()

    def _session(self) -> requests.Session:
        if not hasattr(self._local, "s"):
            s = requests.Session()
            s.headers.update({"Authorization": f"Bearer {self._token}",
                              "accept": "application/json"})
            self._local.s = s
        return self._local.s

    def fetch(self, kind: str, endpoint: str, invoice_id: int, fetched_at: str) -> dict:
        row = {"invoice_kind": kind, "invoice_id": int(invoice_id), "gcs_uri": None,
               "mime_type": None, "file_bytes": None, "filename": None,
               "status": "ok", "error": None, "fetched_at": fetched_at}
        try:
            # 1. URL FRAÎCHE — celle de BQ a expiré (cf. docstring).
            r = http_retry(lambda: self._session().get(
                f"{PENNYLANE_BASE}/{endpoint}/{invoice_id}", timeout=30))
            if r.status_code == 404:
                row.update(status="error", error="invoice_404")
                return row
            r.raise_for_status()
            payload = r.json()
            url = payload.get("public_file_url")
            row["filename"] = payload.get("filename")
            if not url:
                row.update(status="no_url")
                return row

            # 2. Download (l'URL signée ne porte AUCUNE auth → session neutre).
            d = http_retry(lambda: requests.get(url, timeout=90))
            d.raise_for_status()
            data = d.content
            mime = mime_of(data, row["filename"])
            row.update(mime_type=mime, file_bytes=len(data))
            if len(data) > MAX_BYTES:
                row.update(status="too_big", error=f"{len(data)} bytes")
                return row

            # 3. GCS
            blob = self._bucket.blob(f"{kind}/{invoice_id}.{EXT_OF.get(mime, 'pdf')}")
            blob.upload_from_string(data, content_type=mime)
            row["gcs_uri"] = f"gs://{GCS_BUCKET}/{blob.name}"
        except Exception as e:
            row.update(status="error", error=f"{type(e).__name__}: {e}"[:250])
        return row


def to_fetch(bq: bigquery.Client, kind: str, all_docs: bool, limit: int | None,
             days: int | None = DEFAULT_DAYS) -> list[int]:
    """Factures avec un PDF, pas encore posées dans GCS.

    ⚠️ Bornée à `days` par défaut (3 mois). Le périmètre n'est PAS « tout
    l'historique » : 16 700 documents à 2 appels chacun font ~4 h de fetch pour
    un intérêt décroissant avec l'âge des factures. `--all` lève la borne si on
    veut un jour reprendre l'historique complet, par tranches de préférence.
    """
    _, table = KINDS[kind]
    src = f"`{BQ_PROJECT}.{BQ_DATASET}.{table}`"
    tgt = f"`{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`"
    where = "WHERE i.public_file_url IS NOT NULL"
    if days is not None and not all_docs:
        where += f" AND i.date >= DATE_SUB(CURRENT_DATE(), INTERVAL {int(days)} DAY)"
    if not all_docs:
        try:
            bq.get_table(f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}")
            # On ne retente pas les statuts terminaux (no_url/too_big) : ils ne
            # changeront pas d'un run à l'autre. Les `error` SONT retentés.
            where += f""" AND NOT EXISTS (
                SELECT 1 FROM {tgt} f
                WHERE f.invoice_kind = '{kind}' AND f.invoice_id = i.id
                  AND f.status IN ('ok', 'no_url', 'too_big'))"""
        except Exception:
            pass
    q = f"SELECT i.id FROM {src} i {where} ORDER BY i.date DESC"
    if limit:
        q += f" LIMIT {int(limit)}"
    return [r.id for r in bq.query(q).result()]


def ensure_table(bq: bigquery.Client) -> None:
    ref = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    try:
        bq.get_table(ref)
        return
    except Exception:
        pass
    t = bigquery.Table(ref, schema=bq_schema())
    t.clustering_fields = ["invoice_kind", "status"]
    bq.create_table(t)
    logger.info("Created table %s", ref)


def merge_to_bq(rows: list[dict], bq: bigquery.Client) -> dict:
    if not rows:
        return {"merged": 0, "skipped_empty": True}
    ensure_table(bq)
    staging = f"{BQ_PROJECT}.{BQ_DATASET}._{BQ_TABLE}_staging"
    target = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    bq.load_table_from_json(
        rows, staging,
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, schema=bq_schema(),
        ),
    ).result()

    cols = [f.name for f in bq_schema()]
    keys = ("invoice_kind", "invoice_id")
    set_clause = ",\n      ".join(f"{c} = S.{c}" for c in cols if c not in keys)
    bq.query(f"""
    MERGE `{target}` T
    USING `{staging}` S
    ON T.invoice_kind = S.invoice_kind AND T.invoice_id = S.invoice_id
    WHEN MATCHED THEN UPDATE SET
      {set_clause}
    WHEN NOT MATCHED THEN INSERT ({", ".join(cols)}) VALUES ({", ".join("S." + c for c in cols)})
    """).result()
    bq.query(f"DROP TABLE `{staging}`").result()
    return {"merged": len(rows)}


def run(kinds: list[str], all_docs: bool = False, limit: int | None = None,
        days: int | None = DEFAULT_DAYS, dry_run: bool = False) -> dict:
    token = os.environ.get("PENNYLANE_TOKEN")
    if not token:
        raise SystemExit("PENNYLANE_TOKEN env var required")

    bq = bigquery.Client(project=BQ_PROJECT)
    bucket = storage.Client(project=BQ_PROJECT).bucket(GCS_BUCKET)
    fetcher = _Fetcher(token, bucket)
    fetched_at = datetime.now(timezone.utc).isoformat()

    all_rows, results = [], []
    for kind in kinds:
        endpoint, _ = KINDS[kind]
        ids = to_fetch(bq, kind, all_docs, limit, days)
        logger.info("[%s] %d documents à récupérer (périmètre : %s)", kind, len(ids),
                    "tout l'historique" if all_docs else f"{days} derniers jours")
        if dry_run:
            results.append({"kind": kind, "to_fetch": len(ids), "dry_run": True})
            continue

        # Skip GCS-existant AVANT tout appel API : un backfill interrompu reprend gratis.
        rows, done, skipped = [], 0, 0
        lock = threading.Lock()

        def work(iid: int):
            nonlocal done, skipped
            existing = next((b for b in (bucket.blob(f"{kind}/{iid}.{e}")
                                         for e in ("pdf", "jpg", "png")) if b.exists()), None)
            if existing:
                with lock:
                    skipped += 1
                    done += 1
                r = {"invoice_kind": kind, "invoice_id": int(iid),
                     "gcs_uri": f"gs://{GCS_BUCKET}/{existing.name}",
                     "mime_type": None, "file_bytes": existing.size,
                     "filename": None, "status": "skipped_existing",
                     "error": None, "fetched_at": fetched_at}
            else:
                r = fetcher.fetch(kind, endpoint, iid, fetched_at)
                with lock:
                    done += 1
            with lock:
                rows.append(r)
                if done % 250 == 0:
                    logger.info("[%s] %d/%d (%d déjà en GCS)", kind, done, len(ids), skipped)

        with cf.ThreadPoolExecutor(WORKERS) as ex:
            list(ex.map(work, ids))

        by_status = {}
        for r in rows:
            by_status[r["status"]] = by_status.get(r["status"], 0) + 1
        logger.info("[%s] terminé : %s", kind, by_status)
        all_rows.extend(rows)
        results.append({"kind": kind, "processed": len(rows), "by_status": by_status})

    if dry_run:
        return {"results": results, "dry_run": True}
    return {"results": results, **merge_to_bq(all_rows, bq)}


def cli():
    parser = argparse.ArgumentParser()
    parser.add_argument("--kind", choices=[*KINDS, "all"], default="supplier")
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS,
                        help=f"fenêtre sur la date de facture (défaut {DEFAULT_DAYS}j = 3 mois)")
    parser.add_argument("--all", action="store_true",
                        help="lève la borne de date : TOUT l'historique (~16,7k docs, ~4h)")
    parser.add_argument("--limit", type=int, help="plafonne le lot (test)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    kinds = list(KINDS) if args.kind == "all" else [args.kind]
    print(run(kinds, all_docs=args.all, limit=args.limit, days=args.days,
              dry_run=args.dry_run))


if __name__ == "__main__":
    cli()
