"""Pull PennyLane grand livre filtré (loyers/charges/taxes) → BQ.

Pour une fenêtre [from_date, to_date], ratisse /ledger_entry_lines, filtre côté
Python sur les préfixes de comptes 6041 / 6042 / 604720 (convention Archides
custom), récupère le libellé custom du compte via /ledger_accounts/:id (cached),
extrait le `code_appart` du libellé par regex, et MERGE dans
`pennylane.raw_grand_livre`.

Usage CLI :
  PENNYLANE_TOKEN=xxx python -m pennylane.grand_livre --from-date 2026-04-01 --to-date 2026-04-30
  PENNYLANE_TOKEN=xxx python -m pennylane.grand_livre --last-month
  PENNYLANE_TOKEN=xxx python -m pennylane.grand_livre --last-month --dry-run

MERGE clé : ledger_entry_line_id (gère les corrections comptables tardives).
"""

import argparse
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Iterator

import requests
from google.cloud import bigquery

logger = logging.getLogger(__name__)

PENNYLANE_BASE = "https://app.pennylane.com/api/external/v2"

LOYER_PREFIX = "6041"
CHARGES_PREFIX = "6042"
TAXE_PREFIX = "604720"

# code_appart Merveil : P02-ABO52, P03-MAR181-0, P08-ROY15-5&6, P15-FRE17B-2F…
APT_CODE_RE = re.compile(r"(P\d{2}-[A-Z]+\d+[A-Z]?-?[\w&]*)")

BQ_PROJECT = "merveil-data-warehouse"
BQ_DATASET = "pennylane"
BQ_TABLE = "raw_grand_livre"
BQ_STAGING = "_grand_livre_staging"


def classify(account_number: str) -> str | None:
    if account_number.startswith(TAXE_PREFIX):
        return "taxe_fonciere"
    if account_number.startswith(CHARGES_PREFIX):
        return "charges"
    if account_number.startswith(LOYER_PREFIX):
        return "loyer"
    return None


def extract_code_appart(label: str) -> str | None:
    m = APT_CODE_RE.search(label or "")
    return m.group(1) if m else None


class PennylaneGLClient:
    def __init__(self, token: str):
        self._sess = requests.Session()
        self._sess.headers.update({
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        })
        self._account_cache: dict[int, dict] = {}

    def iter_ledger_lines(self, from_date: str, to_date: str) -> Iterator[dict]:
        """Itère les ledger_entry_lines en ordre date DESC (early-stop friendly).

        L'API Pennylane v2 ignore from_date/to_date mais respecte sort=-date.
        On laisse à l'appelant le soin d'arrêter dès qu'il sort de la fenêtre.
        """
        cursor = None
        page = 0
        while True:
            params = {"limit": 100, "sort": "-date"}
            if cursor:
                params["cursor"] = cursor
            d = self._get(f"{PENNYLANE_BASE}/ledger_entry_lines", params=params)
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

    def get_account(self, account_id: int) -> dict:
        if account_id not in self._account_cache:
            self._account_cache[account_id] = self._get(
                f"{PENNYLANE_BASE}/ledger_accounts/{account_id}"
            )
        return self._account_cache[account_id]

    def _get(self, url: str, params: dict | None = None, max_retries: int = 8) -> dict:
        delay = 2.0
        last_err: str = ""
        for attempt in range(max_retries):
            try:
                r = self._sess.get(url, params=params, timeout=60)
            except (requests.Timeout, requests.ConnectionError) as e:
                last_err = f"{type(e).__name__}: {e}"
                logger.info("net %s sleep %.1fs (%d/%d)", type(e).__name__, delay, attempt + 1, max_retries)
                time.sleep(delay)
                delay *= 2
                continue
            if r.status_code == 429:
                last_err = "429 RESOURCE_EXHAUSTED"
                logger.info("429 sleep %.1fs (%d/%d)", delay, attempt + 1, max_retries)
                time.sleep(delay)
                delay *= 2
                continue
            if 500 <= r.status_code < 600:
                last_err = f"HTTP {r.status_code}"
                logger.info("%s sleep %.1fs (%d/%d)", last_err, delay, attempt + 1, max_retries)
                time.sleep(delay)
                delay *= 2
                continue
            r.raise_for_status()
            return r.json()
        raise requests.HTTPError(f"retries exhausted ({last_err}) on {url}")


def transform_line(line: dict, account_info: dict, ingested_at: str) -> dict:
    la = line.get("ledger_account") or {}
    num = str(la.get("number") or "")
    account_label = account_info.get("label") or ""
    return {
        "ledger_entry_line_id": int(line["id"]),
        "ledger_entry_id": int((line.get("ledger_entry") or {}).get("id") or 0) or None,
        "date": line.get("date"),
        "journal_id": int((line.get("journal") or {}).get("id") or 0) or None,
        "account_id": int(la.get("id") or 0) or None,
        "account_number": num,
        "account_label": account_label,
        "code_appart": extract_code_appart(account_label),
        "charge_type": classify(num),
        "libelle_piece": line.get("label") or "",
        "debit": str(line.get("debit") or "0"),
        "credit": str(line.get("credit") or "0"),
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
        bigquery.SchemaField("account_label", "STRING"),
        bigquery.SchemaField("code_appart", "STRING"),
        bigquery.SchemaField("charge_type", "STRING"),
        bigquery.SchemaField("libelle_piece", "STRING"),
        bigquery.SchemaField("debit", "NUMERIC"),
        bigquery.SchemaField("credit", "NUMERIC"),
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
    table.clustering_fields = ["account_number", "code_appart"]
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

    merge_sql = f"""
    MERGE `{target}` T
    USING `{staging}` S
    ON T.ledger_entry_line_id = S.ledger_entry_line_id
    WHEN MATCHED THEN UPDATE SET
      date = S.date,
      journal_id = S.journal_id,
      account_id = S.account_id,
      account_number = S.account_number,
      account_label = S.account_label,
      code_appart = S.code_appart,
      charge_type = S.charge_type,
      libelle_piece = S.libelle_piece,
      debit = S.debit,
      credit = S.credit,
      created_at = S.created_at,
      updated_at = S.updated_at,
      ingested_at = S.ingested_at
    WHEN NOT MATCHED THEN INSERT (
      ledger_entry_line_id, ledger_entry_id, date, journal_id,
      account_id, account_number, account_label, code_appart,
      charge_type, libelle_piece, debit, credit,
      created_at, updated_at, ingested_at
    ) VALUES (
      S.ledger_entry_line_id, S.ledger_entry_id, S.date, S.journal_id,
      S.account_id, S.account_number, S.account_label, S.code_appart,
      S.charge_type, S.libelle_piece, S.debit, S.credit,
      S.created_at, S.updated_at, S.ingested_at
    )
    """
    bq.query(merge_sql).result()
    bq.query(f"DROP TABLE `{staging}`").result()
    return {"merged": len(rows)}


def last_month_range() -> tuple[str, str]:
    today = datetime.now().date()
    first_of_this = today.replace(day=1)
    last_of_last = first_of_this - timedelta(days=1)
    first_of_last = last_of_last.replace(day=1)
    return first_of_last.isoformat(), last_of_last.isoformat()


def run(from_date: str, to_date: str, dry_run: bool = False) -> dict:
    token = os.environ.get("PENNYLANE_TOKEN")
    if not token:
        raise SystemExit("PENNYLANE_TOKEN env var required")

    client = PennylaneGLClient(token)
    bq = bigquery.Client(project=BQ_PROJECT)
    ingested_at = datetime.now(timezone.utc).isoformat()

    rows = []
    total = 0
    out_of_window = 0
    # Items renvoyés en sort=-date desc (futur → passé). On early-stop dès qu'on
    # passe sous from_date depuis un nombre suffisant de lignes consécutives.
    # Marge de sécurité (50) car au sein d'une même date l'ordre interne peut
    # mixer les items, et l'API peut renvoyer un peu de "passé" intercalé en
    # début si elle commence par les écritures futures (prévisions).
    EARLY_STOP_BUFFER = 200
    consecutive_below_from = 0
    for line in client.iter_ledger_lines(from_date, to_date):
        total += 1
        d = line.get("date") or ""
        # Early-stop : N items consécutifs avec date < from_date → on a quitté
        # la fenêtre et on n'y reviendra plus (sort=-date strict).
        if d and d < from_date:
            consecutive_below_from += 1
            if consecutive_below_from >= EARLY_STOP_BUFFER:
                logger.info(
                    "early-stop at scan=%d : %d consecutive lines with date<%s",
                    total, EARLY_STOP_BUFFER, from_date,
                )
                break
        else:
            consecutive_below_from = 0

        num = str((line.get("ledger_account") or {}).get("number") or "")
        if not classify(num):
            continue
        if d < from_date or d > to_date:
            out_of_window += 1
            continue
        account_id = (line.get("ledger_account") or {}).get("id")
        account_info = client.get_account(account_id) if account_id else {"label": ""}
        rows.append(transform_line(line, account_info, ingested_at))
    logger.info("out_of_window dropped: %d", out_of_window)

    logger.info("Window %s → %s : scanned=%d, kept=%d, accts_cached=%d",
                from_date, to_date, total, len(rows), len(client._account_cache))

    if dry_run:
        logger.info("DRY RUN — no BQ write")
        return {"from": from_date, "to": to_date, "scanned": total, "kept": len(rows), "dry_run": True}

    res = merge_to_bq(rows, bq)
    return {"from": from_date, "to": to_date, "scanned": total, "kept": len(rows), **res}


def cli():
    parser = argparse.ArgumentParser()
    parser.add_argument("--from-date", help="YYYY-MM-DD")
    parser.add_argument("--to-date", help="YYYY-MM-DD")
    parser.add_argument("--last-month", action="store_true",
                        help="Ignore --from/--to, ratisser le mois précédent")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    if args.last_month:
        f, t = last_month_range()
        logger.info("--last-month → %s to %s", f, t)
    else:
        if not (args.from_date and args.to_date):
            parser.error("Need --from-date + --to-date OR --last-month")
        f, t = args.from_date, args.to_date

    result = run(f, t, dry_run=args.dry_run)
    print(result)


if __name__ == "__main__":
    cli()
