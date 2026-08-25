"""Pull la BALANCE GÉNÉRALE Pennylane → BQ `pennylane.raw_trial_balance`.

Lot 3 du chantier « max data » (cf. plan `Archides/docs/plans/pennylane-max-data-2026-08-21.md`).
C'est **l'arbitre de réconciliation** qui manquait : `raw_ledger_lines` (Lot A) garde des
orphelins (le MERGE ne supprime jamais une écriture effacée côté Pennylane), donc nos
agrégats par compte n'ont aujourd'hui aucune contrepartie autoritaire. `/trial_balance`
est cette contrepartie, calculée par Pennylane lui-même.

⭐ Deux propriétés mesurées le 25/08, elles fondent tout le design :
  - **la balance est ADDITIVE par période** — la somme des 3 balances mensuelles de Q1 2026
    égale la balance Q1 sur les 378 comptes, au centime. Ce sont donc des MOUVEMENTS de
    période, pas un cumul avec report à nouveau. → on tire au **grain mois**, et n'importe
    quelle période se reconstitue par somme côté dbt.
  - **elle est équilibrée** : Σ débits = Σ crédits = 44 882 188,86 € sur Q1 2026, écart 0,00.
    C'est un contrôle interne gratuit (cf. `check_balanced()`).

⚠️ Écart assumé avec la spec du plan, qui prévoyait un snapshot append façon
`raw_bank_accounts` : cet endpoint n'a pas d'`id`, MAIS il a une **clé naturelle stable**
`(period_start, account_number)`. Un MERGE dessus est donc possible et préférable — un
append quotidien écrirait ~8 000 lignes/jour pour ré-affirmer les mêmes montants.
La table reste petite (~400 comptes × N mois) et `snapshot_at` porte la date de dernier
rafraîchissement. **Bénéfice de bord** : un mois d'exercice CLOS dont le `snapshot_at`
bouge = une écriture rétroactive sur une période fermée, c'est-à-dire exactement le
signal que cherche le chantier « détection d'erreurs de saisie ».

Périmètre par défaut : tous les mois des exercices **ouverts** (lus dans
`pennylane.raw_fiscal_years`, alimentée par `references.py`), bornés à aujourd'hui.
Fallback si la table est absente/vide : l'année civile courante.

Usage CLI :
  PENNYLANE_TOKEN=xxx python -m pennylane.trial_balance                      # exercices ouverts
  PENNYLANE_TOKEN=xxx python -m pennylane.trial_balance --from-month 2024-01 # backfill
  PENNYLANE_TOKEN=xxx python -m pennylane.trial_balance --dry-run
"""

import argparse
import logging
import os
from datetime import date, datetime, timezone

from google.cloud import bigquery

from .grand_livre import PENNYLANE_BASE, PennylaneGLClient

logger = logging.getLogger(__name__)

BQ_PROJECT = "merveil-data-warehouse"
BQ_DATASET = "pennylane"
BQ_TABLE = "raw_trial_balance"

SF = bigquery.SchemaField


def bq_schema() -> list[bigquery.SchemaField]:
    return [
        SF("period_start", "DATE", mode="REQUIRED"),   # 1er du mois — clé naturelle
        SF("period_end", "DATE", mode="REQUIRED"),
        SF("account_number", "STRING", mode="REQUIRED"),
        SF("formatted_number", "STRING"),
        SF("label", "STRING"),
        SF("debits", "NUMERIC"),
        SF("credits", "NUMERIC"),
        SF("snapshot_at", "TIMESTAMP", mode="REQUIRED"),
    ]


def month_bounds(y: int, m: int) -> tuple[date, date]:
    start = date(y, m, 1)
    end = date(y + (m == 12), (m % 12) + 1, 1)
    return start, date.fromordinal(end.toordinal() - 1)


def months_between(first: date, last: date) -> list[tuple[date, date]]:
    out, y, m = [], first.year, first.month
    while date(y, m, 1) <= last:
        out.append(month_bounds(y, m))
        y, m = y + (m == 12), (m % 12) + 1
    return out


def open_fiscal_periods(bq: bigquery.Client) -> tuple[date, date]:
    """Bornes des exercices OUVERTS (raw_fiscal_years), sinon année civile courante.

    ⚠️ La borne haute va jusqu'à la FIN de l'exercice, pas à aujourd'hui : des
    écritures datées dans le futur existent (loyers d'avance, prévisions —
    ~372 k€ sur sept-déc 2026 mesurés le 25/08) et la balance les porte. Borner
    à today les laissait sans contrepartie dans `dash_finance_reco_balance`
    (verdict `absent_de_la_balance` au lieu d'une vraie comparaison).
    """
    today = datetime.now(timezone.utc).date()
    try:
        rows = list(bq.query(
            f"SELECT MIN(start) lo, MAX(finish) hi "
            f"FROM `{BQ_PROJECT}.{BQ_DATASET}.raw_fiscal_years` WHERE status = 'open'"
        ).result())
        if rows and rows[0].lo:
            return rows[0].lo, rows[0].hi or today
    except Exception as e:
        logger.warning("raw_fiscal_years illisible (%s) → repli année civile", str(e)[:100])
    return date(today.year, 1, 1), date(today.year, 12, 31)


def fetch_period(client: PennylaneGLClient, start: date, end: date) -> list[dict]:
    """Balance d'une période. Pagination cursor standard."""
    items, cursor = [], None
    while True:
        params = {"period_start": start.isoformat(), "period_end": end.isoformat(), "limit": 100}
        if cursor:
            params["cursor"] = cursor
        d = client._get(f"{PENNYLANE_BASE}/trial_balance", params=params)
        page = d.get("items", [])
        items.extend(page)
        cursor = d.get("next_cursor")
        if not d.get("has_more") or not cursor or not page:
            return items


def transform(item: dict, start: date, end: date, snapshot_at: str) -> dict:
    return {
        "period_start": start.isoformat(),
        "period_end": end.isoformat(),
        "account_number": str(item.get("number") or ""),
        "formatted_number": item.get("formatted_number"),
        "label": item.get("label"),
        "debits": str(item.get("debits") or "0"),
        "credits": str(item.get("credits") or "0"),
        "snapshot_at": snapshot_at,
    }


def check_balanced(rows: list[dict], start: date) -> None:
    """Σ débits == Σ crédits sur chaque période — contrôle interne offert par l'endpoint."""
    d = sum(float(r["debits"]) for r in rows)
    c = sum(float(r["credits"]) for r in rows)
    if abs(d - c) >= 0.01:
        logger.warning("⚠ balance NON équilibrée sur %s : débits=%.2f crédits=%.2f (écart %.2f)",
                       start.isoformat()[:7], d, c, d - c)


def ensure_table(bq: bigquery.Client) -> None:
    ref = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    try:
        bq.get_table(ref)
        return
    except Exception:
        pass
    t = bigquery.Table(ref, schema=bq_schema())
    t.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.MONTH, field="period_start"
    )
    t.clustering_fields = ["account_number"]
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
    keys = ("period_start", "account_number")
    set_clause = ",\n      ".join(f"{c} = S.{c}" for c in cols if c not in keys)
    bq.query(f"""
    MERGE `{target}` T
    USING `{staging}` S
    ON T.period_start = S.period_start AND T.account_number = S.account_number
    WHEN MATCHED THEN UPDATE SET
      {set_clause}
    WHEN NOT MATCHED THEN INSERT ({", ".join(cols)}) VALUES ({", ".join("S." + c for c in cols)})
    """).result()
    bq.query(f"DROP TABLE `{staging}`").result()
    return {"merged": len(rows)}


def run(from_month: str | None = None, to_month: str | None = None,
        dry_run: bool = False) -> dict:
    token = os.environ.get("PENNYLANE_TOKEN")
    if not token:
        raise SystemExit("PENNYLANE_TOKEN env var required")

    bq = bigquery.Client(project=BQ_PROJECT)
    lo, hi = open_fiscal_periods(bq)
    if from_month:
        lo = date(int(from_month[:4]), int(from_month[5:7]), 1)
    if to_month:
        hi = month_bounds(int(to_month[:4]), int(to_month[5:7]))[1]

    periods = months_between(lo, hi)
    logger.info("périmètre : %s → %s (%d mois)", lo.isoformat(), hi.isoformat(), len(periods))

    client = PennylaneGLClient(token)
    snapshot_at = datetime.now(timezone.utc).isoformat()

    rows = []
    for start, end in periods:
        items = fetch_period(client, start, end)
        page = [transform(i, start, end, snapshot_at) for i in items]
        check_balanced(page, start)
        rows.extend(page)
        logger.info("[%s] %d comptes", start.isoformat()[:7], len(page))

    if dry_run:
        return {"months": len(periods), "fetched": len(rows),
                "sample": rows[:2], "dry_run": True}
    return {"months": len(periods), "fetched": len(rows), **merge_to_bq(rows, bq)}


def cli():
    parser = argparse.ArgumentParser()
    parser.add_argument("--from-month", help="YYYY-MM (backfill ; sinon exercices ouverts)")
    parser.add_argument("--to-month", help="YYYY-MM (borne haute optionnelle)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    print(run(from_month=args.from_month, to_month=args.to_month, dry_run=args.dry_run))


if __name__ == "__main__":
    cli()
