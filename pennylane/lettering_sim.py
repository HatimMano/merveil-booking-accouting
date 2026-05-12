"""
Simulation lettrage PennyLane × Mews.

Pour chaque ligne 411AIRBNB / 411BOOKING sur la fenêtre, propose une paire
candidate via (code_apt_nuits, montant) et compare avec le lettrage manuel
effectif côté comptable (lettered_ledger_entry_lines.ids).

Append-only dans `pennylane.lettering_simulation`. Aucun POST PennyLane.

Le module est utilisable comme job autonome (Cloud Run Job) :
    python -m pennylane.lettering_sim --days 60
"""

from __future__ import annotations

import logging
import os
import re
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Iterable

import requests
from google.cloud import bigquery

logger = logging.getLogger(__name__)

PENNYLANE_BASE = "https://app.pennylane.com/api/external/v2"
TABLE_REF = "merveil-data-warehouse.pennylane.lettering_simulation"

# Tolerance pour matcher 2 montants (centime près)
AMOUNT_TOLERANCE = Decimal("0.01")

# Périmètre V0 : Airbnb + Booking
TARGET_ACCOUNTS = ("411AIRBNB", "411BOOKING")

# Code apt-nuits format observé : 3-6 alphanum + "-" + 1-3 alphanum
# Ex : MAR135-2F, GOU71-3D, HAU152-0G, ABO108-2G
APT_CODE_RE = re.compile(r"\b([A-Z]{3}\d{1,3}[A-Z]?-\d{1,2}[A-Z])\b")

# V1 : extraction checkout_date depuis les labels (cf. exemples ci-dessous).
# - Côté DEBIT (vente Mews) : "25/04/2026-30/04/2026" → checkout = 2e date
# - Côté CREDIT (encaissement) :
#     - Airbnb : "CO :Apr 30, 2026"  (anglais, mois 3 lettres)
#     - Booking : "CO :30 avr. 2026" (français, jour mois année)
_MONTHS = {
    "jan": 1, "janv": 1, "feb": 2, "févr": 2, "fev": 2,
    "mar": 3, "mars": 3, "apr": 4, "avr": 4,
    "may": 5, "mai": 5, "jun": 6, "juin": 6,
    "jul": 7, "juil": 7, "aug": 8, "août": 8, "aou": 8,
    "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12, "déc": 12,
}
_DEBIT_RANGE_RE = re.compile(r"\d{1,2}/\d{1,2}/(\d{4})\s*-\s*(\d{1,2})/(\d{1,2})/(\d{4})")
_CREDIT_EN_RE   = re.compile(r"CO\s*:\s*([A-Za-z]{3,5})\.?\s+(\d{1,2}),\s*(\d{4})")
_CREDIT_FR_RE   = re.compile(r"CO\s*:\s*(\d{1,2})\s+([a-zA-Zéèûôîâ]{3,5})\.?\s+(\d{4})")


def _month_num(name: str) -> int | None:
    return _MONTHS.get(name.lower().rstrip(".")[:4]) or _MONTHS.get(name.lower().rstrip(".")[:3])


def extract_checkout_date(label_line: str, label_entry: str, side: str) -> str | None:
    """Renvoie la date de checkout au format 'YYYY-MM-DD' depuis les labels.
    Stratégie distincte selon le côté (vente vs encaissement)."""
    if side == "DEBIT":
        # Cherche d'abord dans le label de ligne, sinon dans le label entry
        for src in (label_line or "", label_entry or ""):
            m = _DEBIT_RANGE_RE.search(src)
            if m:
                return f"{m.group(4)}-{m.group(3).zfill(2)}-{m.group(2).zfill(2)}"
        return None
    # CREDIT
    for src in (label_line or "", label_entry or ""):
        # Anglais d'abord : "Apr 30, 2026"
        m = _CREDIT_EN_RE.search(src)
        if m:
            mon = _month_num(m.group(1))
            if mon:
                return f"{m.group(3)}-{mon:02d}-{int(m.group(2)):02d}"
        # Français : "30 avr. 2026"
        m = _CREDIT_FR_RE.search(src)
        if m:
            mon = _month_num(m.group(2))
            if mon:
                return f"{m.group(3)}-{mon:02d}-{int(m.group(1)):02d}"
    return None


# ----------------------------------------------------------------------
# Modèle interne
# ----------------------------------------------------------------------

@dataclass
class Line:
    """Une ligne PennyLane simplifiée pour le matching."""
    id: int
    account: str
    line_date: str | None
    debit: Decimal
    credit: Decimal
    label_line: str
    entry_id: int | None
    entry_label: str | None
    entry_date: str | None
    journal_id: int | None
    lettered_with: list[int] = field(default_factory=list)

    @property
    def side(self) -> str:
        return "DEBIT" if self.debit > 0 else "CREDIT"

    @property
    def amount(self) -> Decimal:
        return self.debit if self.debit > 0 else self.credit

    # apt_code et checkout_date sont calculés/enrichis dans fetch_target_lines
    # (apt_code peut requérir un fallback sur les soeurs de l'écriture parente)
    apt_code_override: str | None = None
    checkout_date: str | None = None

    @property
    def apt_code(self) -> str | None:
        if self.apt_code_override:
            return self.apt_code_override
        for source in (self.label_line or "", self.entry_label or ""):
            m = APT_CODE_RE.search(source)
            if m:
                return m.group(1)
        return None


# ----------------------------------------------------------------------
# Client PennyLane (read-only)
# ----------------------------------------------------------------------

class PennyLaneReader:
    """Pagination cursor + cache + fetch entry."""

    def __init__(self, token: str):
        self._sess = requests.Session()
        self._sess.headers.update({
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        })
        self._entry_cache: dict[int, dict] = {}

    def paginate(self, endpoint: str, params: dict, max_pages: int = 200) -> Iterable[dict]:
        cursor = None
        p = {k: v for k, v in params.items() if k != "per_page"}
        p.setdefault("limit", 100)
        for _ in range(max_pages):
            if cursor:
                p["cursor"] = cursor
            d = self._get_with_retry_params(
                f"{PENNYLANE_BASE}/{endpoint}",
                params=p,
            )
            for item in d.get("items", []):
                yield item
            if not d.get("has_more"):
                return
            cursor = d.get("next_cursor")
            if not cursor:
                return

    def _get_with_retry_params(self, url: str, params: dict, max_retries: int = 6) -> dict:
        """Same as _get_with_retry mais avec query params."""
        delay = 2.0
        for attempt in range(max_retries):
            r = self._sess.get(url, params=params)
            if r.status_code == 429:
                logger.info("429 paginate, sleep %.1fs (attempt %d/%d)", delay, attempt + 1, max_retries)
                time.sleep(delay)
                delay *= 2
                continue
            r.raise_for_status()
            return r.json()
        raise requests.HTTPError(f"429 rate limit after {max_retries} retries on {url}")

    def _get_with_retry(self, url: str, max_retries: int = 5) -> dict:
        """GET avec backoff exponentiel sur 429 (rate limit)."""
        delay = 1.0
        for attempt in range(max_retries):
            r = self._sess.get(url)
            if r.status_code == 429:
                logger.info("429 rate limit, sleep %.1fs (attempt %d/%d)", delay, attempt + 1, max_retries)
                time.sleep(delay)
                delay *= 2
                continue
            r.raise_for_status()
            return r.json()
        raise requests.HTTPError(f"429 rate limit after {max_retries} retries on {url}")

    def get_entry(self, entry_id: int) -> dict:
        if entry_id not in self._entry_cache:
            self._entry_cache[entry_id] = self._get_with_retry(
                f"{PENNYLANE_BASE}/ledger_entries/{entry_id}"
            )
        return self._entry_cache[entry_id]


def _to_decimal(v) -> Decimal:
    if v in (None, "", 0):
        return Decimal("0")
    return Decimal(str(v))


def fetch_target_lines(
    reader: PennyLaneReader,
    from_date: date,
    to_date: date,
) -> list[Line]:
    """Récupère toutes les lignes sur les comptes 411 cibles dans la fenêtre.

    L'API PennyLane v2 ignore le filtre `ledger_account_number` → on ratisse
    large puis on filtre côté client.
    """
    seen: dict[int, dict] = {}
    # Ratissage : 2 passes (1 par compte) pour maximiser la couverture
    for acct in TARGET_ACCOUNTS:
        n_before = len(seen)
        for raw in reader.paginate(
            "ledger_entry_lines",
            {"ledger_account_number": acct},
            max_pages=200,
        ):
            seen[raw["id"]] = raw
        logger.info("Ratissage %s : +%d lignes (total %d)", acct, len(seen) - n_before, len(seen))

    # Filtre client-side
    target: list[Line] = []
    entry_ids_needed: set[int] = set()
    for raw in seen.values():
        acct = (raw.get("ledger_account") or {}).get("number")
        if acct not in TARGET_ACCOUNTS:
            continue
        d = raw.get("date") or ""
        if not (from_date.isoformat() <= d <= to_date.isoformat()):
            continue
        entry_id = (raw.get("ledger_entry") or {}).get("id")
        if entry_id:
            entry_ids_needed.add(entry_id)
        target.append(Line(
            id=raw["id"],
            account=acct,
            line_date=d,
            debit=_to_decimal(raw.get("debit")),
            credit=_to_decimal(raw.get("credit")),
            label_line=raw.get("label") or "",
            entry_id=entry_id,
            entry_label=None,  # rempli ci-dessous
            entry_date=None,
            journal_id=None,
            lettered_with=[
                lid for lid in ((raw.get("lettered_ledger_entry_lines") or {}).get("ids") or [])
                if lid != raw["id"]  # exclure soi-même
            ],
        ))
    logger.info("Lignes 411 cibles dans la fenêtre : %d (de %d entries distinctes)",
                len(target), len(entry_ids_needed))

    # Enrichir label/date/journal_id de chaque entry (1 GET / entry)
    # + Fix A : si une ligne 411 a un label trop court (pas d'apt_code), on cherche
    #   l'apt_code dans les AUTRES lignes de la même écriture (les soeurs partagent
    #   l'apt sur une vente Mews : 411 + 708101 + 44571008 pointent toutes la même résa).
    for i, line in enumerate(target):
        if not line.entry_id:
            continue
        try:
            entry = reader.get_entry(line.entry_id)
            line.entry_label = entry.get("label")
            line.entry_date = entry.get("date")
            line.journal_id = (entry.get("journal") or {}).get("id")
            # Fix A : si la ligne n'a pas trouvé d'apt_code, scanner les soeurs
            if line.apt_code is None:
                for sister in entry.get("ledger_entry_lines", []) or []:
                    sister_label = sister.get("label") or ""
                    m = APT_CODE_RE.search(sister_label)
                    if m:
                        line.apt_code_override = m.group(1)
                        break
            # V1 : extraction checkout_date (label de ligne + fallback soeurs)
            line.checkout_date = extract_checkout_date(line.label_line, line.entry_label, line.side)
            if line.checkout_date is None:
                # Fallback : chercher dans les soeurs de l'écriture (sur DEBIT vente)
                for sister in entry.get("ledger_entry_lines", []) or []:
                    sister_co = extract_checkout_date(sister.get("label") or "", line.entry_label, line.side)
                    if sister_co:
                        line.checkout_date = sister_co
                        break
        except requests.HTTPError as e:
            logger.warning("Skip entry %s : %s", line.entry_id, e)
        if (i + 1) % 100 == 0:
            logger.info("  → %d/%d entries enrichies", i + 1, len(target))

    return target


# ----------------------------------------------------------------------
# Algorithme de matching
# ----------------------------------------------------------------------

def build_pair_proposals(lines: list[Line]) -> dict[int, dict]:
    """Pour chaque ligne, propose une paire candidate.

    Stratégie V1 :
      - Index *précis* DEBIT par (account, apt_code, amount, checkout_date)
      - Index *fallback* DEBIT par (account, apt_code, amount) — pour les lignes
        sans checkout_date côté l'un des 2 sides
      - Lookup en 2 étapes : précis → fallback
      - Si 1 candidat à l'un des 2 niveaux → 'paired'
      - Si 0 candidat → 'no_match'
      - Si >1 candidats (même au niveau précis) → 'ambiguous' (pas de tie-breaker
        heuristique, on laisse le comptable trancher)

    Précondition : DEBIT et CREDIT partagent le même compte 411 (411AIRBNB ↔
    411AIRBNB, 411BOOKING ↔ 411BOOKING) — vérifié par les requêtes BQ.
    """
    debit_precise: dict[tuple, list[Line]] = defaultdict(list)
    debit_fallback: dict[tuple, list[Line]] = defaultdict(list)
    credit_precise: dict[tuple, list[Line]] = defaultdict(list)
    credit_fallback: dict[tuple, list[Line]] = defaultdict(list)

    for ln in lines:
        if not ln.apt_code:
            continue
        key_fb = (ln.account, ln.apt_code, ln.amount)
        if ln.debit > 0:
            debit_fallback[key_fb].append(ln)
            if ln.checkout_date:
                debit_precise[(*key_fb, ln.checkout_date)].append(ln)
        elif ln.credit > 0:
            credit_fallback[key_fb].append(ln)
            if ln.checkout_date:
                credit_precise[(*key_fb, ln.checkout_date)].append(ln)

    proposals: dict[int, dict] = {}
    for ln in lines:
        if not ln.apt_code:
            proposals[ln.id] = {
                "prediction": "no_match",
                "paired_line_id": None,
                "paired_amount": None,
                "paired_apt_code": None,
                "ambiguity_count": 0,
            }
            continue

        # Étape 1 : lookup précis (avec checkout_date)
        candidates: list[Line] = []
        if ln.checkout_date:
            idx_precise = debit_precise if ln.side == "CREDIT" else credit_precise
            candidates = idx_precise.get(
                (ln.account, ln.apt_code, ln.amount, ln.checkout_date), []
            )
            candidates = [c for c in candidates if c.id != ln.id]

        # Étape 2 : fallback (sans checkout_date) si rien trouvé en précis
        used_fallback = False
        if not candidates:
            idx_fb = debit_fallback if ln.side == "CREDIT" else credit_fallback
            candidates = idx_fb.get((ln.account, ln.apt_code, ln.amount), [])
            candidates = [c for c in candidates if c.id != ln.id]
            used_fallback = True

        if len(candidates) == 1:
            c = candidates[0]
            proposals[ln.id] = {
                "prediction": "paired",
                "paired_line_id": c.id,
                "paired_amount": c.amount,
                "paired_apt_code": c.apt_code,
                "ambiguity_count": 1,
            }
        elif len(candidates) > 1:
            proposals[ln.id] = {
                "prediction": "ambiguous",
                "paired_line_id": None,
                "paired_amount": None,
                "paired_apt_code": None,
                "ambiguity_count": len(candidates),
            }
        else:
            proposals[ln.id] = {
                "prediction": "no_match",
                "paired_line_id": None,
                "paired_amount": None,
                "paired_apt_code": None,
                "ambiguity_count": 0,
            }
    return proposals


def compute_sim_status(line: Line, proposal: dict) -> str:
    """Compare notre proposition au lettrage comptable.

    'PENDING'  : comptable n'a pas lettré cette ligne (peu importe notre prédiction)
    'MATCH'    : on propose la même paire que le comptable
    'WRONG'    : on propose une paire différente
    'MISS'     : comptable a lettré, on n'a rien proposé (no_match/ambiguous)
    """
    lettered = line.lettered_with
    if not lettered:
        return "PENDING"
    # Comptable a lettré → on compare
    if proposal["prediction"] != "paired":
        return "MISS"
    if proposal["paired_line_id"] in lettered:
        return "MATCH"
    return "WRONG"


# ----------------------------------------------------------------------
# Écriture BigQuery
# ----------------------------------------------------------------------

def write_to_bq(
    bq: bigquery.Client,
    run_id: str,
    run_at: datetime,
    lines: list[Line],
    proposals: dict[int, dict],
) -> int:
    """Insert append-only des résultats de simulation."""
    rows = []
    for line in lines:
        proposal = proposals[line.id]
        sim_status = compute_sim_status(line, proposal)
        rows.append({
            "run_id": run_id,
            "run_at": run_at.isoformat(),
            "line_id": line.id,
            "account": line.account,
            "line_date": line.line_date,
            "side": line.side,
            "amount": float(line.amount),
            "apt_code": line.apt_code,
            "label_line": line.label_line[:500] if line.label_line else None,
            "ledger_entry_id": line.entry_id,
            "entry_label": line.entry_label[:500] if line.entry_label else None,
            "entry_date": line.entry_date,
            "journal_id": line.journal_id,
            "our_prediction": proposal["prediction"],
            "our_paired_line_id": proposal["paired_line_id"],
            "our_paired_amount": float(proposal["paired_amount"]) if proposal["paired_amount"] else None,
            "our_paired_apt_code": proposal["paired_apt_code"],
            "ambiguity_count": proposal["ambiguity_count"],
            "comptable_status": "lettered" if line.lettered_with else "not_lettered",
            "comptable_lettered_with": line.lettered_with,
            "sim_status": sim_status,
        })

    errors = bq.insert_rows_json(TABLE_REF, rows)
    if errors:
        logger.error("BQ insert errors: %s", errors[:3])
        raise RuntimeError(f"{len(errors)} insert errors")
    logger.info("Insert BQ : %d rows", len(rows))
    return len(rows)


# ----------------------------------------------------------------------
# Entry point
# ----------------------------------------------------------------------

def run(days: int = 60) -> dict:
    """Pipeline complet — appelable depuis Cloud Run Job ou CLI."""
    token = os.environ.get("PENNYLANE_TOKEN")
    if not token:
        raise RuntimeError("PENNYLANE_TOKEN manquant")

    run_at = datetime.now(timezone.utc)
    run_id = run_at.strftime("%Y%m%dT%H%M%SZ")
    to_date = run_at.date()
    from_date = to_date - timedelta(days=days)

    logger.info("=" * 70)
    logger.info("RUN %s — fenêtre %s → %s (%d j)", run_id, from_date, to_date, days)
    logger.info("=" * 70)

    reader = PennyLaneReader(token)
    lines = fetch_target_lines(reader, from_date, to_date)
    proposals = build_pair_proposals(lines)

    # Stats avant insert
    sim_counter: dict[str, int] = defaultdict(int)
    for line in lines:
        sim_counter[compute_sim_status(line, proposals[line.id])] += 1
    logger.info("Sim status : %s", dict(sim_counter))

    bq = bigquery.Client(project="merveil-data-warehouse")
    n = write_to_bq(bq, run_id, run_at, lines, proposals)

    return {
        "run_id": run_id,
        "from_date": from_date.isoformat(),
        "to_date": to_date.isoformat(),
        "n_lines": len(lines),
        "n_inserted": n,
        "stats": dict(sim_counter),
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    days = int(sys.argv[1]) if len(sys.argv) > 1 else 60
    result = run(days=days)
    print()
    print("=" * 70)
    print("RÉSULTAT")
    print("=" * 70)
    for k, v in result.items():
        print(f"  {k:15} : {v}")
