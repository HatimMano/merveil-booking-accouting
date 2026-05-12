"""
Fetch des écritures sur 411AIRBNB / 411BOOKING en avril 2026, en filtrant
côté client (l'API ignore le filtre par compte). Suit les lettrages pour
sortir des exemples bout-en-bout vente Mews ↔ encaissement OTA.

Usage :
    PENNYLANE_TOKEN=$(gcloud secrets versions access latest --secret=pennylane-token \
        --project=merveil-data-warehouse) python3 utils/check_lettering.py
"""

import os
import sys
from collections import Counter, defaultdict

import requests

BASE_URL = "https://app.pennylane.com/api/external/v2"
DATE_FROM = "2026-04-01"
DATE_TO   = "2026-04-30"
TARGET_ACCOUNTS = {"411AIRBNB", "411BOOKING"}

token = os.environ.get("PENNYLANE_TOKEN")
if not token:
    sys.exit("Erreur : PENNYLANE_TOKEN non défini.")

HEADERS = {"Authorization": f"Bearer {token}", "Accept": "application/json"}


def paginate(endpoint, params, max_pages=100):
    items = []
    cursor = None
    p = {k: v for k, v in params.items() if k != "per_page"}
    p.setdefault("limit", 100)
    for _ in range(max_pages):
        if cursor:
            p["cursor"] = cursor
        r = requests.get(f"{BASE_URL}/{endpoint}", headers=HEADERS, params=p)
        if r.status_code != 200:
            print(f"    ✗ {r.status_code} : {r.text[:200]}")
            return items
        d = r.json()
        items.extend(d.get("items", []))
        if not d.get("has_more"):
            break
        cursor = d.get("next_cursor")
        if not cursor:
            break
    return items


# Cache pour éviter les GET répétés
_entry_cache = {}
_line_cache = {}

def get_entry(eid):
    if eid not in _entry_cache:
        r = requests.get(f"{BASE_URL}/ledger_entries/{eid}", headers=HEADERS)
        _entry_cache[eid] = r.json() if r.status_code == 200 else {"_error": r.status_code}
    return _entry_cache[eid]

def get_line(lid):
    if lid not in _line_cache:
        r = requests.get(f"{BASE_URL}/ledger_entry_lines/{lid}", headers=HEADERS)
        _line_cache[lid] = r.json() if r.status_code == 200 else {"_error": r.status_code}
    return _line_cache[lid]


# ---------- 1. Get raw lines + filter client-side ----------
print("=" * 80)
print(f"STEP 1 — Récupération + filtre client-side sur {TARGET_ACCOUNTS}")
print("=" * 80)

# Récupère beaucoup de lignes (le filtre API ne marche pas, on doit ratisser)
raw_lines = paginate("ledger_entry_lines", {"ledger_account_number": "411AIRBNB"}, max_pages=100)
print(f"  Brut (ratissage 1) : {len(raw_lines)}")
raw_lines2 = paginate("ledger_entry_lines", {"ledger_account_number": "411BOOKING"}, max_pages=100)
print(f"  Brut (ratissage 2) : {len(raw_lines2)}")

# Dédupe sur line id
all_seen = {}
for l in raw_lines + raw_lines2:
    all_seen[l["id"]] = l
print(f"  Total dédupé       : {len(all_seen)}")

# Filtre client-side sur compte + date
filtered = [
    l for l in all_seen.values()
    if (l.get("ledger_account") or {}).get("number") in TARGET_ACCOUNTS
       and DATE_FROM <= (l.get("date") or "") <= DATE_TO
]
print(f"  Filtré (compte 411 + avril) : {len(filtered)}")

# Si pas assez, essayer un filtre par date côté API
if len(filtered) < 50:
    print("\n  ⚠ Trop peu de lignes — tentative avec filtre date côté API")
    for date_param in ("date_from", "from_date", "start_date", "posted_after"):
        more = paginate("ledger_entry_lines", {date_param: DATE_FROM, "to_date": DATE_TO}, max_pages=20)
        if more:
            print(f"    {date_param}={DATE_FROM} → {len(more)} lignes")
            for l in more:
                all_seen[l["id"]] = l
            break
    filtered = [
        l for l in all_seen.values()
        if (l.get("ledger_account") or {}).get("number") in TARGET_ACCOUNTS
           and DATE_FROM <= (l.get("date") or "") <= DATE_TO
    ]
    print(f"    après extension : {len(filtered)} lignes 411 en avril")

# Distribution
by_acct = Counter((l.get("ledger_account") or {}).get("number") for l in filtered)
by_side = Counter("D" if float(l.get("debit") or 0) > 0 else "C" for l in filtered)
print(f"\n  Par compte : {dict(by_acct)}")
print(f"  Par sens   : {dict(by_side)}")

# ---------- 2. Stats lettrage ----------
print("\n" + "=" * 80)
print("STEP 2 — Lettrage")
print("=" * 80)

lettered_pairs = {}  # key = tuple(sorted ids), value = sample line
unlettered = []
for l in filtered:
    ids = (l.get("lettered_ledger_entry_lines") or {}).get("ids") or []
    if len(ids) >= 2:
        key = tuple(sorted(ids))
        if key not in lettered_pairs:
            lettered_pairs[key] = l
    else:
        unlettered.append(l)

print(f"  Groupes de lettrage uniques : {len(lettered_pairs)}")
print(f"  Lignes non lettrées         : {len(unlettered)}")

# Taille des groupes
sizes = Counter(len(k) for k in lettered_pairs.keys())
print(f"  Distribution tailles : {dict(sizes)}")

# ---------- 3. EXEMPLES BOUT-EN-BOUT par OTA ----------
print("\n" + "=" * 80)
print("STEP 3 — EXEMPLES BOUT-EN-BOUT (1 par OTA, lettrage 1-to-1)")
print("=" * 80)

# Trier les lettrages 1-to-1 par compte 411
pairs_by_acct = defaultdict(list)
for key, sample in lettered_pairs.items():
    if len(key) == 2:
        acct = (sample.get("ledger_account") or {}).get("number")
        pairs_by_acct[acct].append((key, sample))

for acct, pairs in pairs_by_acct.items():
    print(f"\n  ▓▓▓▓ {acct} — exemples ▓▓▓▓")
    for i, (ids_tuple, _) in enumerate(pairs[:2], 1):
        print(f"\n  ── EXEMPLE {acct} #{i} (line_ids lettrés = {list(ids_tuple)}) ──")
        for lid in ids_tuple:
            line = get_line(lid)
            if "_error" in line:
                print(f"    ✗ line {lid} : {line['_error']}")
                continue
            eid = (line.get("ledger_entry") or {}).get("id")
            entry = get_entry(eid) if eid else {}
            line_acct = (line.get("ledger_account") or {}).get("number")
            print(f"\n    LIGNE id={lid} compte={line_acct}")
            print(f"      date         : {line.get('date')}")
            print(f"      D / C        : {line.get('debit')} / {line.get('credit')}")
            print(f"      label_ligne  : '{line.get('label') or ''}'")
            print(f"      → écriture parente id={eid}")
            print(f"          entry.label          : {entry.get('label')}")
            print(f"          entry.date           : {entry.get('date')}")
            print(f"          entry.piece_number   : {entry.get('piece_number')}")
            print(f"          entry.invoice_number : {entry.get('invoice_number')}")
            j = entry.get("journal") or {}
            print(f"          entry.journal        : id={j.get('id')}  code={j.get('code')}  label={j.get('label')}")
            lines = entry.get("ledger_entry_lines") or []
            print(f"          nb lignes écriture   : {len(lines)}")
            # Print only the first 5 lines if many (payout has 50+ lines)
            for el in lines[:6]:
                a = (el.get("ledger_account") or {}).get("number")
                print(f"            acct={a:<15} D={str(el.get('debit') or '0'):>11}  "
                      f"C={str(el.get('credit') or '0'):>11}  label='{(el.get('label') or '')[:55]}'")
            if len(lines) > 6:
                print(f"            ... +{len(lines)-6} autres lignes")

# ---------- 4. Exemple GROS lettrage (payout entier) ----------
print("\n" + "=" * 80)
print("STEP 4 — Lettrage de taille > 5 (payouts entiers ?)")
print("=" * 80)

big_pairs = [(k, v) for k, v in lettered_pairs.items() if len(k) >= 5]
print(f"  Trouvé {len(big_pairs)} groupes ≥ 5 lignes")

for ids_tuple, sample in big_pairs[:1]:
    acct = (sample.get("ledger_account") or {}).get("number")
    print(f"\n  ── Lettrage {acct} de {len(ids_tuple)} lignes ──")
    entries_involved = defaultdict(list)
    for lid in ids_tuple[:20]:  # limite à 20 sinon trop d'appels
        line = get_line(lid)
        eid = (line.get("ledger_entry") or {}).get("id")
        entries_involved[eid].append(line)
    print(f"  → impliquant {len(entries_involved)} écriture(s) distincte(s)")
    for eid, lines in entries_involved.items():
        entry = get_entry(eid)
        j = entry.get("journal") or {}
        print(f"\n    écriture {eid} : '{entry.get('label')}' "
              f"(date={entry.get('date')}, journal_id={j.get('id')}, piece={entry.get('piece_number')})")
        for l in lines[:3]:
            a = (l.get("ledger_account") or {}).get("number")
            print(f"      ligne {l.get('id')} acct={a} D={l.get('debit')} C={l.get('credit')}")

print("\n" + "=" * 80)
print("FIN")
print("=" * 80)
