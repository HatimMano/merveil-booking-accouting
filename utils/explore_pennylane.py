"""
Script d'exploration PennyLane — récupère les IDs journaux et comptes comptables.
Usage : PENNYLANE_TOKEN=xxx python3 utils/explore_pennylane.py
"""

import os
import sys
import requests

BASE_URL = "https://app.pennylane.com/api/external/v2"

token = os.environ.get("PENNYLANE_TOKEN")
if not token:
    sys.exit("Erreur : variable PENNYLANE_TOKEN non définie.")

HEADERS = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json",
    "Accept": "application/json",
}

ACCOUNT_CODES = {"411BOOKING", "411AIRBNB", "401BOOKING", "401AIRBNB", "51105000", "604610"}
JOURNAL_CODES = {"BOOK", "AIRB"}


def get_all(endpoint: str) -> list:
    """Paginate through all pages of an endpoint."""
    results = []
    page = 1
    while True:
        r = requests.get(f"{BASE_URL}/{endpoint}", headers=HEADERS, params={"page": page, "per_page": 100})
        r.raise_for_status()
        data = r.json()
        # Debug: show raw keys on first page
        if page == 1:
            print(f"  [debug] réponse brute keys={list(data.keys()) if isinstance(data, dict) else type(data)}")
            if isinstance(data, dict) and not any(k in data for k in ("ledger_accounts", "journals")):
                # Show first item to understand structure
                for k, v in data.items():
                    sample = v[:2] if isinstance(v, list) else v
                    print(f"  [debug] {k}: {sample}")
        # Try all known response keys
        items = (
            data.get("ledger_accounts")
            or data.get("journals")
            or data.get("data")
            or data.get("items")
            or []
        )
        if not items:
            break
        results.extend(items)
        if len(items) < 100:
            break
        page += 1
    return results


def main():
    print("=" * 60)
    print("PennyLane — Exploration journaux et comptes comptables")
    print("=" * 60)

    # --- Journaux ---
    print("\n>>> JOURNAUX")
    try:
        journals = get_all("journals")
        print(f"Total : {len(journals)} journaux")
        print(f"\nFiltré sur {JOURNAL_CODES} :")
        found_journals = {}
        for j in journals:
            code = j.get("code", "")
            if code in JOURNAL_CODES:
                print(f"  id={j['id']:>8}  code={code:<10}  label={j.get('label', '')}")
                found_journals[code] = j["id"]
        if not found_journals:
            print("  Aucun journal BOOK/AIRB trouvé — liste complète :")
            for j in journals:
                print(f"  id={j['id']:>8}  code={j.get('code','')::<10}  label={j.get('label','')}")
    except requests.HTTPError as e:
        print(f"Erreur journaux : {e.response.status_code} — {e.response.text}")

    # --- Comptes comptables ---
    print("\n>>> COMPTES COMPTABLES")
    try:
        accounts = get_all("ledger_accounts")
        print(f"Total : {len(accounts)} comptes")
        print(f"\nFiltré sur {ACCOUNT_CODES} :")
        found_accounts = {}
        for a in accounts:
            number = a.get("number", "") or a.get("code", "")
            if number in ACCOUNT_CODES:
                print(f"  id={a['id']:>8}  number={number:<15}  label={a.get('label', a.get('name', ''))}")
                found_accounts[number] = a["id"]
        if not found_accounts:
            print("  Aucun compte trouvé — les 30 premiers comptes disponibles :")
            for a in accounts[:30]:
                number = a.get("number", "") or a.get("code", "")
                print(f"  id={a['id']:>8}  number={number:<15}  label={a.get('label', a.get('name', ''))}")
    except requests.HTTPError as e:
        print(f"Erreur comptes : {e.response.status_code} — {e.response.text}")

    print("\n" + "=" * 60)
    print("Copie ces IDs dans config/settings.py une fois validés.")


if __name__ == "__main__":
    main()
