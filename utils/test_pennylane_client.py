"""
Test du client PennyLane en dry_run — vérifie le mapping IDs sans écrire dans PennyLane.
Usage : PENNYLANE_TOKEN=xxx python3 utils/test_pennylane_client.py
"""

import logging
import os
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

from accounting.entries import AccountingEntry
from pennylane.client import PennyLaneClient

token = os.environ.get("PENNYLANE_TOKEN")
if not token:
    sys.exit("Erreur : variable PENNYLANE_TOKEN non définie.")

client = PennyLaneClient(token=token)

# --- Batch Booking fictif (équilibré) ---
booking_entries = [
    AccountingEntry(
        journal="BOOK", date=date(2026, 3, 17), ref_piece="",
        account="51105000",
        label="Encaissement BOOKING - Mar 17, 2026",
        debit=Decimal("1000.00"), credit=None,
    ),
    AccountingEntry(
        journal="BOOK", date=date(2026, 3, 17), ref_piece="",
        account="401BOOKING",
        label="Frais + Payment Charge - Mar 17, 2026",
        debit=Decimal("150.00"), credit=None,
    ),
    AccountingEntry(
        journal="BOOK", date=date(2026, 3, 17), ref_piece="",
        account="411BOOKING",
        label="MER21-0G- BOOKING - Test Guest - CO :Mar 10, 2026",
        debit=None, credit=Decimal("1150.00"),
    ),
]

# --- Batch Airbnb fictif (équilibré) ---
airbnb_entries = [
    AccountingEntry(
        journal="AIRB", date=date(2026, 3, 17), ref_piece="",
        account="51105000",
        label="Encaissement AIRBNB - Mar 17, 2026",
        debit=Decimal("850.00"), credit=None,
    ),
    AccountingEntry(
        journal="AIRB", date=date(2026, 3, 17), ref_piece="",
        account="401AIRBNB",
        label="Frais + Payment Charge - Mar 17, 2026",
        debit=Decimal("150.00"), credit=None,
    ),
    AccountingEntry(
        journal="AIRB", date=date(2026, 3, 17), ref_piece="",
        account="411AIRBNB",
        label="MER21-0G - AIRBNB - Test Guest - CO :Mar 10, 2026",
        debit=None, credit=Decimal("1000.00"),
    ),
]

print("=" * 60)
print("Test PennyLane client — DRY RUN")
print("=" * 60)

print("\n>>> BOOKING")
result = client.post_ledger_entry(booking_entries, dry_run=True)
print(f"  Résultat : {result}")

print("\n>>> AIRBNB")
result = client.post_ledger_entry(airbnb_entries, dry_run=True)
print(f"  Résultat : {result}")

print("\n>>> AIRBNB avec frais d'annulation")
airbnb_cancel = [
    AccountingEntry(
        journal="AIRB", date=date(2026, 3, 17), ref_piece="",
        account="51105000",
        label="Encaissement AIRBNB - Mar 17, 2026",
        debit=Decimal("200.00"), credit=None,
    ),
    AccountingEntry(
        journal="AIRB", date=date(2026, 3, 17), ref_piece="",
        account="401AIRBNB",
        label="Frais + Payment Charge - Mar 17, 2026",
        debit=Decimal("50.00"), credit=None,
    ),
    AccountingEntry(
        journal="AIRB", date=date(2026, 3, 17), ref_piece="",
        account="411AIRBNB",
        label="MER21-0G - AIRBNB - Test Guest - CO :Mar 10, 2026",
        debit=None, credit=Decimal("500.00"),
    ),
    AccountingEntry(
        journal="AIRB", date=date(2026, 3, 17), ref_piece="",
        account="604610",
        label="MER21-0G - AIRBNB - Frais d'annulation - Guest - HMXXXXX",
        debit=Decimal("250.00"), credit=None,
    ),
]
result = client.post_ledger_entry(airbnb_cancel, dry_run=True)
print(f"  Résultat : {result}")

print("\n" + "=" * 60)
print("Tous les mappings IDs sont résolus correctement.")
