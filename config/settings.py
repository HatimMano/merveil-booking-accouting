"""Global settings and constants for the Booking.com / Airbnb → PennyLane pipeline."""

from decimal import Decimal

# === Booking.com accounting configuration ===

JOURNAL_CODE = "BOOK"

# Account codes
ACCOUNT_CLIENT = "411BOOKING"    # Client receivable (debited for each reservation's Amount)
ACCOUNT_SUPPLIER = "401BOOKING"  # Supplier payable (credited for Booking.com fees)
ACCOUNT_BANK = "51105000"        # Bank account (debited for net payout)

# === Airbnb accounting configuration ===

AIRBNB_JOURNAL_CODE = "AIRB"

AIRBNB_ACCOUNT_CLIENT = "411AIRBNB"    # Client receivable (Airbnb)
AIRBNB_ACCOUNT_SUPPLIER = "604600"     # Direct charge — Airbnb issues 1 invoice per resa (no 401 intermediate)
AIRBNB_ACCOUNT_BANK = "51105000"       # Same bank account

# Airbnb Excel row types to include in accounting entries
AIRBNB_RESERVATION_TYPES = {"Réservation", "Régularisation de la résolution", "Hors réservation", "Frais d'annulation"}

# Account for Airbnb cancellation fees charged to the host (reclassement automatique)
AIRBNB_ACCOUNT_CANCELLATION_FEE = "604610"

# === Validation thresholds ===

# Maximum allowed difference for the global balance check
BALANCE_TOLERANCE = Decimal("0.05")  # ±0.05€

# Maximum allowed difference for per-reservation Net verification
AMOUNT_MISMATCH_TOLERANCE = Decimal("0.02")  # ±0.02€

# Commission rate bounds (as a fraction of Amount)
COMMISSION_HIGH_THRESHOLD = Decimal("0.20")  # 20% — above this triggers a warning
COMMISSION_LOW_THRESHOLD = Decimal("0.10")   # 10% — below this triggers a warning

# === Supported values ===

SUPPORTED_CURRENCIES = {"EUR"}

# Expected number of active apartments with a valid Booking.com code
EXPECTED_APARTMENT_COUNT = 115

# === File / pattern constants ===

# Booking.com CSV filename pattern: {digits}-{alphanumeric}.csv
BOOKING_FILENAME_PATTERN = r"^\d+-[A-Za-z0-9]+\.csv$"

# === Date formats ===

# Booking.com exports dates as "Aug 28 2025" or "Aug 28, 2025"
BOOKING_DATE_FORMATS = ["%b %d %Y", "%b %d, %Y"]

# Output date format used in PennyLane CSV and labels
PENNYLANE_DATE_FORMAT = "%d/%m/%Y"

# === PennyLane API — IDs numériques (résolus le 2026-03-17) ===
# Résolution via GET /api/external/v2/journals et /ledger_accounts

PENNYLANE_JOURNAL_IDS = {
    "BOOK": 3621237,
    "AIRB": 3621262,
}

PENNYLANE_ACCOUNT_IDS = {
    # Booking
    "411BOOKING":       760098756,
    "401BOOKING":       756231429,  # Commissions Booking — fournisseur (facture mensuelle / appart)
    # Airbnb
    "411AIRBNB":        671112489,
    "604600":           671113615,  # Commissions Airbnb — charge directe (PennyLane "6046")
    "604610":           671113726,  # Frais d'annulation Airbnb (PennyLane "60461")
    # Banque — compte différent selon OTA (même code interne "51105000")
    "51105000_BOOK":    671113821,  # → 51105  BOOKING
    "51105000_AIRB":    671113820,  # → 51104  AIR BNB
}
