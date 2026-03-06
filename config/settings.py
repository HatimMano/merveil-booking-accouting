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
AIRBNB_ACCOUNT_SUPPLIER = "401AIRBNB"  # Supplier payable — Airbnb host service fees
AIRBNB_ACCOUNT_BANK = "51105000"       # Same bank account

# Airbnb Excel row types to include in accounting entries
AIRBNB_RESERVATION_TYPES = {"Réservation", "Régularisation de la résolution", "Hors réservation"}

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
