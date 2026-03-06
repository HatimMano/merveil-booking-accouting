"""Normalized reservation data model, shared across all OTA parsers."""

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Optional


@dataclass
class Reservation:
    """
    A single reservation, normalized from any OTA source (Booking.com, Airbnb, …).

    All OTA parsers return lists of Reservation objects.  The accounting,
    validation, and reporting modules only depend on this model — they are
    OTA-agnostic.
    """

    # --- Source metadata ---
    source_file: str
    """Original CSV filename, e.g. '3015679-7oaOsO2VGKHbvBNQ.csv'."""

    ref_appart: str
    """Property reference code extracted from the filename, e.g. '3015679'."""

    payout_id: str
    """Payout identifier extracted from the filename, e.g. '7oaOsO2VGKHbvBNQ'."""

    # --- Reservation details ---
    reference_number: str
    """OTA reservation number, e.g. '5493245107'."""

    check_in: date
    checkout: date
    guest_name: str
    reservation_status: str   # "ok", "cancelled", etc.
    currency: str             # "EUR"
    payment_status: str       # "by_booking", etc.

    # --- Financial amounts (stored as Decimal for precision) ---
    # city_tax, commission, and payment_charge are negative in the source CSV.
    city_tax: Decimal
    amount: Decimal          # Gross amount billed to the guest (positive)
    commission: Decimal      # Booking.com commission (negative)
    payment_charge: Decimal  # Payment processing fee (negative)
    net: Decimal             # Net payout received (positive)

    # --- Payout metadata ---
    payout_date: date

    # --- Set after mapping lookup ---
    code_comptable: Optional[str] = None
    """Accounting code resolved from the mapping file, e.g. 'MER21-0G'."""
