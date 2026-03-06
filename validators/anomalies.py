"""Anomaly data model and detection rules (two-pass validation)."""

from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, List, Optional

from config.settings import (
    AMOUNT_MISMATCH_TOLERANCE,
    BALANCE_TOLERANCE,
    COMMISSION_HIGH_THRESHOLD,
    COMMISSION_LOW_THRESHOLD,
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

class AnomalyType:
    """String constants for every recognised anomaly type."""

    # Passe 1 — source data validation (before entry generation)
    FILE_BAD_NAME = "FILE_BAD_NAME"
    FILE_EMPTY = "FILE_EMPTY"
    MAPPING_NOT_FOUND = "MAPPING_NOT_FOUND"
    DUPLICATE_RESERVATION = "DUPLICATE_RESERVATION"
    NON_EUR_CURRENCY = "NON_EUR_CURRENCY"
    CANCELLED_WITH_AMOUNT = "CANCELLED_WITH_AMOUNT"
    NON_RESERVATION_TYPE = "NON_RESERVATION_TYPE"

    # Passe 2 — result validation (after entry generation)
    AMOUNT_MISMATCH = "AMOUNT_MISMATCH"
    COMMISSION_RATE_HIGH = "COMMISSION_RATE_HIGH"
    COMMISSION_RATE_LOW = "COMMISSION_RATE_LOW"
    BALANCE_ERROR = "BALANCE_ERROR"


class Severity:
    """Severity levels for anomalies."""
    BLOCKING = "BLOCKING"  # Stops processing of the affected reservation / file
    WARNING = "WARNING"    # Logged and included in the report; processing continues
    INFO = "INFO"          # Informational only


# ---------------------------------------------------------------------------
# Anomaly dataclass
# ---------------------------------------------------------------------------

@dataclass
class Anomaly:
    """Represents a single detected anomaly."""

    type: str
    """Anomaly type code (see AnomalyType)."""

    severity: str
    """Severity level: 'BLOCKING', 'WARNING', or 'INFO'."""

    message: str
    """Human-readable description of the problem."""

    source_file: str
    """CSV filename where the anomaly was detected."""

    reservation_ref: Optional[str]
    """Booking.com reservation number, if applicable."""

    details: Dict
    """Additional key/value data for debugging."""


# ---------------------------------------------------------------------------
# Passe 1 — cross-file checks (run after all files have been parsed)
# ---------------------------------------------------------------------------

def check_duplicate_reservations(reservations: list) -> List[Anomaly]:
    """
    Detect reservations whose reference number appears in more than one file.

    Args:
        reservations: All Reservation objects parsed from the input directory.

    Returns:
        List of DUPLICATE_RESERVATION anomalies (WARNING severity).
    """
    anomalies: List[Anomaly] = []
    seen: Dict[str, str] = {}  # reference_number → source_file

    for r in reservations:
        ref = r.reference_number
        if ref in seen:
            anomalies.append(Anomaly(
                type=AnomalyType.DUPLICATE_RESERVATION,
                severity=Severity.WARNING,
                message=(
                    f"Reservation {ref} appears in multiple files: "
                    f"'{seen[ref]}' and '{r.source_file}'"
                ),
                source_file=r.source_file,
                reservation_ref=ref,
                details={"first_file": seen[ref], "second_file": r.source_file},
            ))
        else:
            seen[ref] = r.source_file

    return anomalies


# ---------------------------------------------------------------------------
# Passe 2 — per-reservation checks (run after mapping + entry generation)
# ---------------------------------------------------------------------------

def validate_reservation_amounts(reservation) -> List[Anomaly]:
    """
    Validate the financial amounts of a single reservation.

    Checks:
      - Net ≈ Amount + Commission + Payment_charge + City_tax  (tolerance ±0.02€)
      - Commission rate is between 10 % and 20 % of Amount

    Args:
        reservation: A Reservation object (code_comptable already set).

    Returns:
        List of anomalies for this reservation (may be empty).
    """
    anomalies: List[Anomaly] = []
    r = reservation

    # Check: Net should equal Amount + Commission + Payment_charge + City_tax
    expected_net = r.amount + r.commission + r.payment_charge + r.city_tax
    net_diff = abs(r.net - expected_net)
    if net_diff > AMOUNT_MISMATCH_TOLERANCE:
        anomalies.append(Anomaly(
            type=AnomalyType.AMOUNT_MISMATCH,
            severity=Severity.WARNING,
            message=(
                f"Net mismatch for reservation {r.reference_number}: "
                f"computed {expected_net:.2f}, CSV shows {r.net:.2f} "
                f"(diff={net_diff:.2f}€)"
            ),
            source_file=r.source_file,
            reservation_ref=r.reference_number,
            details={
                "amount": str(r.amount),
                "commission": str(r.commission),
                "payment_charge": str(r.payment_charge),
                "city_tax": str(r.city_tax),
                "expected_net": str(expected_net),
                "actual_net": str(r.net),
                "difference": str(net_diff),
            },
        ))

    # Check commission rate (only meaningful when Amount > 0 AND Commission != 0).
    # "Paid Online" reservations have no commission column, so Commission = 0 is
    # expected — not a low-rate anomaly.
    if r.amount > Decimal("0") and r.commission != Decimal("0"):
        commission_rate = abs(r.commission) / r.amount

        if commission_rate > COMMISSION_HIGH_THRESHOLD:
            anomalies.append(Anomaly(
                type=AnomalyType.COMMISSION_RATE_HIGH,
                severity=Severity.WARNING,
                message=(
                    f"Commission rate {commission_rate:.1%} exceeds the "
                    f"{COMMISSION_HIGH_THRESHOLD:.0%} threshold "
                    f"for reservation {r.reference_number}"
                ),
                source_file=r.source_file,
                reservation_ref=r.reference_number,
                details={
                    "commission_rate": f"{commission_rate:.4f}",
                    "amount": str(r.amount),
                    "commission": str(r.commission),
                },
            ))
        elif commission_rate < COMMISSION_LOW_THRESHOLD:
            anomalies.append(Anomaly(
                type=AnomalyType.COMMISSION_RATE_LOW,
                severity=Severity.WARNING,
                message=(
                    f"Commission rate {commission_rate:.1%} is below the "
                    f"{COMMISSION_LOW_THRESHOLD:.0%} threshold "
                    f"for reservation {r.reference_number}"
                ),
                source_file=r.source_file,
                reservation_ref=r.reference_number,
                details={
                    "commission_rate": f"{commission_rate:.4f}",
                    "amount": str(r.amount),
                    "commission": str(r.commission),
                },
            ))

    return anomalies


# ---------------------------------------------------------------------------
# Passe 2 — global balance check
# ---------------------------------------------------------------------------

def check_balance(
    total_net: Decimal,
    total_amount: Decimal,
    total_commission: Decimal,
    total_payment_charge: Decimal,
    total_city_tax: Decimal,
    label: str = "global",
) -> Optional[Anomaly]:
    """
    Verify global accounting consistency across all processed reservations.

    The check is: Sum(Net) ≈ Sum(Amount) + Sum(Commission) + Sum(Payment_charge) + Sum(City_tax)

    This is the global version of the per-reservation AMOUNT_MISMATCH check and
    catches any systematic discrepancy between the CSV amounts and the net payout.
    Tolerance: ±0.05€

    Args:
        total_net:             Sum of all Net amounts.
        total_amount:          Sum of all Amount values.
        total_commission:      Sum of all Commission values (negative numbers).
        total_payment_charge:  Sum of all Payment_charge values (negative numbers).
        total_city_tax:        Sum of all City_tax values (negative numbers).
        label:                 Descriptive label for the error message.

    Returns:
        A BALANCE_ERROR Anomaly if the check fails, None otherwise.
    """
    expected_net = total_amount + total_commission + total_payment_charge + total_city_tax
    diff = abs(total_net - expected_net)
    if diff > BALANCE_TOLERANCE:
        return Anomaly(
            type=AnomalyType.BALANCE_ERROR,
            severity=Severity.BLOCKING,
            message=(
                f"Global balance check failed ({label}): "
                f"Sum(Net)={total_net:.2f} ≠ expected {expected_net:.2f} "
                f"(diff={diff:.2f}€, tolerance=±{BALANCE_TOLERANCE}€)"
            ),
            source_file="(all files)",
            reservation_ref=None,
            details={
                "total_net": str(total_net),
                "total_amount": str(total_amount),
                "total_commission": str(total_commission),
                "total_payment_charge": str(total_payment_charge),
                "total_city_tax": str(total_city_tax),
                "expected_net": str(expected_net),
                "difference": str(diff),
                "tolerance": str(BALANCE_TOLERANCE),
            },
        )
    return None
