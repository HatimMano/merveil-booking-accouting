"""Generate PennyLane accounting entries from normalized reservations."""

import logging
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

from config.settings import (
    ACCOUNT_BANK,
    ACCOUNT_CLIENT,
    ACCOUNT_SUPPLIER,
    JOURNAL_CODE,
    AIRBNB_JOURNAL_CODE,
    AIRBNB_ACCOUNT_BANK,
    AIRBNB_ACCOUNT_CLIENT,
    AIRBNB_ACCOUNT_SUPPLIER,
)
from models.reservation import Reservation
from validators.anomalies import Anomaly, AnomalyType, Severity

logger = logging.getLogger(__name__)

# English month abbreviations (locale-independent)
_MONTHS = {
    1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
    7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
}


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class AccountingEntry:
    """A single line in the PennyLane accounting journal."""

    journal: str          # e.g. "BOOK"
    date: date            # Python date object (stored natively in Excel)
    ref_piece: str        # usually empty for Booking.com entries
    account: str          # e.g. "411BOOKING", "401BOOKING", "51105000"
    label: str
    debit: Optional[Decimal]   # None → empty cell
    credit: Optional[Decimal]  # None → empty cell


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _label_date(d: date) -> str:
    """Format a date as English label, e.g. 'Jan 29, 2026' (no leading zero)."""
    return f"{_MONTHS[d.month]} {d.day}, {d.year}"


# ---------------------------------------------------------------------------
# Main function
# ---------------------------------------------------------------------------

def generate_entries(
    reservations: List[Reservation],
    processing_date: date,
    mapping: Dict[str, str],
    *,
    journal_code: str = JOURNAL_CODE,
    account_bank: str = ACCOUNT_BANK,
    account_client: str = ACCOUNT_CLIENT,
    account_supplier: str = ACCOUNT_SUPPLIER,
    account_cancellation_fee: Optional[str] = None,
    ota_label: str = "BOOKING",
) -> Tuple[List[AccountingEntry], List[Reservation], List[Anomaly]]:
    """
    Generate PennyLane accounting entries for a batch of reservations.

    Journal structure (matches the accountant's reference format):
      - Header 1: DEBIT  account_bank     = Sum(Net)              — total bank receipt
      - Header 2: DEBIT  account_supplier = Sum(|Commission| + |Payment_charge|)  — total OTA fees
      - Per-res:  CREDIT account_client   = Amount − |CityTax|   — gross rental excl. city tax

    The journal is balanced: Total DEBIT = Total CREDIT.
      Proof per reservation: Net + Fees = Amount − |CityTax|
      (since Net = Amount − Fees − |CityTax|)

    Reservations whose ref_appart is not found in the mapping are skipped and
    a MAPPING_NOT_FOUND anomaly is generated for each one.

    Args:
        reservations:     Normalized reservations from the parser.
        processing_date:  Date to stamp on every accounting entry.
        mapping:          Dict of OTA-ref → accounting-code from the mapping file.
        journal_code:            Journal code (default "BOOK", use "AIRB" for Airbnb).
        account_bank:            Bank account code (default "51105000").
        account_client:          Client account code (default "411BOOKING").
        account_supplier:        Supplier account code (default "401BOOKING").
        account_cancellation_fee: If provided, Airbnb "Frais d'annulation" rows are
                                  routed here (e.g. "604610") instead of account_client.
        ota_label:               OTA name used in entry labels (default "BOOKING").

    Returns:
        (entries, processed_reservations, anomalies)
    """
    anomalies: List[Anomaly] = []
    valid_reservations: List[Reservation] = []

    # --- Step 1: resolve mapping for every reservation ---
    for r in reservations:
        code_comptable = mapping.get(r.ref_appart)
        if code_comptable is None:
            anomalies.append(Anomaly(
                type=AnomalyType.MAPPING_NOT_FOUND,
                severity=Severity.BLOCKING,
                message=(
                    f"Ref Appart '{r.ref_appart}' not found in mapping — "
                    f"reservation {r.reference_number} skipped (file: {r.source_file})"
                ),
                source_file=r.source_file,
                reservation_ref=r.reference_number,
                details={"ref_appart": r.ref_appart},
            ))
            continue

        r.code_comptable = code_comptable
        valid_reservations.append(r)

    if not valid_reservations:
        logger.warning("No reservations remain after mapping lookup.")
        return [], [], anomalies

    # --- Step 2: compute aggregate totals ---
    total_net  = sum(r.net for r in valid_reservations)
    # Use -commission - payment_charge (not abs) so refund rows where Booking
    # reimburses their commission correctly reduce the total instead of adding to it.
    total_fees = sum(-r.commission - r.payment_charge for r in valid_reservations)

    # All reservations in a payout batch share the same payout_date.
    # Use the first reservation's payout_date for the global label.
    payout_label = _label_date(valid_reservations[0].payout_date)

    # --- Step 3: build entries list ---
    entries: List[AccountingEntry] = []

    # Header 1 — total bank receipts (net payout)
    # Normal: DEBIT bank = total_net (positive)
    # Refund batch: CREDIT bank = |total_net| (negative net means money flows back to OTA)
    entries.append(AccountingEntry(
        journal=journal_code,
        date=processing_date,
        ref_piece="",
        account=account_bank,
        label=f"Encaissement {ota_label} - {payout_label}",
        debit=total_net if total_net >= 0 else None,
        credit=None if total_net >= 0 else -total_net,
    ))

    # Header 2 — total OTA fees (commissions + payment charges)
    # Normal: DEBIT supplier = total_fees (positive)
    # Fee reimbursement: CREDIT supplier = |total_fees|
    entries.append(AccountingEntry(
        journal=journal_code,
        date=processing_date,
        ref_piece="",
        account=account_supplier,
        label=f"Frais + Payment Charge  - {payout_label}",
        debit=total_fees if total_fees >= 0 else None,
        credit=None if total_fees >= 0 else -total_fees,
    ))

    # Per-reservation entries
    # Normal reservation:   CREDIT account_client            = gross (positive)
    # Adjustment/refund:    DEBIT  account_client            = |gross| (reversal)
    # Cancellation fee:     DEBIT  account_cancellation_fee  = |gross| (expense)
    for r in valid_reservations:
        checkout_label = _label_date(r.checkout)
        gross_excl_city_tax = r.amount + r.city_tax  # city_tax is stored negative

        is_cancellation_fee = (
            account_cancellation_fee is not None
            and r.reservation_status == "Frais d'annulation"
        )

        if is_cancellation_fee:
            # Route directly to 604610 — no intermediate 411AIRBNB step
            entries.append(AccountingEntry(
                journal=journal_code,
                date=processing_date,
                ref_piece="",
                account=account_cancellation_fee,
                label=f"{r.code_comptable} - {ota_label} - Frais d'annulation - {r.guest_name} - {r.reference_number}",
                debit=-gross_excl_city_tax if gross_excl_city_tax < 0 else gross_excl_city_tax,
                credit=None,
            ))
        else:
            entries.append(AccountingEntry(
                journal=journal_code,
                date=processing_date,
                ref_piece="",
                account=account_client,
                label=f"{r.code_comptable}- {ota_label} - {r.guest_name} - CO :{checkout_label}",
                debit=None if gross_excl_city_tax >= 0 else -gross_excl_city_tax,
                credit=gross_excl_city_tax if gross_excl_city_tax >= 0 else None,
            ))

    logger.info(
        "Generated %d accounting entries for %d reservation(s)",
        len(entries), len(valid_reservations),
    )
    return entries, valid_reservations, anomalies
