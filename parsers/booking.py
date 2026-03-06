"""Parser for Booking.com CSV exports from the extranet."""

import csv
import logging
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from config.settings import (
    BOOKING_DATE_FORMATS,
    BOOKING_FILENAME_PATTERN,
    SUPPORTED_CURRENCIES,
)
from models.reservation import Reservation
from parsers.base import OTAParser
from validators.anomalies import Anomaly, AnomalyType, Severity

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _parse_booking_date(date_str: str) -> Optional[datetime]:
    """
    Parse a Booking.com date string.

    Accepts both 'Aug 28 2025' and 'Aug 28, 2025'.
    Returns a date object, or None if the string cannot be parsed.
    """
    date_str = date_str.strip()
    for fmt in BOOKING_DATE_FORMATS:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    return None


def _parse_decimal(value: str) -> Optional[Decimal]:
    """
    Parse a decimal value from a CSV cell.

    Returns None if the cell is empty or cannot be parsed as a number.
    """
    value = value.strip()
    if not value:
        return None
    try:
        return Decimal(value)
    except InvalidOperation:
        return None


def _build_col_map(header_row: List[str]) -> Dict[str, int]:
    """
    Build a mapping from column name → CSV column index, skipping empty columns.

    The Booking.com CSV has an unnamed empty column between 'Payment charge'
    and 'Net'. This function silently skips all empty column names.
    """
    col_map: Dict[str, int] = {}
    for idx, col in enumerate(header_row):
        name = col.strip()
        if name:
            col_map[name] = idx
    return col_map


def _get_cell(row: List[str], col_map: Dict[str, int], col_name: str) -> str:
    """Return the cell value for a given column name, or '' if missing."""
    idx = col_map.get(col_name)
    if idx is None or idx >= len(row):
        return ""
    return row[idx].strip()


def _get_cell_aliases(
    row: List[str], col_map: Dict[str, int], *aliases: str
) -> str:
    """
    Try multiple column name aliases in order, return the first non-empty value.

    Booking.com has renamed some columns over time:
      - "City tax" → "Tourism tax"
      - "Payment charge" → "Payments Service Fee"
    This function handles both old and new names transparently.
    """
    for name in aliases:
        val = _get_cell(row, col_map, name)
        if val:
            return val
    return ""


# ---------------------------------------------------------------------------
# BookingParser
# ---------------------------------------------------------------------------

class BookingParser(OTAParser):
    """
    Parser for Booking.com CSV exports downloaded from the extranet.

    Expected filename format:  {ref_appart}-{payout_id}.csv
    Example:                   3015679-7oaOsO2VGKHbvBNQ.csv

    The ref_appart and payout_id come ONLY from the filename — they are not
    present inside the CSV itself.

    The CSV uses comma as separator and has an anonymous empty column between
    'Payment charge' and 'Net'. Only rows with Type='Reservation' are parsed;
    other types (adjustments, etc.) are logged at INFO level and skipped.
    """

    def _extract_filename_parts(
        self, path: Path
    ) -> Tuple[Optional[str], Optional[str], List[Anomaly]]:
        """
        Extract ref_appart and payout_id from the filename.

        Returns (ref_appart, payout_id, anomalies).
        If the filename is invalid, ref_appart and payout_id are None and
        a BLOCKING anomaly is included.
        """
        anomalies: List[Anomaly] = []
        filename = path.name

        if not re.match(BOOKING_FILENAME_PATTERN, filename):
            anomalies.append(Anomaly(
                type=AnomalyType.FILE_BAD_NAME,
                severity=Severity.BLOCKING,
                message=(
                    f"Filename does not match expected pattern "
                    f"'{{digits}}-{{alphanum}}.csv': {filename}"
                ),
                source_file=filename,
                reservation_ref=None,
                details={"filename": filename},
            ))
            return None, None, anomalies

        stem = path.stem   # filename without the .csv extension
        ref_appart, payout_id = stem.split("-", 1)
        return ref_appart, payout_id, anomalies

    def _parse_csv_rows(
        self, path: Path, ref_appart: str, payout_id: str
    ) -> Tuple[List[Reservation], List[Anomaly]]:
        """Parse the data rows of a Booking.com CSV file."""
        reservations: List[Reservation] = []
        anomalies: List[Anomaly] = []
        filename = path.name

        # Read the file, trying UTF-8-sig first (handles BOM), then latin-1
        try:
            with open(path, encoding="utf-8-sig", newline="") as f:
                rows = list(csv.reader(f))
        except UnicodeDecodeError:
            with open(path, encoding="latin-1", newline="") as f:
                rows = list(csv.reader(f))

        # Remove completely blank rows
        rows = [r for r in rows if any(cell.strip() for cell in r)]

        if not rows:
            anomalies.append(Anomaly(
                type=AnomalyType.FILE_EMPTY,
                severity=Severity.WARNING,
                message=f"File is empty or contains only blank rows: {filename}",
                source_file=filename,
                reservation_ref=None,
                details={},
            ))
            return reservations, anomalies

        # First row is the header
        col_map = _build_col_map(rows[0])
        data_rows = rows[1:]

        if not data_rows:
            anomalies.append(Anomaly(
                type=AnomalyType.FILE_EMPTY,
                severity=Severity.WARNING,
                message=f"File has headers but no data rows: {filename}",
                source_file=filename,
                reservation_ref=None,
                details={},
            ))
            return reservations, anomalies

        for row_num, row in enumerate(data_rows, start=2):
            row_type = _get_cell(row, col_map, "Type")
            ref_num = _get_cell(row, col_map, "Reference number")

            if not row_type:
                continue

            if row_type != "Reservation":
                logger.info(
                    "Non-reservation row (Type='%s', ref='%s') in %s — skipped",
                    row_type, ref_num, filename,
                )
                anomalies.append(Anomaly(
                    type=AnomalyType.NON_RESERVATION_TYPE,
                    severity=Severity.INFO,
                    message=(
                        f"Row {row_num} has Type='{row_type}' "
                        f"(not 'Reservation') — skipped"
                    ),
                    source_file=filename,
                    reservation_ref=ref_num or None,
                    details={"type": row_type, "row": row_num},
                ))
                continue

            # --- Parse dates ---
            check_in = _parse_booking_date(_get_cell(row, col_map, "Check-in"))
            checkout = _parse_booking_date(_get_cell(row, col_map, "Checkout"))
            payout_date = _parse_booking_date(_get_cell(row, col_map, "Payout date"))

            if not check_in or not checkout or not payout_date:
                anomalies.append(Anomaly(
                    type=AnomalyType.AMOUNT_MISMATCH,
                    severity=Severity.WARNING,
                    message=(
                        f"Could not parse one or more dates in row {row_num} "
                        f"of {filename} — row skipped"
                    ),
                    source_file=filename,
                    reservation_ref=ref_num or None,
                    details={
                        "check_in_raw": _get_cell(row, col_map, "Check-in"),
                        "checkout_raw": _get_cell(row, col_map, "Checkout"),
                        "payout_date_raw": _get_cell(row, col_map, "Payout date"),
                    },
                ))
                continue

            # --- Parse financial amounts ---
            # Support both old and new Booking.com column names:
            #   "City tax" (old) / "Tourism tax" (new)
            #   "Payment charge" (old) / "Payments Service Fee" (new)
            city_tax = _parse_decimal(
                _get_cell_aliases(row, col_map, "City tax", "Tourism tax")
            ) or Decimal("0")
            amount = _parse_decimal(_get_cell(row, col_map, "Amount")) or Decimal("0")
            commission = _parse_decimal(_get_cell(row, col_map, "Commission")) or Decimal("0")
            payment_charge = _parse_decimal(
                _get_cell_aliases(row, col_map, "Payment charge", "Payments Service Fee")
            ) or Decimal("0")
            net_raw = _parse_decimal(_get_cell(row, col_map, "Net"))
            if net_raw is not None:
                net = net_raw
            else:
                # "Paid Online" reservations omit the Net column entirely.
                # Derive Net from the other fields (Net = Amount + fees + tax).
                net = amount + commission + payment_charge + city_tax

            # --- Validate currency ---
            currency = _get_cell(row, col_map, "Currency")
            if currency not in SUPPORTED_CURRENCIES:
                anomalies.append(Anomaly(
                    type=AnomalyType.NON_EUR_CURRENCY,
                    severity=Severity.BLOCKING,
                    message=(
                        f"Unsupported currency '{currency}' "
                        f"in reservation {ref_num} ({filename})"
                    ),
                    source_file=filename,
                    reservation_ref=ref_num,
                    details={"currency": currency},
                ))
                continue

            # --- Check cancelled reservations with non-zero amount ---
            status = _get_cell(row, col_map, "Reservation status")
            if status != "ok" and amount != Decimal("0"):
                anomalies.append(Anomaly(
                    type=AnomalyType.CANCELLED_WITH_AMOUNT,
                    severity=Severity.WARNING,
                    message=(
                        f"Reservation {ref_num} has status='{status}' "
                        f"but Amount={amount} (expected 0 for non-ok status)"
                    ),
                    source_file=filename,
                    reservation_ref=ref_num,
                    details={"status": status, "amount": str(amount)},
                ))

            reservation = Reservation(
                source_file=filename,
                ref_appart=ref_appart,
                payout_id=payout_id,
                reference_number=ref_num,
                check_in=check_in,
                checkout=checkout,
                guest_name=_get_cell(row, col_map, "Guest name"),
                reservation_status=status,
                currency=currency,
                payment_status=_get_cell(row, col_map, "Payment status"),
                city_tax=city_tax,
                amount=amount,
                commission=commission,
                payment_charge=payment_charge,
                net=net,
                payout_date=payout_date,
            )
            reservations.append(reservation)

        return reservations, anomalies

    def parse_file(self, path: Path) -> Tuple[List[Reservation], List[Anomaly]]:
        """Parse a single Booking.com CSV file."""
        logger.info("Parsing file: %s", path.name)

        ref_appart, payout_id, anomalies = self._extract_filename_parts(path)
        if ref_appart is None:
            # Invalid filename — cannot determine the apartment reference
            return [], anomalies

        reservations, parse_anomalies = self._parse_csv_rows(path, ref_appart, payout_id)
        anomalies.extend(parse_anomalies)

        logger.info(
            "  → %d reservation(s), %d anomaly(ies) in %s",
            len(reservations), len(anomalies), path.name,
        )
        return reservations, anomalies

    def parse_directory(self, path: Path) -> Tuple[List[Reservation], List[Anomaly]]:
        """Parse all CSV files in a directory."""
        all_reservations: List[Reservation] = []
        all_anomalies: List[Anomaly] = []

        csv_files = sorted(path.glob("*.csv"))
        if not csv_files:
            logger.warning("No CSV files found in: %s", path)

        for csv_file in csv_files:
            reservations, anomalies = self.parse_file(csv_file)
            all_reservations.extend(reservations)
            all_anomalies.extend(anomalies)

        logger.info(
            "Parsed %d file(s): %d reservation(s) total, %d anomaly(ies) total",
            len(csv_files), len(all_reservations), len(all_anomalies),
        )
        return all_reservations, all_anomalies
