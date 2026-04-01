"""Parser for Booking.com exports (CSV legacy + Excel weekly)."""

import csv
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from config.settings import (
    BOOKING_DATE_FORMATS,
    BOOKING_FILENAME_PATTERN,
    SUPPORTED_CURRENCIES,
)

try:
    import openpyxl
except ImportError:
    openpyxl = None  # type: ignore
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


# ---------------------------------------------------------------------------
# BookingPayoutBatch — data model for the Excel parser
# ---------------------------------------------------------------------------

@dataclass
class BookingPayoutBatch:
    """A group of Booking.com reservations sharing the same payout identifier."""
    payout_id: str
    payout_date: date
    reservations: List[Reservation]


# ---------------------------------------------------------------------------
# Helper for Excel cell values
# ---------------------------------------------------------------------------

def _cell_to_decimal(value) -> Optional[Decimal]:
    """Convert an openpyxl cell value (float, int, str, None) to Decimal."""
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


# ---------------------------------------------------------------------------
# BookingExcelParser
# ---------------------------------------------------------------------------

class BookingExcelParser(OTAParser):
    """
    Parser for Booking.com weekly Excel exports.

    File format (one sheet, header on row 0):
      Col 0:  Ref Appart         — numeric apartment ID (float in Excel)
      Col 1:  Type               — "Reservation" or "Commission adjustment"
      Col 2:  Numéro de référence
      Col 3:  Date de départ     — checkout date ("Mar 19, 2026")
      Col 4:  Nom du client
      Col 5:  Statut             — "ok" / "cancelled"
      Col 6:  Devise             — "EUR"
      Col 7:  Statut du paiement — "by_booking" / "Paid Online"
      Col 8:  Montant            — gross amount (positive)
      Col 9:  Commission         — negative
      Col 10: Coûts de transaction (payment_charge) — negative
      Col 11: Taxe de séjour (city_tax) — negative
      Col 12: (empty)
      Col 13: Net
      Col 14: Date du paiement
      Col 15: Identifiant du paiement (payout_id)

    Rows are grouped by payout_id to form payout batches.
    """

    SUPPORTED_TYPES = {"Reservation", "Commission adjustment"}

    def parse_file(self, path: Path) -> Tuple[List[Reservation], List[Anomaly]]:
        batches, anomalies = self.parse_into_batches(path)
        return [r for b in batches for r in b.reservations], anomalies

    def parse_directory(self, path: Path) -> Tuple[List[Reservation], List[Anomaly]]:
        xlsx_files = sorted(path.glob("*.xlsx"))
        if not xlsx_files:
            return [], []
        return self.parse_file(xlsx_files[0])

    def parse_into_batches(
        self, path: Path
    ) -> Tuple[List[BookingPayoutBatch], List[Anomaly]]:
        if openpyxl is None:
            raise ImportError(
                "openpyxl is required to parse Booking Excel files. "
                "Install it with: pip install openpyxl"
            )

        anomalies: List[Anomaly] = []
        filename = path.name

        try:
            wb = openpyxl.load_workbook(path, data_only=True)
        except Exception as exc:
            anomalies.append(Anomaly(
                type=AnomalyType.FILE_BAD_NAME,
                severity=Severity.BLOCKING,
                message=f"Cannot open Booking Excel file '{filename}': {exc}",
                source_file=filename,
                reservation_ref=None,
                details={"error": str(exc)},
            ))
            return [], anomalies

        ws = wb.active
        all_rows = list(ws.iter_rows(values_only=True))
        if len(all_rows) < 2:
            anomalies.append(Anomaly(
                type=AnomalyType.FILE_EMPTY,
                severity=Severity.WARNING,
                message=f"Booking Excel file has no data rows: {filename}",
                source_file=filename,
                reservation_ref=None,
                details={},
            ))
            return [], anomalies

        data_rows = all_rows[1:]  # skip header

        # Group reservations by payout_id (col 15), preserving row order
        batches_map: Dict[str, List[Reservation]] = {}
        payout_dates: Dict[str, Optional[date]] = {}

        for row_num, row in enumerate(data_rows, start=2):
            if all(c is None for c in row):
                continue

            row_type = row[1]
            if not row_type or row_type not in self.SUPPORTED_TYPES:
                if row_type:
                    logger.debug("Row %d: ignoring type '%s'", row_num, row_type)
                continue

            payout_id = str(row[15] or "").strip()
            if not payout_id:
                continue

            if payout_id not in payout_dates:
                pd_raw = row[14]
                payout_dates[payout_id] = _parse_booking_date(str(pd_raw)) if pd_raw else None

            payout_date = payout_dates[payout_id]
            if payout_date is None:
                anomalies.append(Anomaly(
                    type=AnomalyType.AMOUNT_MISMATCH,
                    severity=Severity.WARNING,
                    message=f"Row {row_num}: cannot parse payout date '{row[14]}' — row skipped",
                    source_file=filename,
                    reservation_ref=str(row[2] or ""),
                    details={"row": row_num},
                ))
                continue

            res, res_anomalies = self._parse_row(row, row_num, filename, payout_id, payout_date)
            anomalies.extend(res_anomalies)
            if res is not None:
                if payout_id not in batches_map:
                    batches_map[payout_id] = []
                batches_map[payout_id].append(res)

        batches = [
            BookingPayoutBatch(
                payout_id=pid,
                payout_date=payout_dates[pid],
                reservations=reservations,
            )
            for pid, reservations in batches_map.items()
        ]

        logger.info(
            "Parsed Booking Excel '%s': %d payout batch(es), %d reservation(s), %d anomaly(ies)",
            filename,
            len(batches),
            sum(len(b.reservations) for b in batches),
            len(anomalies),
        )
        return batches, anomalies

    def _parse_row(
        self,
        row: tuple,
        row_num: int,
        filename: str,
        payout_id: str,
        payout_date: date,
    ) -> Tuple[Optional[Reservation], List[Anomaly]]:
        anomalies: List[Anomaly] = []
        row_type = str(row[1] or "")

        # Ref Appart: float in Excel (e.g. 6698991.0) → "6698991"
        if row[0] is None:
            anomalies.append(Anomaly(
                type=AnomalyType.MAPPING_NOT_FOUND,
                severity=Severity.BLOCKING,
                message=f"Row {row_num}: empty Ref Appart — row skipped",
                source_file=filename,
                reservation_ref=None,
                details={"row": row_num},
            ))
            return None, anomalies
        ref_appart = str(int(float(row[0])))
        ref_num = str(int(float(row[2]))) if row[2] is not None else ""

        # Financial amounts
        amount         = _cell_to_decimal(row[8])  or Decimal("0")
        commission     = _cell_to_decimal(row[9])  or Decimal("0")
        payment_charge = _cell_to_decimal(row[10]) or Decimal("0")
        city_tax       = _cell_to_decimal(row[11]) or Decimal("0")
        net_raw        = _cell_to_decimal(row[13])

        if row_type == "Commission adjustment":
            net            = net_raw or Decimal("0")
            amount         = net
            commission     = Decimal("0")
            payment_charge = Decimal("0")
            city_tax       = Decimal("0")
            guest_name     = "Ajustement commission"
            status         = "ok"
            currency       = "EUR"
            payment_status = "by_booking"
        else:
            net            = net_raw if net_raw is not None else amount + commission + payment_charge + city_tax
            guest_name     = str(row[4] or "").strip()
            status         = str(row[5] or "ok").strip()
            currency       = str(row[6] or "EUR").strip()
            payment_status = str(row[7] or "by_booking").strip()

        # Currency check
        if currency not in SUPPORTED_CURRENCIES:
            anomalies.append(Anomaly(
                type=AnomalyType.NON_EUR_CURRENCY,
                severity=Severity.WARNING,
                message=f"Row {row_num}: devise non-EUR '{currency}' pour '{ref_num}' — ligne exclue",
                source_file=filename,
                reservation_ref=ref_num or None,
                details={"currency": currency, "montant": str(amount)},
            ))
            return None, anomalies

        # Checkout date (col 3) — check_in is not available in this export
        checkout_raw = row[3]
        checkout = _parse_booking_date(str(checkout_raw)) if checkout_raw else payout_date
        check_in = checkout

        if status != "ok" and amount != Decimal("0"):
            anomalies.append(Anomaly(
                type=AnomalyType.CANCELLED_WITH_AMOUNT,
                severity=Severity.WARNING,
                message=f"Reservation {ref_num} has status='{status}' but Amount={amount}",
                source_file=filename,
                reservation_ref=ref_num,
                details={"status": status, "amount": str(amount)},
            ))

        return Reservation(
            source_file=filename,
            ref_appart=ref_appart,
            payout_id=payout_id,
            reference_number=ref_num,
            check_in=check_in,
            checkout=checkout,
            guest_name=guest_name or "(inconnu)",
            reservation_status=status,
            currency=currency,
            payment_status=payment_status,
            city_tax=city_tax,
            amount=amount,
            commission=commission,
            payment_charge=payment_charge,
            net=net,
            payout_date=payout_date,
        ), anomalies
