"""Parser for Airbnb Excel exports (transactions / payout history)."""

import logging
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from config.settings import AIRBNB_RESERVATION_TYPES, SUPPORTED_CURRENCIES
from models.reservation import Reservation
from parsers.base import OTAParser
from validators.anomalies import Anomaly, AnomalyType, Severity

logger = logging.getLogger(__name__)

try:
    import openpyxl
except ImportError:
    openpyxl = None  # type: ignore


# ---------------------------------------------------------------------------
# Internal data model for one Airbnb payout batch
# ---------------------------------------------------------------------------

@dataclass
class AirbnbPayoutBatch:
    """A group of Airbnb reservations sharing the same bank payout."""
    payout_date: date
    payout_reference: str        # Code de référence on the Payout row
    payout_amount: Decimal       # Versé — actual bank transfer amount
    reservations: List[Reservation]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_date(value) -> Optional[date]:
    """Convert a cell value (datetime or date) to a date object."""
    if value is None:
        return None
    if hasattr(value, "date"):
        return value.date()
    if isinstance(value, date):
        return value
    return None


def _to_decimal(value) -> Optional[Decimal]:
    """Convert a cell value (float, int, str, None) to Decimal.
    Handles French decimal format (comma separator, e.g. '-905,72')."""
    if value is None:
        return None
    try:
        return Decimal(str(value).replace(",", ".").replace("\xa0", "").replace(" ", ""))
    except (InvalidOperation, ValueError):
        return None


# ---------------------------------------------------------------------------
# AirbnbParser
# ---------------------------------------------------------------------------

class AirbnbParser(OTAParser):
    """
    Parser for Airbnb transaction Excel exports (.xlsx).

    The Excel file contains one sheet with columns:
        Date | Type | Code de confirmation | Date de début | Nuits | Voyageur |
        Logement | Détails | Code de référence | Devise | Montant | Versé |
        Frais de service | Frais de ménage | Année des revenus

    Structure:
        - "Payout" rows mark the start of a new payout batch (Versé = bank amount)
        - "Réservation", "Régularisation de la résolution", "Hors réservation" rows
          are accounting lines that belong to the preceding Payout row.

    Financial mapping to the Reservation model:
        net              = Montant
        commission       = −Frais de service  (stored negative, like Booking)
        amount           = Montant + Frais de service  (gross before Airbnb fee)
        payment_charge   = 0
        city_tax         = 0
        payout_date      = Payout row's Date
        ref_appart       = Logement name  (mapped via AirbnbLogement_Compta.csv)
    """

    # -----------------------------------------------------------------
    # OTAParser interface
    # -----------------------------------------------------------------

    def parse_file(self, path: Path) -> Tuple[List[Reservation], List[Anomaly]]:
        """Parse a single Airbnb Excel file; return flat list of reservations."""
        batches, anomalies = self.parse_into_batches(path)
        reservations = [r for batch in batches for r in batch.reservations]
        return reservations, anomalies

    def parse_directory(self, path: Path) -> Tuple[List[Reservation], List[Anomaly]]:
        """Parse the first Airbnb .xlsx file found in *path*."""
        xlsx_files = sorted(path.glob("*.xlsx"))
        if not xlsx_files:
            logger.warning("No .xlsx files found in: %s", path)
            return [], []
        if len(xlsx_files) > 1:
            logger.warning(
                "Multiple .xlsx files found in %s — using the first: %s",
                path, xlsx_files[0].name,
            )
        return self.parse_file(xlsx_files[0])

    # -----------------------------------------------------------------
    # Main parsing logic
    # -----------------------------------------------------------------

    def parse_into_batches(
        self, path: Path
    ) -> Tuple[List[AirbnbPayoutBatch], List[Anomaly]]:
        """
        Parse the Excel file and group rows into payout batches.

        Each "Payout" row begins a new batch; the rows that follow (until the
        next Payout) are the reservations that compose that batch.

        Returns:
            (batches, anomalies)
        """
        if openpyxl is None:
            raise ImportError(
                "openpyxl is required to parse Airbnb Excel files. "
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
                message=f"Cannot open Airbnb Excel file '{filename}': {exc}",
                source_file=filename,
                reservation_ref=None,
                details={"error": str(exc)},
            ))
            return [], anomalies

        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))

        if not rows:
            anomalies.append(Anomaly(
                type=AnomalyType.FILE_EMPTY,
                severity=Severity.WARNING,
                message=f"Airbnb Excel file is empty: {filename}",
                source_file=filename,
                reservation_ref=None,
                details={},
            ))
            return [], anomalies

        # --- Locate the header row (first row whose second cell == "Type") ---
        header_row_idx = None
        for i, row in enumerate(rows):
            if row[1] == "Type":
                header_row_idx = i
                break

        if header_row_idx is None:
            anomalies.append(Anomaly(
                type=AnomalyType.FILE_BAD_NAME,
                severity=Severity.BLOCKING,
                message=f"Cannot find header row in Airbnb Excel file: {filename}",
                source_file=filename,
                reservation_ref=None,
                details={},
            ))
            return [], anomalies

        data_rows = rows[header_row_idx + 1 :]

        # --- Group rows into payout batches ---
        batches: List[AirbnbPayoutBatch] = []
        current_payout_date: Optional[date] = None
        current_payout_ref: str = ""
        current_payout_amount: Decimal = Decimal("0")
        pending_reservations: List[Reservation] = []

        for row_num, row in enumerate(data_rows, start=header_row_idx + 2):
            # Skip completely blank rows
            if all(cell is None for cell in row):
                continue

            row_type: Optional[str] = row[1]
            if not row_type:
                continue

            if row_type == "Payout":
                # --- Flush any pending batch first ---
                if current_payout_date is not None and pending_reservations:
                    batch = self._make_batch(
                        current_payout_date,
                        current_payout_ref,
                        current_payout_amount,
                        pending_reservations,
                        filename,
                        anomalies,
                    )
                    batches.append(batch)

                # --- Start a new batch ---
                current_payout_date = _to_date(row[0])
                current_payout_ref = str(row[8] or "")
                current_payout_amount = _to_decimal(row[11]) or Decimal("0")
                pending_reservations = []

            elif row_type in AIRBNB_RESERVATION_TYPES:
                if current_payout_date is None:
                    # Reservation before any Payout row — assign a dummy payout context
                    logger.warning(
                        "Row %d: reservation '%s' found before any Payout row — skipped",
                        row_num, row[2],
                    )
                    continue

                res, res_anomalies = self._parse_reservation_row(
                    row, row_num, filename, current_payout_date, current_payout_ref
                )
                anomalies.extend(res_anomalies)
                if res is not None:
                    pending_reservations.append(res)

            else:
                logger.debug(
                    "Row %d: ignoring row type '%s'", row_num, row_type
                )

        # --- Flush the last batch ---
        if current_payout_date is not None and pending_reservations:
            batch = self._make_batch(
                current_payout_date,
                current_payout_ref,
                current_payout_amount,
                pending_reservations,
                filename,
                anomalies,
            )
            batches.append(batch)

        logger.info(
            "Parsed Airbnb file '%s': %d payout batch(es), %d reservation(s), "
            "%d anomaly(ies)",
            filename,
            len(batches),
            sum(len(b.reservations) for b in batches),
            len(anomalies),
        )
        return batches, anomalies

    # -----------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------

    def _make_batch(
        self,
        payout_date: date,
        payout_ref: str,
        payout_amount: Decimal,
        reservations: List[Reservation],
        filename: str,
        anomalies: List[Anomaly],
    ) -> AirbnbPayoutBatch:
        """Create a batch and verify that Sum(Net) matches the declared Versé."""
        total_net = sum(r.net for r in reservations)
        diff = abs(total_net - payout_amount)
        if diff > Decimal("0.10"):
            anomalies.append(Anomaly(
                type=AnomalyType.BALANCE_ERROR,
                severity=Severity.WARNING,
                message=(
                    f"Airbnb payout {payout_ref} ({payout_date}): "
                    f"Sum(Net)={total_net:.2f} ≠ Versé={payout_amount:.2f} "
                    f"(diff={diff:.2f}€)"
                ),
                source_file=filename,
                reservation_ref=None,
                details={
                    "payout_reference": payout_ref,
                    "payout_amount": str(payout_amount),
                    "sum_net": str(total_net),
                    "difference": str(diff),
                },
            ))
        return AirbnbPayoutBatch(
            payout_date=payout_date,
            payout_reference=payout_ref,
            payout_amount=payout_amount,
            reservations=reservations,
        )

    def _parse_reservation_row(
        self,
        row: tuple,
        row_num: int,
        filename: str,
        payout_date: date,
        payout_ref: str,
    ) -> Tuple[Optional[Reservation], List[Anomaly]]:
        """
        Parse a single Airbnb reservation / adjustment row into a Reservation object.

        Column order (0-based):
          0=Date, 1=Type, 2=Code de confirmation, 3=Date de début, 4=Nuits,
          5=Voyageur, 6=Logement, 7=Détails, 8=Code de référence, 9=Devise,
          10=Montant, 11=Versé, 12=Frais de service, 13=Frais de ménage,
          14=Année des revenus
        """
        anomalies: List[Anomaly] = []

        row_type    = row[1] or ""
        conf_code   = str(row[2] or "").strip()
        check_in_dt = _to_date(row[3])
        nuits_raw   = row[4]
        voyageur    = str(row[5] or "").strip()
        logement    = str(row[6] or "").strip()
        currency    = str(row[9] or "").strip()
        montant_raw = _to_decimal(row[10])
        frais_raw   = _to_decimal(row[12])

        # --- Validate ---
        if not logement:
            anomalies.append(Anomaly(
                type=AnomalyType.MAPPING_NOT_FOUND,
                severity=Severity.BLOCKING,
                message=f"Row {row_num}: empty Logement field — row skipped",
                source_file=filename,
                reservation_ref=conf_code or None,
                details={"row": row_num},
            ))
            return None, anomalies

        if montant_raw is None:
            anomalies.append(Anomaly(
                type=AnomalyType.AMOUNT_MISMATCH,
                severity=Severity.WARNING,
                message=f"Row {row_num}: empty Montant for '{conf_code}' — row skipped",
                source_file=filename,
                reservation_ref=conf_code or None,
                details={"row": row_num},
            ))
            return None, anomalies

        if currency not in SUPPORTED_CURRENCIES:
            nuits_for_label = int(nuits_raw) if nuits_raw is not None else 0
            checkout_str = ""
            if check_in_dt is not None:
                checkout_str = (check_in_dt + timedelta(days=max(nuits_for_label, 1))).strftime("%d/%m/%Y")
            anomalies.append(Anomaly(
                type=AnomalyType.NON_EUR_CURRENCY,
                severity=Severity.WARNING,
                message=(
                    f"Row {row_num}: devise non-EUR '{currency}' "
                    f"pour la réservation '{conf_code}' — ligne exclue, correction manuelle requise"
                ),
                source_file=filename,
                reservation_ref=conf_code or None,
                details={
                    "currency": currency,
                    "montant": str(montant_raw),
                    "logement": logement,
                    "voyageur": voyageur,
                    "checkout_date": checkout_str,
                    "row_type": row_type,
                },
            ))
            return None, anomalies

        if check_in_dt is None:
            # Use payout_date as fallback for check_in when date is missing
            check_in_dt = payout_date
            logger.debug(
                "Row %d: missing Date de début for '%s' — using payout date %s",
                row_num, conf_code, payout_date,
            )

        # Compute checkout from Nuits
        nuits = int(nuits_raw) if nuits_raw is not None else 0
        checkout_dt = check_in_dt + timedelta(days=max(nuits, 1))

        # Financial amounts
        frais_service = frais_raw if frais_raw is not None else Decimal("0")
        net           = montant_raw
        commission    = -frais_service          # stored negative, like Booking
        amount        = net - commission        # = Montant + Frais de service (gross)
        payment_charge = Decimal("0")
        city_tax       = Decimal("0")

        reservation = Reservation(
            source_file=filename,
            ref_appart=logement,               # Airbnb listing name → looked up in mapping
            payout_id=payout_ref,
            reference_number=conf_code,
            check_in=check_in_dt,
            checkout=checkout_dt,
            guest_name=voyageur or "(inconnu)",
            reservation_status="ok" if row_type == "Réservation" else row_type,
            currency=currency,
            payment_status="by_airbnb",
            city_tax=city_tax,
            amount=amount,
            commission=commission,
            payment_charge=payment_charge,
            net=net,
            payout_date=payout_date,
        )
        return reservation, anomalies
