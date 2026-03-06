"""Write validation reports to Google Sheets using gspread."""

import logging
from datetime import date
from decimal import Decimal
from typing import List

from validators.anomalies import Anomaly, Severity

logger = logging.getLogger(__name__)

# OAuth scopes required by gspread
_SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

# Worksheet names
SHEET_SUMMARY = "Résumé"
SHEET_DETAIL = "Détail"
SHEET_ANOMALIES = "Anomalies"

# Lazy imports — gspread and google-auth are optional dependencies
try:
    import gspread
    from google.oauth2.service_account import Credentials as _Credentials
    _GSPREAD_AVAILABLE = True
except ImportError:
    _GSPREAD_AVAILABLE = False
    logger.warning(
        "gspread / google-auth not installed — Google Sheets reporting is disabled. "
        "Install with: pip install gspread google-auth"
    )


class GoogleSheetsReporter:
    """
    Writes the three-tab validation report to a Google Sheets document.

    The spreadsheet must already exist and be shared with the service account.
    On each run the reporter clears and rewrites the three worksheets:
      - 'Résumé'    → key metrics
      - 'Détail'    → one row per reservation
      - 'Anomalies' → one row per anomaly

    Args:
        sheet_id:             Google Sheets document ID (from the URL).
        service_account_file: Path to the GCP service account JSON key file.
    """

    def __init__(self, sheet_id: str, service_account_file: str) -> None:
        if not _GSPREAD_AVAILABLE:
            raise RuntimeError(
                "gspread and google-auth are required for Google Sheets reporting. "
                "Install them with:  pip install gspread google-auth"
            )
        creds = _Credentials.from_service_account_file(service_account_file, scopes=_SCOPES)
        client = gspread.authorize(creds)
        self.spreadsheet = client.open_by_key(sheet_id)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def write_report(
        self,
        processing_date: date,
        reservations: list,
        anomalies: List[Anomaly],
        total_files_expected: int,
        total_files_processed: int,
        balance_ok: bool,
    ) -> None:
        """
        Write the full validation report.

        Args:
            processing_date:       The --date argument passed to the CLI.
            reservations:          Successfully processed Reservation objects.
            anomalies:             All anomalies detected during the run.
            total_files_expected:  Expected number of CSV files (from settings).
            total_files_processed: Actual number of CSV files found in the input dir.
            balance_ok:            Whether the global balance check passed.
        """
        self._write_summary(
            processing_date, reservations, anomalies,
            total_files_expected, total_files_processed, balance_ok,
        )
        self._write_detail(reservations)
        self._write_anomalies(anomalies)
        logger.info("Google Sheets report written (spreadsheet ID: %s)", self.spreadsheet.id)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _get_or_create_sheet(self, name: str):
        """Return an existing worksheet or create it."""
        try:
            return self.spreadsheet.worksheet(name)
        except gspread.WorksheetNotFound:
            return self.spreadsheet.add_worksheet(title=name, rows=2000, cols=20)

    def _write_summary(
        self,
        processing_date: date,
        reservations: list,
        anomalies: List[Anomaly],
        total_files_expected: int,
        total_files_processed: int,
        balance_ok: bool,
    ) -> None:
        ws = self._get_or_create_sheet(SHEET_SUMMARY)
        ws.clear()

        total_net = sum(r.net for r in reservations)
        total_fees = sum(abs(r.commission) + abs(r.payment_charge) for r in reservations)
        total_amount = sum(r.amount for r in reservations)
        blocking_count = sum(1 for a in anomalies if a.severity == Severity.BLOCKING)
        warning_count = sum(1 for a in anomalies if a.severity == Severity.WARNING)

        def _fmt_eur(value: Decimal) -> str:
            """Format a Decimal as a readable euro amount."""
            return f"{value:,.2f} €".replace(",", "\u00a0")  # non-breaking space

        rows = [
            ["Métrique", "Valeur"],
            ["Date traitement", processing_date.strftime("%d/%m/%Y")],
            ["Fichiers traités", f"{total_files_processed} / {total_files_expected} attendus"],
            ["Réservations traitées", len(reservations)],
            ["Total encaissements (Net)", _fmt_eur(total_net)],
            ["Total frais (Commission + Payment charge)", _fmt_eur(total_fees)],
            ["Total revenus (Amount)", _fmt_eur(total_amount)],
            ["Anomalies bloquantes", blocking_count],
            ["Warnings", warning_count],
            ["Équilibre débit/crédit", "✅ OK" if balance_ok else "❌ ERREUR"],
        ]

        ws.update("A1", rows)

    def _write_detail(self, reservations: list) -> None:
        ws = self._get_or_create_sheet(SHEET_DETAIL)
        ws.clear()

        headers = [
            "Code Appart", "Code Comptable", "Ref Booking", "Guest Name",
            "Check-in", "Checkout", "Amount", "Commission", "Payment Charge",
            "City Tax", "Net", "Payout Date", "Payout ID",
        ]
        rows = [headers]

        for r in reservations:
            rows.append([
                r.ref_appart,
                r.code_comptable or "",
                r.reference_number,
                r.guest_name,
                r.check_in.strftime("%d/%m/%Y"),
                r.checkout.strftime("%d/%m/%Y"),
                float(r.amount),
                float(r.commission),
                float(r.payment_charge),
                float(r.city_tax),
                float(r.net),
                r.payout_date.strftime("%d/%m/%Y"),
                r.payout_id,
            ])

        ws.update("A1", rows)

    def _write_anomalies(self, anomalies: List[Anomaly]) -> None:
        ws = self._get_or_create_sheet(SHEET_ANOMALIES)
        ws.clear()

        headers = ["Type", "Sévérité", "Fichier source", "Ref réservation", "Message", "Détails"]
        rows = [headers]

        for a in anomalies:
            rows.append([
                a.type,
                a.severity,
                a.source_file,
                a.reservation_ref or "",
                a.message,
                str(a.details),
            ])

        ws.update("A1", rows)
