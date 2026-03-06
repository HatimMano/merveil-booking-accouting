"""
CLI entry point for the combined Booking.com + Airbnb → PennyLane pipeline.

Usage examples
--------------
# Booking only
python main.py --booking-dir ./csv_input/booking/ --date 2026-01-31

# Airbnb only
python main.py --airbnb-file ./csv_input/airbnb/2601_Imports_AirBnb.xlsx --date 2026-01-31

# Combined (one output CSV for both OTAs)
python main.py \\
    --booking-dir ./csv_input/booking/ \\
    --airbnb-file ./csv_input/airbnb/2601_Imports_AirBnb.xlsx \\
    --date 2026-01-31

# Dry-run (validate only, no output files written)
python main.py --booking-dir ./csv_input/booking/ --date 2026-01-31 --dry-run
"""

import logging
import sys
from datetime import date, datetime
from pathlib import Path
from typing import List, Optional

import click

sys.path.insert(0, str(Path(__file__).parent))

from config.mapping_loader import load_mapping, load_airbnb_mapping
from config.settings import (
    EXPECTED_APARTMENT_COUNT,
    AIRBNB_JOURNAL_CODE,
    AIRBNB_ACCOUNT_BANK,
    AIRBNB_ACCOUNT_CLIENT,
    AIRBNB_ACCOUNT_SUPPLIER,
)
from parsers.booking import BookingParser
from parsers.airbnb import AirbnbParser
from accounting.entries import generate_entries, AccountingEntry
from accounting.pennylane import export_to_csv
from accounting.reports import export_anomalies_csv, export_verification_matrix
from validators.anomalies import (
    Anomaly,
    Severity,
    check_duplicate_reservations,
    validate_reservation_amounts,
    check_balance,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

@click.command()
@click.option(
    "--booking-dir", "booking_dir",
    default=None,
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    help="Directory containing Booking.com CSV files.",
)
@click.option(
    "--airbnb-file", "airbnb_file",
    default=None,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Airbnb Excel export file (.xlsx).",
)
@click.option(
    "--date", "processing_date_str",
    required=True,
    help="Processing date in YYYY-MM-DD format (e.g. 2026-01-31).",
)
@click.option(
    "--output", "output_path",
    default=None,
    type=click.Path(path_type=Path),
    help="Output PennyLane CSV file path. Default: ./output/pennylane_combined_{date}.csv",
)
@click.option(
    "--booking-mapping", "booking_mapping_path",
    default="config/mapping/CodeAppart_Compta.csv",
    type=click.Path(path_type=Path),
    show_default=True,
    help="Path to the Booking.com mapping CSV.",
)
@click.option(
    "--airbnb-mapping", "airbnb_mapping_path",
    default="config/mapping/AirbnbLogement_Compta.csv",
    type=click.Path(path_type=Path),
    show_default=True,
    help="Path to the Airbnb listing→accounting-code mapping CSV.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Run validation only — no files are written.",
)
def main(
    booking_dir: Optional[Path],
    airbnb_file: Optional[Path],
    processing_date_str: str,
    output_path: Optional[Path],
    booking_mapping_path: Path,
    airbnb_mapping_path: Path,
    dry_run: bool,
) -> None:
    """Transform Booking.com and/or Airbnb exports into a single PennyLane CSV."""

    if not booking_dir and not airbnb_file:
        logger.error("Provide at least --booking-dir or --airbnb-file (or both).")
        sys.exit(1)

    # --- Parse & validate the processing date ---
    try:
        processing_date: date = datetime.strptime(processing_date_str, "%Y-%m-%d").date()
    except ValueError:
        logger.error("Invalid date format '%s'. Expected YYYY-MM-DD.", processing_date_str)
        sys.exit(1)

    # --- Default output path ---
    if output_path is None:
        output_path = Path("output") / f"pennylane_combined_{processing_date_str}.csv"

    logger.info("=" * 60)
    logger.info("Booking + Airbnb → PennyLane Combined Pipeline")
    logger.info("=" * 60)
    logger.info("Processing date  : %s", processing_date.strftime("%d/%m/%Y"))
    logger.info("Output file      : %s", output_path)
    logger.info("Dry run          : %s", dry_run)
    logger.info("-" * 60)

    all_entries: List[AccountingEntry] = []
    all_anomalies: List[Anomaly] = []
    all_processed_reservations = []

    # =======================================================================
    # BOOKING.COM PIPELINE
    # =======================================================================
    if booking_dir:
        logger.info(">>> BOOKING.COM <<<")
        logger.info("Input directory  : %s", booking_dir)

        # Step 1 — Load mapping
        try:
            booking_mapping = load_mapping(booking_mapping_path)
        except FileNotFoundError:
            logger.error("Booking mapping file not found: %s", booking_mapping_path)
            sys.exit(1)

        # Step 2 — Parse CSVs
        booking_parser = BookingParser()
        booking_reservations, booking_anomalies = booking_parser.parse_directory(booking_dir)
        all_anomalies.extend(booking_anomalies)

        # Cross-file duplicate check
        all_anomalies.extend(check_duplicate_reservations(booking_reservations))

        csv_files = list(booking_dir.glob("*.csv"))
        logger.info(
            "Found %d CSV file(s), parsed %d reservation(s)",
            len(csv_files), len(booking_reservations),
        )

        # Step 3 — Generate accounting entries
        booking_entries, booking_processed, entry_anomalies = generate_entries(
            booking_reservations,
            processing_date,
            booking_mapping,
        )
        all_anomalies.extend(entry_anomalies)
        all_entries.extend(booking_entries)
        all_processed_reservations.extend(booking_processed)

        # Step 4 — Per-reservation validation
        for r in booking_processed:
            all_anomalies.extend(validate_reservation_amounts(r))

        # Step 5 — Global balance check for Booking
        b_total_net = sum(r.net for r in booking_processed)
        b_total_amount = sum(r.amount for r in booking_processed)
        b_total_commission = sum(r.commission for r in booking_processed)
        b_total_payment_charge = sum(r.payment_charge for r in booking_processed)
        b_total_city_tax = sum(r.city_tax for r in booking_processed)
        balance_a = check_balance(
            b_total_net, b_total_amount, b_total_commission,
            b_total_payment_charge, b_total_city_tax,
            label="Booking global",
        )
        if balance_a:
            all_anomalies.append(balance_a)
        logger.info(
            "Booking balance OK: %s | %d entries generated",
            balance_a is None, len(booking_entries),
        )
        logger.info("-" * 60)

    # =======================================================================
    # AIRBNB PIPELINE
    # =======================================================================
    if airbnb_file:
        logger.info(">>> AIRBNB <<<")
        logger.info("Input file       : %s", airbnb_file)

        # Step 1 — Load Airbnb mapping
        try:
            airbnb_mapping = load_airbnb_mapping(airbnb_mapping_path)
        except FileNotFoundError:
            logger.error("Airbnb mapping file not found: %s", airbnb_mapping_path)
            sys.exit(1)

        # Step 2 — Parse Excel into payout batches
        airbnb_parser = AirbnbParser()
        airbnb_batches, airbnb_anomalies = airbnb_parser.parse_into_batches(airbnb_file)
        all_anomalies.extend(airbnb_anomalies)

        logger.info(
            "Found %d payout batch(es)",
            len(airbnb_batches),
        )

        # Step 3 — Generate one set of accounting entries per payout batch
        airbnb_processed_all = []
        for batch in airbnb_batches:
            batch_entries, batch_processed, entry_anomalies = generate_entries(
                batch.reservations,
                processing_date,
                airbnb_mapping,
                journal_code=AIRBNB_JOURNAL_CODE,
                account_bank=AIRBNB_ACCOUNT_BANK,
                account_client=AIRBNB_ACCOUNT_CLIENT,
                account_supplier=AIRBNB_ACCOUNT_SUPPLIER,
                ota_label="AIRBNB",
            )
            all_anomalies.extend(entry_anomalies)
            all_entries.extend(batch_entries)
            airbnb_processed_all.extend(batch_processed)
            all_processed_reservations.extend(batch_processed)

        # Step 4 — Per-reservation validation for Airbnb
        for r in airbnb_processed_all:
            all_anomalies.extend(validate_reservation_amounts(r))

        # Step 5 — Global balance check for Airbnb (all batches combined)
        if airbnb_processed_all:
            a_total_net = sum(r.net for r in airbnb_processed_all)
            a_total_amount = sum(r.amount for r in airbnb_processed_all)
            a_total_commission = sum(r.commission for r in airbnb_processed_all)
            a_total_payment_charge = sum(r.payment_charge for r in airbnb_processed_all)
            a_total_city_tax = sum(r.city_tax for r in airbnb_processed_all)
            balance_b = check_balance(
                a_total_net, a_total_amount, a_total_commission,
                a_total_payment_charge, a_total_city_tax,
                label="Airbnb global",
            )
            if balance_b:
                all_anomalies.append(balance_b)
            logger.info(
                "Airbnb balance OK: %s | %d entries generated (%d reservations)",
                balance_b is None,
                sum(1 for e in all_entries if e.journal == AIRBNB_JOURNAL_CODE),
                len(airbnb_processed_all),
            )
        logger.info("-" * 60)

    # =======================================================================
    # SUMMARY
    # =======================================================================
    blocking = [a for a in all_anomalies if a.severity == Severity.BLOCKING]
    warnings  = [a for a in all_anomalies if a.severity == Severity.WARNING]
    infos     = [a for a in all_anomalies if a.severity == Severity.INFO]

    logger.info("Validation summary")
    logger.info("  Total reservations processed : %d", len(all_processed_reservations))
    logger.info("  Total accounting entries     : %d", len(all_entries))
    logger.info("  Blocking anomalies           : %d", len(blocking))
    logger.info("  Warnings                     : %d", len(warnings))
    logger.info("  Info messages                : %d", len(infos))
    logger.info("-" * 60)

    for a in blocking:
        logger.error("[BLOCKING] %s — %s (file: %s)", a.type, a.message, a.source_file)
    for a in warnings:
        logger.warning("[WARNING]  %s — %s (file: %s)", a.type, a.message, a.source_file)

    # =======================================================================
    # EXPORT
    # =======================================================================
    if dry_run:
        logger.info("Dry run — no output files written.")
    elif blocking:
        logger.error(
            "%d blocking anomaly(ies) prevent export. Fix the issues above, then re-run.",
            len(blocking),
        )
        sys.exit(1)
    else:
        date_tag = processing_date_str
        out_dir = output_path.parent
        anomaly_path = out_dir / f"anomalies_{date_tag}.csv"
        matrix_path  = out_dir / f"matrice_verification_{date_tag}.csv"

        export_to_csv(all_entries, output_path)
        logger.info("PennyLane CSV written to         : %s", output_path)

        export_anomalies_csv(all_anomalies, anomaly_path)
        export_verification_matrix(all_processed_reservations, matrix_path)

    logger.info("=" * 60)
    logger.info("Done.")


if __name__ == "__main__":
    main()
