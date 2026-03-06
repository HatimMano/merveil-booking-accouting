"""Tests for accounting entry generation and PennyLane export."""

import csv
import tempfile
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from accounting.entries import generate_entries, AccountingEntry
from accounting.pennylane import export_to_csv
from models.reservation import Reservation


def make_reservation(**kwargs) -> Reservation:
    """Create a Reservation with sensible defaults for testing."""
    defaults = dict(
        source_file="3015679-TEST.csv",
        ref_appart="3015679",
        payout_id="TESTPAYOUTID",
        reference_number="5493245107",
        check_in=date(2025, 8, 28),
        checkout=date(2025, 9, 1),
        guest_name="Test Guest",
        reservation_status="ok",
        currency="EUR",
        payment_status="by_booking",
        city_tax=Decimal("-20.80"),
        amount=Decimal("544.60"),
        commission=Decimal("-89.05"),
        payment_charge=Decimal("-7.33"),
        net=Decimal("427.42"),
        payout_date=date(2025, 10, 1),
    )
    defaults.update(kwargs)
    return Reservation(**defaults)


MAPPING = {"3015679": "MER21-0G"}
PROC_DATE = date(2025, 10, 3)


# ---------------------------------------------------------------------------
# Entry generation
# ---------------------------------------------------------------------------

class TestGenerateEntries:
    def test_correct_number_of_entries(self):
        r = make_reservation()
        entries, processed, anomalies = generate_entries([r], PROC_DATE, MAPPING)
        # 2 header lines + 2 per reservation
        assert len(entries) == 4

    def test_header_line1_bank_debit(self):
        r = make_reservation()
        entries, _, _ = generate_entries([r], PROC_DATE, MAPPING)
        header1 = entries[0]
        assert header1.account == "51105000"
        assert header1.debit == Decimal("427.42")
        assert header1.credit is None

    def test_header_line2_supplier_credit(self):
        r = make_reservation()
        entries, _, _ = generate_entries([r], PROC_DATE, MAPPING)
        header2 = entries[1]
        assert header2.account == "401BOOKING"
        assert header2.credit == Decimal("89.05") + Decimal("7.33")
        assert header2.debit is None

    def test_reservation_entry1_client_debit(self):
        r = make_reservation()
        entries, _, _ = generate_entries([r], PROC_DATE, MAPPING)
        entry1 = entries[2]
        assert entry1.account == "411BOOKING"
        assert entry1.debit == Decimal("544.60")
        assert entry1.credit is None

    def test_reservation_entry2_supplier_credit(self):
        r = make_reservation()
        entries, _, _ = generate_entries([r], PROC_DATE, MAPPING)
        entry2 = entries[3]
        assert entry2.account == "401BOOKING"
        assert entry2.credit == Decimal("89.05") + Decimal("7.33")
        assert entry2.debit is None

    def test_entry_labels_contain_expected_parts(self):
        r = make_reservation()
        entries, _, _ = generate_entries([r], PROC_DATE, MAPPING)
        label1 = entries[2].label  # client entry
        assert "MER21-0G" in label1
        assert "BOOKING" in label1
        assert "Test Guest" in label1
        assert "01/09/2025" in label1  # checkout date

        label2 = entries[3].label  # fee entry
        assert "MER21-0G" in label2
        assert "3015679" in label2
        assert "FEE BOOKING" in label2

    def test_mapping_not_found_blocks_reservation(self):
        r = make_reservation(ref_appart="UNKNOWN")
        entries, processed, anomalies = generate_entries([r], PROC_DATE, MAPPING)
        assert processed == []
        assert entries == []
        assert any(a.type == "MAPPING_NOT_FOUND" for a in anomalies)

    def test_processing_date_formatted_correctly(self):
        r = make_reservation()
        entries, _, _ = generate_entries([r], PROC_DATE, MAPPING)
        # 03/10/2025
        assert entries[0].date == "03/10/2025"

    def test_payout_date_in_header_label(self):
        r = make_reservation()
        entries, _, _ = generate_entries([r], PROC_DATE, MAPPING)
        assert "01/10/2025" in entries[0].label
        assert "01/10/2025" in entries[1].label

    def test_total_net_aggregated_over_multiple_reservations(self):
        r1 = make_reservation(net=Decimal("427.42"))
        r2 = make_reservation(
            reference_number="5500000001",
            net=Decimal("252.55"),
            amount=Decimal("320.00"),
            commission=Decimal("-52.48"),
            payment_charge=Decimal("-4.57"),
        )
        entries, _, _ = generate_entries([r1, r2], PROC_DATE, MAPPING)
        header1 = entries[0]
        assert header1.debit == Decimal("427.42") + Decimal("252.55")

    def test_code_comptable_set_on_reservation(self):
        r = make_reservation()
        _, processed, _ = generate_entries([r], PROC_DATE, MAPPING)
        assert processed[0].code_comptable == "MER21-0G"


# ---------------------------------------------------------------------------
# PennyLane CSV export
# ---------------------------------------------------------------------------

class TestPennylaneExport:
    def test_csv_has_correct_headers(self):
        r = make_reservation()
        entries, _, _ = generate_entries([r], PROC_DATE, MAPPING)

        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as f:
            out = Path(f.name)

        export_to_csv(entries, out)
        with open(out, encoding="utf-8") as f:
            reader = csv.reader(f)
            headers = next(reader)

        assert headers == ["Journal", "Date", "Réf. pièce", "Compte", "Libellé", "Débit", "Crédit"]
        out.unlink()

    def test_csv_row_count_matches_entries(self):
        r = make_reservation()
        entries, _, _ = generate_entries([r], PROC_DATE, MAPPING)

        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as f:
            out = Path(f.name)

        export_to_csv(entries, out)
        with open(out, encoding="utf-8") as f:
            rows = list(csv.reader(f))

        # 1 header row + len(entries) data rows
        assert len(rows) == 1 + len(entries)
        out.unlink()

    def test_debit_column_empty_when_none(self):
        r = make_reservation()
        entries, _, _ = generate_entries([r], PROC_DATE, MAPPING)

        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as f:
            out = Path(f.name)

        export_to_csv(entries, out)
        with open(out, encoding="utf-8") as f:
            rows = list(csv.reader(f))

        # Header 2 (401BOOKING credit) should have empty Débit
        header2_row = rows[2]  # index 0=header, 1=header1, 2=header2
        debit_col = header2_row[5]
        assert debit_col == ""
        out.unlink()

    def test_amounts_formatted_with_two_decimals(self):
        r = make_reservation()
        entries, _, _ = generate_entries([r], PROC_DATE, MAPPING)

        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as f:
            out = Path(f.name)

        export_to_csv(entries, out)
        with open(out, encoding="utf-8") as f:
            rows = list(csv.reader(f))

        # Check first data row (header1) debit
        debit = rows[1][5]
        assert "." in debit
        assert len(debit.split(".")[1]) == 2
        out.unlink()
