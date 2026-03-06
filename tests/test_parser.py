"""Tests for the Booking.com CSV parser."""

from decimal import Decimal
from pathlib import Path

import pytest

from parsers.booking import BookingParser, _parse_booking_date, _build_col_map
from validators.anomalies import AnomalyType, Severity

FIXTURES = Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------

class TestParseDateFormats:
    def test_without_comma(self):
        d = _parse_booking_date("Aug 28 2025")
        assert d.year == 2025
        assert d.month == 8
        assert d.day == 28

    def test_with_comma(self):
        d = _parse_booking_date("Aug 28, 2025")
        assert d is not None
        assert d.day == 28

    def test_invalid_returns_none(self):
        assert _parse_booking_date("not-a-date") is None

    def test_strips_whitespace(self):
        d = _parse_booking_date("  Sep 1 2025  ")
        assert d is not None
        assert d.month == 9 and d.day == 1


# ---------------------------------------------------------------------------
# Column map (empty column handling)
# ---------------------------------------------------------------------------

class TestBuildColMap:
    def test_skips_empty_columns(self):
        header = ["Type", "Amount", "", "Net"]
        col_map = _build_col_map(header)
        assert "Type" in col_map
        assert "Amount" in col_map
        assert "Net" in col_map
        assert "" not in col_map

    def test_preserves_indices(self):
        header = ["Type", "Amount", "", "Net"]
        col_map = _build_col_map(header)
        # "Net" should be at index 3 (not 2), because the empty column is skipped in mapping
        # but the original CSV index is kept
        assert col_map["Net"] == 3
        assert col_map["Amount"] == 1


# ---------------------------------------------------------------------------
# Filename parsing
# ---------------------------------------------------------------------------

class TestFilenameValidation:
    def test_bad_filename_produces_blocking_anomaly(self):
        parser = BookingParser()
        path = FIXTURES / "bad_filename.csv"
        reservations, anomalies = parser.parse_file(path)
        assert reservations == []
        assert any(a.type == AnomalyType.FILE_BAD_NAME for a in anomalies)
        assert any(a.severity == Severity.BLOCKING for a in anomalies)

    def test_good_filename_extracts_parts(self):
        parser = BookingParser()
        path = FIXTURES / "3015679-7oaOsO2VGKHbvBNQ.csv"
        reservations, anomalies = parser.parse_file(path)
        assert len(reservations) == 2
        assert reservations[0].ref_appart == "3015679"
        assert reservations[0].payout_id == "7oaOsO2VGKHbvBNQ"


# ---------------------------------------------------------------------------
# Normal CSV parsing
# ---------------------------------------------------------------------------

class TestNormalCSVParsing:
    def setup_method(self):
        self.parser = BookingParser()
        self.reservations, self.anomalies = self.parser.parse_file(
            FIXTURES / "3015679-7oaOsO2VGKHbvBNQ.csv"
        )

    def test_two_reservations_parsed(self):
        assert len(self.reservations) == 2

    def test_first_reservation_fields(self):
        r = self.reservations[0]
        assert r.reference_number == "5493245107"
        assert r.guest_name == "Momin Bashir"
        assert r.currency == "EUR"
        assert r.amount == Decimal("544.60")
        assert r.commission == Decimal("-89.05")
        assert r.payment_charge == Decimal("-7.33")
        assert r.city_tax == Decimal("-20.80")
        assert r.net == Decimal("427.42")

    def test_dates_parsed_correctly(self):
        r = self.reservations[0]
        assert r.check_in.month == 8   # Aug
        assert r.check_in.day == 28
        assert r.checkout.month == 9   # Sep
        assert r.checkout.day == 1
        assert r.payout_date.month == 10  # Oct

    def test_no_blocking_anomalies(self):
        blocking = [a for a in self.anomalies if a.severity == Severity.BLOCKING]
        assert blocking == []


# ---------------------------------------------------------------------------
# Empty file
# ---------------------------------------------------------------------------

class TestEmptyFile:
    def test_empty_file_produces_warning(self):
        parser = BookingParser()
        reservations, anomalies = parser.parse_file(FIXTURES / "8888888-EMPTYFILE.csv")
        assert reservations == []
        assert any(a.type == AnomalyType.FILE_EMPTY for a in anomalies)
        assert any(a.severity == Severity.WARNING for a in anomalies)


# ---------------------------------------------------------------------------
# Non-EUR currency
# ---------------------------------------------------------------------------

class TestNonEurCurrency:
    def test_usd_reservation_is_blocked(self):
        parser = BookingParser()
        reservations, anomalies = parser.parse_file(FIXTURES / "9999999-INVALIDCURRTEST.csv")
        # The reservation is skipped (BLOCKING)
        assert reservations == []
        assert any(a.type == AnomalyType.NON_EUR_CURRENCY for a in anomalies)
        assert any(a.severity == Severity.BLOCKING for a in anomalies)


# ---------------------------------------------------------------------------
# Cancelled reservation with non-zero amount
# ---------------------------------------------------------------------------

class TestCancelledReservation:
    def test_cancelled_with_amount_produces_warning(self):
        parser = BookingParser()
        reservations, anomalies = parser.parse_file(FIXTURES / "3788679-CANCELTEST.csv")
        # The reservation IS included (cancellation with amount is only a WARNING)
        assert len(reservations) == 1
        assert any(a.type == AnomalyType.CANCELLED_WITH_AMOUNT for a in anomalies)
        assert any(a.severity == Severity.WARNING for a in anomalies)


# ---------------------------------------------------------------------------
# Directory parsing
# ---------------------------------------------------------------------------

class TestDirectoryParsing:
    def test_parses_all_valid_files(self):
        parser = BookingParser()
        reservations, anomalies = parser.parse_directory(FIXTURES)
        # bad_filename.csv is blocked, 9999999- has non-EUR (blocked), 8888888- is empty
        # Valid: 3015679- (2 reservations), 3788679- (1 reservation)
        assert len(reservations) >= 3
