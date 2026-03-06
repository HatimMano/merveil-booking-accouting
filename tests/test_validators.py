"""Tests for anomaly detection rules."""

from decimal import Decimal

import pytest

from models.reservation import Reservation
from validators.anomalies import (
    AnomalyType,
    Severity,
    check_balance,
    check_duplicate_reservations,
    validate_reservation_amounts,
)
from datetime import date


def make_reservation(**kwargs) -> Reservation:
    defaults = dict(
        source_file="3015679-TEST.csv",
        ref_appart="3015679",
        payout_id="TESTID",
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
        code_comptable="MER21-0G",
    )
    defaults.update(kwargs)
    return Reservation(**defaults)


# ---------------------------------------------------------------------------
# Duplicate reservation check
# ---------------------------------------------------------------------------

class TestDuplicateReservations:
    def test_no_duplicates_returns_empty(self):
        r1 = make_reservation(reference_number="AAA")
        r2 = make_reservation(reference_number="BBB")
        assert check_duplicate_reservations([r1, r2]) == []

    def test_duplicate_reference_detected(self):
        r1 = make_reservation(reference_number="AAA", source_file="file1.csv")
        r2 = make_reservation(reference_number="AAA", source_file="file2.csv")
        anomalies = check_duplicate_reservations([r1, r2])
        assert len(anomalies) == 1
        assert anomalies[0].type == AnomalyType.DUPLICATE_RESERVATION
        assert anomalies[0].severity == Severity.WARNING

    def test_three_copies_detected(self):
        r1 = make_reservation(reference_number="AAA", source_file="file1.csv")
        r2 = make_reservation(reference_number="AAA", source_file="file2.csv")
        r3 = make_reservation(reference_number="AAA", source_file="file3.csv")
        anomalies = check_duplicate_reservations([r1, r2, r3])
        assert len(anomalies) == 2


# ---------------------------------------------------------------------------
# Per-reservation amount validation
# ---------------------------------------------------------------------------

class TestValidateReservationAmounts:
    def test_correct_net_no_anomaly(self):
        # Net = Amount + Commission + PaymentCharge + CityTax
        # 427.42 = 544.60 - 89.05 - 7.33 - 20.80
        r = make_reservation()
        anomalies = validate_reservation_amounts(r)
        assert not any(a.type == AnomalyType.AMOUNT_MISMATCH for a in anomalies)

    def test_net_mismatch_detected(self):
        r = make_reservation(net=Decimal("999.99"))  # obviously wrong
        anomalies = validate_reservation_amounts(r)
        assert any(a.type == AnomalyType.AMOUNT_MISMATCH for a in anomalies)
        assert any(a.severity == Severity.WARNING for a in anomalies)

    def test_commission_rate_within_bounds_no_anomaly(self):
        # 89.05 / 544.60 ≈ 16.4% — within [10%, 20%]
        r = make_reservation()
        anomalies = validate_reservation_amounts(r)
        assert not any(
            a.type in (AnomalyType.COMMISSION_RATE_HIGH, AnomalyType.COMMISSION_RATE_LOW)
            for a in anomalies
        )

    def test_high_commission_rate_detected(self):
        # 25% commission rate
        r = make_reservation(
            amount=Decimal("100.00"),
            commission=Decimal("-25.00"),
            payment_charge=Decimal("-2.00"),
            city_tax=Decimal("-5.00"),
            net=Decimal("68.00"),
        )
        anomalies = validate_reservation_amounts(r)
        assert any(a.type == AnomalyType.COMMISSION_RATE_HIGH for a in anomalies)

    def test_low_commission_rate_detected(self):
        # 5% commission rate
        r = make_reservation(
            amount=Decimal("100.00"),
            commission=Decimal("-5.00"),
            payment_charge=Decimal("-1.00"),
            city_tax=Decimal("-2.00"),
            net=Decimal("92.00"),
        )
        anomalies = validate_reservation_amounts(r)
        assert any(a.type == AnomalyType.COMMISSION_RATE_LOW for a in anomalies)

    def test_zero_amount_skips_commission_check(self):
        # Should not crash or warn when Amount = 0
        r = make_reservation(
            amount=Decimal("0"),
            commission=Decimal("0"),
            payment_charge=Decimal("0"),
            city_tax=Decimal("0"),
            net=Decimal("0"),
        )
        anomalies = validate_reservation_amounts(r)
        assert not any(
            a.type in (AnomalyType.COMMISSION_RATE_HIGH, AnomalyType.COMMISSION_RATE_LOW)
            for a in anomalies
        )


# ---------------------------------------------------------------------------
# Global balance check
# ---------------------------------------------------------------------------

def _call_balance(net, amount, commission, payment_charge, city_tax):
    return check_balance(
        total_net=net,
        total_amount=amount,
        total_commission=commission,
        total_payment_charge=payment_charge,
        total_city_tax=city_tax,
    )


class TestCheckBalance:
    def test_consistent_amounts_no_anomaly(self):
        # 427.42 = 544.60 - 89.05 - 7.33 - 20.80
        result = _call_balance(
            net=Decimal("427.42"),
            amount=Decimal("544.60"),
            commission=Decimal("-89.05"),
            payment_charge=Decimal("-7.33"),
            city_tax=Decimal("-20.80"),
        )
        assert result is None

    def test_within_tolerance_no_anomaly(self):
        result = _call_balance(
            net=Decimal("427.45"),  # 0.03€ off — within ±0.05 tolerance
            amount=Decimal("544.60"),
            commission=Decimal("-89.05"),
            payment_charge=Decimal("-7.33"),
            city_tax=Decimal("-20.80"),
        )
        assert result is None

    def test_outside_tolerance_blocking_anomaly(self):
        result = _call_balance(
            net=Decimal("999.00"),  # wildly wrong Net
            amount=Decimal("544.60"),
            commission=Decimal("-89.05"),
            payment_charge=Decimal("-7.33"),
            city_tax=Decimal("-20.80"),
        )
        assert result is not None
        assert result.type == AnomalyType.BALANCE_ERROR
        assert result.severity == Severity.BLOCKING

    def test_difference_exactly_at_tolerance_passes(self):
        # diff = 0.05 should NOT trigger (condition is strictly diff > tolerance)
        result = _call_balance(
            net=Decimal("427.47"),  # 0.05€ off
            amount=Decimal("544.60"),
            commission=Decimal("-89.05"),
            payment_charge=Decimal("-7.33"),
            city_tax=Decimal("-20.80"),
        )
        assert result is None

    def test_difference_just_over_tolerance_fails(self):
        result = _call_balance(
            net=Decimal("427.48"),  # 0.06€ off
            amount=Decimal("544.60"),
            commission=Decimal("-89.05"),
            payment_charge=Decimal("-7.33"),
            city_tax=Decimal("-20.80"),
        )
        assert result is not None
        assert result.type == AnomalyType.BALANCE_ERROR
