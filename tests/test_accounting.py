"""Tests for accounting entry generation and PennyLane export.

Réécrits 2026-07-03 (audit finding #11) : les anciennes assertions codaient la
structure PRÉ-refacto 2026-04-13 (fees agrégés pour Booking, client débité du
gross TTC). Les assertions ci-dessous sont re-dérivées de la logique comptable
en prod depuis avril 2026 (validée au centime sur les re-déversements) :

Booking (per_reservation_fees=True, comm adjustment → 401BOOKING) :
  - Header : DEBIT 51105000 = Sum(Net)
  - 1 ligne FEE par résa : DEBIT 401BOOKING = |commission| + |payment_charge|
  - 1 ligne client par résa : CREDIT 411BOOKING = Amount − |CityTax|
Airbnb (fees agrégés, cancellation fees → 604610) :
  - Header : DEBIT 51105000 = Sum(Net)
  - 1 ligne agrégée : DEBIT 604600 = Sum(fees) (omise si 0)
  - 1 ligne client par résa : CREDIT 411AIRBNB = Amount (city_tax = 0)

Invariant fondamental dans tous les cas : Total DEBIT == Total CREDIT.
"""

import csv
import tempfile
from datetime import date
from decimal import Decimal
from pathlib import Path

from accounting.entries import generate_entries
from accounting.pennylane import export_to_csv
from models.reservation import Reservation

# Kwargs prod (mêmes valeurs que sources/booking.py et sources/airbnb.py —
# dupliqués sciemment : si la config prod change, ces tests doivent casser).
BOOKING_KWARGS = dict(
    per_reservation_fees=True,
    account_commission_adjustment="401BOOKING",
)
AIRBNB_KWARGS = dict(
    journal_code="AIRB",
    account_bank="51105000",
    account_client="411AIRBNB",
    account_supplier="604600",
    account_cancellation_fee="604610",
    ota_label="AIRBNB",
)

MAPPING = {"3015679": "MER21-0G"}
PROC_DATE = date(2025, 10, 3)


def make_reservation(**kwargs) -> Reservation:
    """Résa Booking standard : Net = Amount − |Commission| − |Charge| − |CityTax|."""
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


def assert_balanced(entries):
    """L'invariant comptable : le journal doit être équilibré au centime."""
    total_debit = sum(e.debit or Decimal("0") for e in entries)
    total_credit = sum(e.credit or Decimal("0") for e in entries)
    assert total_debit == total_credit, f"déséquilibre : D={total_debit} C={total_credit}"


# ---------------------------------------------------------------------------
# Booking — config prod (fees éclatés par résa)
# ---------------------------------------------------------------------------

class TestBookingEntries:
    def test_structure_standard(self):
        """1 résa → header bank + 1 FEE + 1 client = 3 lignes."""
        entries, processed, anomalies = generate_entries(
            [make_reservation()], PROC_DATE, MAPPING, **BOOKING_KWARGS)
        assert len(entries) == 3
        assert [e.account for e in entries] == ["51105000", "401BOOKING", "411BOOKING"]
        assert not anomalies
        assert_balanced(entries)

    def test_bank_debit_equals_net(self):
        entries, _, _ = generate_entries(
            [make_reservation()], PROC_DATE, MAPPING, **BOOKING_KWARGS)
        bank = entries[0]
        assert bank.debit == Decimal("427.42")
        assert bank.credit is None
        assert "Encaissement BOOKING" in bank.label
        assert "Oct 1, 2025" in bank.label  # payout date, format label EN

    def test_fee_line_per_reservation(self):
        entries, _, _ = generate_entries(
            [make_reservation()], PROC_DATE, MAPPING, **BOOKING_KWARGS)
        fee = entries[1]
        assert fee.debit == Decimal("89.05") + Decimal("7.33")
        assert fee.credit is None
        assert "FEE BOOKING" in fee.label
        assert "MER21-0G" in fee.label and "3015679" in fee.label
        assert fee.ota_reservation_ref == "5493245107"

    def test_client_credit_is_gross_excl_city_tax(self):
        """CREDIT 411 = Amount − |CityTax| (PAS le TTC, PAS le net)."""
        entries, _, _ = generate_entries(
            [make_reservation()], PROC_DATE, MAPPING, **BOOKING_KWARGS)
        client = entries[2]
        assert client.credit == Decimal("544.60") - Decimal("20.80")
        assert client.debit is None
        assert "Test Guest" in client.label
        assert "CO :Sep 1, 2025" in client.label  # checkout, format label EN

    def test_multi_reservations_aggregate_bank(self):
        r2 = make_reservation(
            reference_number="5500000001",
            amount=Decimal("320.00"), commission=Decimal("-52.48"),
            payment_charge=Decimal("-4.57"), city_tax=Decimal("-10.40"),
            net=Decimal("252.55"),
        )
        entries, _, _ = generate_entries(
            [make_reservation(), r2], PROC_DATE, MAPPING, **BOOKING_KWARGS)
        # header + 2 FEE + 2 client
        assert len(entries) == 5
        assert entries[0].debit == Decimal("427.42") + Decimal("252.55")
        assert_balanced(entries)

    def test_refund_reservation_inverts_sides(self):
        """Remboursement : montants négatifs → client passe en DEBIT, fee en CREDIT."""
        refund = make_reservation(
            amount=Decimal("-544.60"), commission=Decimal("89.05"),
            payment_charge=Decimal("7.33"), city_tax=Decimal("20.80"),
            net=Decimal("-427.42"), reservation_status="cancelled_by_guest",
        )
        # NB : city_tax positive ici = ligne de remboursement, le validator la
        # signale en WARNING mais generate_entries doit rester cohérent.
        entries, _, _ = generate_entries([refund], PROC_DATE, MAPPING, **BOOKING_KWARGS)
        bank, fee, client = entries
        assert bank.credit == Decimal("427.42") and bank.debit is None
        assert fee.credit == Decimal("89.05") + Decimal("7.33") and fee.debit is None
        assert client.debit == Decimal("544.60") - Decimal("20.80") and client.credit is None
        assert_balanced(entries)

    def test_commission_adjustment_routed_to_supplier(self):
        """Statut 'Commission adjustment' → 401BOOKING direct, pas 411 (Philippe 2026-06-08)."""
        adj = make_reservation(
            reference_number="ADJ001", reservation_status="Commission adjustment",
            amount=Decimal("-12.50"), net=Decimal("-12.50"),
            commission=Decimal("0"), payment_charge=Decimal("0"), city_tax=Decimal("0"),
        )
        entries, _, _ = generate_entries(
            [make_reservation(), adj], PROC_DATE, MAPPING, **BOOKING_KWARGS)
        adj_lines = [e for e in entries if e.ota_reservation_ref == "ADJ001"]
        assert len(adj_lines) == 1  # pas de ligne FEE (commission nulle)
        assert adj_lines[0].account == "401BOOKING"
        assert adj_lines[0].debit == Decimal("12.50")  # net négatif → DEBIT
        assert "Comm ajustement" in adj_lines[0].label
        assert_balanced(entries)

    def test_mapping_not_found_blocks_reservation(self):
        r = make_reservation(ref_appart="UNKNOWN")
        entries, processed, anomalies = generate_entries(
            [r], PROC_DATE, MAPPING, **BOOKING_KWARGS)
        assert processed == []
        assert entries == []
        assert any(a.type == "MAPPING_NOT_FOUND" and a.severity == "BLOCKING"
                   for a in anomalies)

    def test_mapping_normalized_key_fallback(self):
        """Le lookup retombe sur la clé normalisée (espaces autour des tirets)."""
        mapping = {"HOC16 - 7G": "P08-HOC16-7G"}
        r = make_reservation(ref_appart="HOC16-7G")
        _, processed, anomalies = generate_entries(
            [r], PROC_DATE, mapping, **BOOKING_KWARGS)
        assert not anomalies
        assert processed[0].code_comptable == "P08-HOC16-7G"

    def test_mews_fallback_resolves_renamed_listing(self):
        """Libellé absent du mapping (annonce renommée) → résolu via Mews sur
        le code de confirmation, aucune anomalie bloquante."""
        r = make_reservation(ref_appart="Libellé Renommé Inconnu", reference_number="HM8QYX9WZN")
        _, processed, anomalies = generate_entries(
            [r], PROC_DATE, MAPPING, mews_fallback={"HM8QYX9WZN": "MER21-0G"}, **BOOKING_KWARGS)
        assert not [a for a in anomalies if a.severity == "BLOCKING"]
        assert processed[0].code_comptable == "MER21-0G"

    def test_mews_fallback_ignored_when_label_present(self):
        """Le libellé reste primaire : un fallback Mews divergent n'écrase jamais
        une résolution par le mapping (zéro régression sur le happy path)."""
        r = make_reservation(ref_appart="3015679", reference_number="HMX")
        _, processed, _ = generate_entries(
            [r], PROC_DATE, MAPPING, mews_fallback={"HMX": "WRONG-CODE"}, **BOOKING_KWARGS)
        assert processed[0].code_comptable == "MER21-0G"

    def test_mews_fallback_absent_still_blocks(self):
        """Ni mapping ni fallback → BLOCKING inchangé (comportement historique)."""
        r = make_reservation(ref_appart="UNKNOWN", reference_number="HMNOPE")
        _, processed, anomalies = generate_entries(
            [r], PROC_DATE, MAPPING, mews_fallback={}, **BOOKING_KWARGS)
        assert processed == []
        assert any(a.type == "MAPPING_NOT_FOUND" and a.severity == "BLOCKING" for a in anomalies)

    def test_ref_piece_filled_from_bill_lookup(self):
        bill_lookup = {"5493245107": ("bill-uuid-1", "FAC-2025-042")}
        entries, _, _ = generate_entries(
            [make_reservation()], PROC_DATE, MAPPING,
            bill_lookup=bill_lookup, **BOOKING_KWARGS)
        fee, client = entries[1], entries[2]
        assert fee.ref_piece == "FAC-2025-042"
        assert client.ref_piece == "FAC-2025-042"
        assert client.bill_id_mews == "bill-uuid-1"

    def test_entry_date_is_processing_date(self):
        entries, _, _ = generate_entries(
            [make_reservation()], PROC_DATE, MAPPING, **BOOKING_KWARGS)
        assert all(e.date == PROC_DATE for e in entries)


# ---------------------------------------------------------------------------
# Airbnb — config prod (fees agrégés, cancellation fees → 604610)
# ---------------------------------------------------------------------------

def make_airbnb_reservation(**kwargs) -> Reservation:
    """Résa Airbnb standard : city_tax toujours 0, Net = Amount − |Commission|."""
    defaults = dict(
        source_file="airbnb-2025-10.xlsx",
        ref_appart="3015679",
        payout_id="AIRBNBPAYOUT",
        reference_number="HMABCDE123",
        check_in=date(2025, 8, 28),
        checkout=date(2025, 9, 1),
        guest_name="Airbnb Guest",
        reservation_status="Réservation",
        currency="EUR",
        payment_status="",
        city_tax=Decimal("0"),
        amount=Decimal("600.00"),
        commission=Decimal("-90.00"),
        payment_charge=Decimal("0"),
        net=Decimal("510.00"),
        payout_date=date(2025, 10, 1),
    )
    defaults.update(kwargs)
    return Reservation(**defaults)


class TestAirbnbEntries:
    def test_structure_standard(self):
        """1 résa → header bank + 1 fees agrégée + 1 client = 3 lignes."""
        entries, _, anomalies = generate_entries(
            [make_airbnb_reservation()], PROC_DATE, MAPPING, **AIRBNB_KWARGS)
        assert len(entries) == 3
        assert [e.account for e in entries] == ["51105000", "604600", "411AIRBNB"]
        assert entries[0].journal == "AIRB"
        assert entries[0].debit == Decimal("510.00")
        assert entries[1].debit == Decimal("90.00")
        assert entries[2].credit == Decimal("600.00")
        assert_balanced(entries)

    def test_fees_aggregated_over_batch(self):
        r2 = make_airbnb_reservation(
            reference_number="HMZZZ999", amount=Decimal("400.00"),
            commission=Decimal("-60.00"), net=Decimal("340.00"),
        )
        entries, _, _ = generate_entries(
            [make_airbnb_reservation(), r2], PROC_DATE, MAPPING, **AIRBNB_KWARGS)
        # header + 1 seule ligne fees agrégée + 2 client
        assert len(entries) == 4
        assert entries[1].debit == Decimal("150.00")
        assert_balanced(entries)

    def test_cancellation_fee_routed_to_604610(self):
        """'Frais d'annulation' (montant négatif) → DEBIT 604610 (charge)."""
        fee_row = make_airbnb_reservation(
            reference_number="HMCANCEL1", reservation_status="Frais d'annulation",
            amount=Decimal("-50.00"), commission=Decimal("0"), net=Decimal("-50.00"),
        )
        entries, _, _ = generate_entries(
            [make_airbnb_reservation(), fee_row], PROC_DATE, MAPPING, **AIRBNB_KWARGS)
        cancel_lines = [e for e in entries if e.ota_reservation_ref == "HMCANCEL1"]
        assert len(cancel_lines) == 1
        assert cancel_lines[0].account == "604610"
        assert cancel_lines[0].debit == Decimal("50.00")
        assert_balanced(entries)

    def test_cancellation_fee_reimbursement_credited(self):
        """'Remboursement des frais d'annulation' (positif) → CREDIT 604610 (Philippe 2026-06-15)."""
        reimb = make_airbnb_reservation(
            reference_number="HMREIMB1",
            reservation_status="Remboursement des frais d'annulation",
            amount=Decimal("50.00"), commission=Decimal("0"), net=Decimal("50.00"),
        )
        entries, _, _ = generate_entries([reimb], PROC_DATE, MAPPING, **AIRBNB_KWARGS)
        # Batch 100% remboursements sans fees → PAS de ligne 604600 vide
        assert [e.account for e in entries] == ["51105000", "604610"]
        assert entries[1].credit == Decimal("50.00")
        assert_balanced(entries)

    def test_refund_batch_credits_bank(self):
        """Net total négatif (l'argent repart vers l'OTA) → CREDIT banque."""
        refund = make_airbnb_reservation(
            amount=Decimal("-600.00"), commission=Decimal("90.00"),
            net=Decimal("-510.00"),
        )
        entries, _, _ = generate_entries([refund], PROC_DATE, MAPPING, **AIRBNB_KWARGS)
        assert entries[0].credit == Decimal("510.00") and entries[0].debit is None
        assert_balanced(entries)


# ---------------------------------------------------------------------------
# PennyLane CSV export (mode CLI main.py)
# ---------------------------------------------------------------------------

def _export(entries):
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as f:
        out = Path(f.name)
    export_to_csv(entries, out)
    with open(out, encoding="utf-8") as f:
        rows = list(csv.reader(f))
    out.unlink()
    return rows


class TestPennylaneExport:
    def test_csv_has_correct_headers(self):
        entries, _, _ = generate_entries(
            [make_reservation()], PROC_DATE, MAPPING, **BOOKING_KWARGS)
        rows = _export(entries)
        assert rows[0] == ["Journal", "Date", "Réf. pièce", "Compte", "Libellé", "Débit", "Crédit"]

    def test_csv_row_count_matches_entries(self):
        entries, _, _ = generate_entries(
            [make_reservation()], PROC_DATE, MAPPING, **BOOKING_KWARGS)
        rows = _export(entries)
        assert len(rows) == 1 + len(entries)

    def test_debit_empty_when_none_and_two_decimals(self):
        entries, _, _ = generate_entries(
            [make_reservation()], PROC_DATE, MAPPING, **BOOKING_KWARGS)
        rows = _export(entries)
        # Ligne client (411BOOKING, CREDIT) : Débit vide, Crédit à 2 décimales
        client_row = rows[3]
        assert client_row[3] == "411BOOKING"
        assert client_row[5] == ""
        assert len(client_row[6].split(".")[1]) == 2
