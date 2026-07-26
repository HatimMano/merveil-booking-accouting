"""Tests du flux 2 — MewsPaymentsSource (encaissements carte Adyen).

Pas de BQ : on teste `_to_reservation` + `generate_entries` avec les kwargs
prod de la source sur des rows synthétiques calqués sur le format réel des
exports webhook Mews (convention validée au centime : net = gross + commission,
commission SIGNÉE — négative = frais prélevés, positive = frais restitués).

Invariant fondamental : Total DEBIT == Total CREDIT sur chaque payout.
"""

from datetime import date
from decimal import Decimal

from accounting.entries import generate_entries
from sources.mews_payments import MewsPaymentsSource, MewsPayoutBatch

PAYOUT_DATE = date(2026, 7, 16)
SRC = MewsPaymentsSource.__new__(MewsPaymentsSource)  # sans client BQ


def make_row(**overrides):
    """Transaction Charge nominale : Site direct, 1000€ gross, -25€ commission."""
    row = {
        "payout_id": "91ab8bfa-payout",
        "payout_date": PAYOUT_DATE,
        "payout_net": Decimal("975.00"),
        "transaction_id": "FS5V4BP7BVZMKSG6",
        "transaction_type": "Charge",
        "gross": Decimal("1000.00"),
        "commission": Decimal("-25.00"),
        "net": Decimal("975.00"),
        "payment_matched": True,
        "bill_id": "bill-1",
        "account_type": "Customer",
        "original_currency": "EUR",
        "mews_value": Decimal("1000.00"),
        "charged_date": PAYOUT_DATE,
        "canal": "Site direct",
        "apartment_code": "P02-MER21-0G",
        "guest_name": "Marie Lopez",
        "checkout_date": date(2026, 7, 20),
        "bill_number": "27753",
    }
    row.update(overrides)
    return row


def build_entries(rows):
    anomalies = []
    reservations = [SRC._to_reservation(r, "bq:test", anomalies) for r in rows]
    batch = MewsPayoutBatch(
        payout_id=rows[0]["payout_id"],
        payout_date=PAYOUT_DATE,
        payout_net=sum(r["net"] for r in rows),
    )
    batch.reservations = reservations
    entries, processed, entry_anoms = generate_entries(
        batch.reservations, batch.entry_date,
        {"P02-MER21-0G": "MER21-0G"},
        **SRC.entries_kwargs,
    )
    return entries, processed, anomalies + entry_anoms


def assert_balanced(entries):
    debit = sum(e.debit or Decimal("0") for e in entries)
    credit = sum(e.credit or Decimal("0") for e in entries)
    assert abs(debit - credit) < Decimal("0.01"), f"D={debit} C={credit}"


def test_charge_nominal():
    entries, _, anomalies = build_entries([make_row()])
    assert_balanced(entries)
    assert not anomalies

    bank = next(e for e in entries if e.account == "511035")
    assert bank.debit == Decimal("975.00")
    assert bank.date == PAYOUT_DATE  # écriture datée du versement

    fees = next(e for e in entries if e.account == "401MEWS")
    assert fees.debit == Decimal("25.00")

    client = next(e for e in entries if e.account == "411WEBSITE")
    assert client.credit == Decimal("1000.00")
    assert client.ref_piece == "27753"  # bill pré-résolu par la source
    assert "MER21-0G" in client.label and "DIRECT" in client.label


def test_canal_expedia_compte_dedie():
    entries, _, _ = build_entries([make_row(canal="Expedia")])
    assert any(e.account == "411EXPEDIA" for e in entries)


def test_refund_avec_commission_restituee():
    # Adyen restitue sa commission sur un refund : commission POSITIVE.
    entries, _, anomalies = build_entries([
        make_row(),
        make_row(
            transaction_id="RF1", transaction_type="Refund",
            gross=Decimal("-200.00"), commission=Decimal("5.00"),
            net=Decimal("-195.00"), bill_number=None,
        ),
    ])
    assert_balanced(entries)
    assert not [a for a in anomalies if a.type == "AMOUNT_MISMATCH"]
    # Le refund inverse la ligne client : DEBIT 411.
    refund_line = [e for e in entries if e.account == "411WEBSITE" and e.debit]
    assert refund_line and refund_line[0].debit == Decimal("200.00")
    # Frais agrégés signés : 25 prélevés − 5 restitués = 20.
    fees = next(e for e in entries if e.account == "401MEWS")
    assert fees.debit == Decimal("20.00")


def test_chargeback_via_chemin_nominal():
    entries, _, _ = build_entries([
        make_row(),
        make_row(
            transaction_id="CB1", transaction_type="Chargeback",
            gross=Decimal("-808.80"), commission=Decimal("25.07"),
            net=Decimal("-783.73"), bill_number=None,
        ),
    ])
    assert_balanced(entries)
    cb = [e for e in entries if e.account == "411WEBSITE" and e.debit]
    assert cb and cb[0].debit == Decimal("808.80")


def test_fee_row_gross_null_route_401():
    # Commission adjustment / Platform fee (ère Stripe) : gross NULL → 401MEWS.
    entries, _, anomalies = build_entries([
        make_row(),
        make_row(
            transaction_id="ADJ1", transaction_type="Platform fee",
            gross=None, commission=Decimal("-31.81"), net=Decimal("-31.81"),
            canal=None, apartment_code=None, guest_name=None,
            checkout_date=None, bill_id=None, bill_number=None,
        ),
    ])
    assert_balanced(entries)
    assert any(a.type == "FEE_TRANSACTION" for a in anomalies)
    adj = next(e for e in entries if e.ota_reservation_ref == "ADJ1")
    assert adj.account == "401MEWS" and adj.debit == Decimal("31.81")
    # Aucune ligne client 411 pour cette transaction.
    assert not any(
        e.ota_reservation_ref == "ADJ1" and e.account.startswith("411")
        for e in entries
    )


def test_canal_booking_emet_unexpected_channel():
    _, _, anomalies = build_entries([make_row(canal="Booking.com")])
    assert any(a.type == "UNEXPECTED_CHANNEL" for a in anomalies)


def test_sans_resa_compte_customer_route_website():
    _, _, anomalies = build_entries([
        make_row(canal=None, apartment_code=None, bill_number=None),
    ])
    assert any(a.type == "CANAL_UNRESOLVED" for a in anomalies)


def test_fx_mismatch_detecte():
    _, _, anomalies = build_entries([
        make_row(original_currency="USD", mews_value=Decimal("966.15")),
    ])
    assert any(a.type == "FX_VALUE_MISMATCH" for a in anomalies)


def test_payout_balance_check():
    anomalies = []
    batch = MewsPayoutBatch(
        payout_id="p1", payout_date=PAYOUT_DATE, payout_net=Decimal("999.99"),
    )
    batch.reservations = [SRC._to_reservation(make_row(), "bq:test", anomalies)]
    SRC._check_payout_balance(batch, "bq:test", anomalies)
    assert any(a.type == "PAYOUT_UNBALANCED" and a.severity == "BLOCKING" for a in anomalies)
