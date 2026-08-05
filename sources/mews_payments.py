"""Source Mews Payments (flux 2) — encaissements carte Adyen depuis BigQuery.

Lit les exports webhook Mews (stg_mews_exports__payouts + payout_transactions),
joint stg_mews__payments (identifier, validé au centime — ADR 2026-07-16) puis
fct_reservations pour le canal, et produit 1 batch par VERSEMENT :

    DEBIT  511035        = net versé (arrive en banque, lettré par la jambe import bancaire)
    DEBIT  401MEWS       = Σ frais Adyen du versement (lettrés contre la facture mensuelle Mews)
    CREDIT 411<canal>    = gross par paiement (Site direct/Expedia/VRBO/Marriott/…)

Idempotence : journal_scope='batch_key' — un payout_id posté une fois est posté
pour toujours, quelle que soit la fenêtre de fetch (les payouts sont immuables).

Résolution canal, par priorité :
  1. résa du paiement (fct_reservations.ota_source)
  2. bill du paiement → résas des order items (si 1 seul canal distinct)
  3. compte Customer → Site direct (pratique observée de Philippe, ex. Jason Paez)
  4. sinon 411DIVERS — chaque fallback 3/4 émet une anomalie WARNING.

Multi-devises : le gross export = montant réellement réglé par Adyen (Charged,
EUR) — l'écriture reste équilibrée avec la banque. L'écart vs le TTC Mews
(Value) est signalé en WARNING FX_VALUE_MISMATCH (convention à figer avec
Philippe pendant la revue bq_only).
"""

import hashlib
import logging
import os
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

from google.cloud import bigquery

from config.mapping_loader import load_apartment_comptable_map
from config.settings import (
    MEWS_PAYMENTS_ACCOUNT_BANK,
    MEWS_PAYMENTS_ACCOUNT_FALLBACK,
    MEWS_PAYMENTS_ACCOUNT_SUPPLIER,
    MEWS_PAYMENTS_CHANNEL_ACCOUNTS,
    MEWS_PAYMENTS_EXPORT_MAX_AGE_HOURS,
    MEWS_PAYMENTS_JOURNAL_CODE,
    MEWS_PAYMENTS_OVERLAP_DAYS,
)
from models.reservation import Reservation
from validators.anomalies import Anomaly, Severity
from .base import Source, SourceFetchResult

logger = logging.getLogger(__name__)

_PROJECT = "merveil-data-warehouse"

_APARTMENT_CODE_MAP_PATH = (
    Path(__file__).parent.parent / "config" / "mapping" / "Mapping_appart_code.csv"
)

# Un paiement carte sur ces canaux est légitime (VCC / lien de paiement) mais
# doit rester rare — les payouts OTA (flux 1) sont le circuit nominal.
_FLUX1_CHANNELS = {"Booking.com", "Airbnb"}

_FRESHNESS_QUERY = f"""
SELECT MAX(delivered_at) AS last_delivery
FROM `{_PROJECT}.staging.stg_mews_exports__payouts`
"""

_FETCH_QUERY = f"""
WITH payouts AS (
    -- Périmètre = DATE DU PAYOUT (created_at), pas delivered_at : la livraison
    -- du 2026-07-16 était un dump historique one-shot (toute l'ère Stripe
    -- depuis 2025-01) — un filtre sur delivered_at ré-embarquerait 365 payouts
    -- Stripe déjà comptabilisés manuellement par Philippe.
    SELECT payout_id, DATE(created_at) AS payout_date, net AS payout_net
    FROM `{_PROJECT}.staging.stg_mews_exports__payouts`
    WHERE payout_type = 'Payout' AND DATE(created_at) >= @since_date
    QUALIFY ROW_NUMBER() OVER (PARTITION BY payout_id ORDER BY delivered_at DESC) = 1
),
tx AS (
    SELECT payout_id, transaction_id, transaction_type, gross, commission, net
    FROM `{_PROJECT}.staging.stg_mews_exports__payout_transactions`
    WHERE payout_id IN (SELECT payout_id FROM payouts)
      AND NOT (COALESCE(gross, 0) = 0 AND net = 0 AND commission = 0)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY payout_id, transaction_id ORDER BY delivered_at DESC
    ) = 1
),
pay AS (
    SELECT identifier, reservation_id, bill_id, account_id, account_type,
           original_currency, -amount_gross AS mews_value, DATE(charged_at) AS charged_date
    FROM `{_PROJECT}.staging.stg_mews__payments`
    WHERE identifier IN (SELECT transaction_id FROM tx)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY identifier ORDER BY updated_at DESC) = 1
),
resa AS (
    SELECT reservation_id, ota_source, apartment_code, customer_name, checkout_date
    FROM `{_PROJECT}.marts.fct_reservations`
    WHERE reservation_id IN (SELECT reservation_id FROM pay WHERE reservation_id IS NOT NULL)
),
-- Fallback canal pour les paiements sans résa : via le bill → résas des items
bill_ctx AS (
    SELECT
        oi.bill_id,
        ARRAY_AGG(DISTINCT r.ota_source IGNORE NULLS) AS canals,
        ARRAY_AGG(
            STRUCT(r.apartment_code, r.customer_name, r.checkout_date)
            ORDER BY r.checkout_date DESC LIMIT 1
        )[SAFE_OFFSET(0)] AS ctx
    FROM `{_PROJECT}.staging.stg_mews__order_items` oi
    JOIN `{_PROJECT}.marts.fct_reservations` r ON r.reservation_id = oi.reservation_id
    WHERE oi.bill_id IN (
        SELECT bill_id FROM pay WHERE reservation_id IS NULL AND bill_id IS NOT NULL
    )
    GROUP BY oi.bill_id
),
bills AS (
    SELECT bill_id, bill_number
    FROM `{_PROJECT}.staging.stg_mews__bills`
    WHERE bill_id IN (SELECT bill_id FROM pay WHERE bill_id IS NOT NULL)
),
cust AS (
    SELECT customer_id,
           TRIM(CONCAT(COALESCE(first_name, ''), ' ', COALESCE(last_name, ''))) AS account_name
    FROM `{_PROJECT}.staging.stg_mews__customers`
    WHERE customer_id IN (SELECT account_id FROM pay WHERE reservation_id IS NULL)
)
SELECT
    po.payout_id,
    po.payout_date,
    po.payout_net,
    t.transaction_id,
    t.transaction_type,
    t.gross,
    t.commission,
    t.net,
    p.identifier IS NOT NULL AS payment_matched,
    p.bill_id,
    p.account_type,
    p.original_currency,
    p.mews_value,
    p.charged_date,
    COALESCE(r.ota_source,
             IF(ARRAY_LENGTH(bc.canals) = 1, bc.canals[SAFE_OFFSET(0)], NULL)) AS canal,
    COALESCE(r.apartment_code, bc.ctx.apartment_code) AS apartment_code,
    COALESCE(r.customer_name, bc.ctx.customer_name, c.account_name) AS guest_name,
    COALESCE(r.checkout_date, bc.ctx.checkout_date) AS checkout_date,
    b.bill_number
FROM tx t
JOIN payouts po USING (payout_id)
LEFT JOIN pay p ON p.identifier = t.transaction_id
LEFT JOIN resa r ON r.reservation_id = p.reservation_id
LEFT JOIN bill_ctx bc ON bc.bill_id = p.bill_id AND p.reservation_id IS NULL
LEFT JOIN bills b ON b.bill_id = p.bill_id
LEFT JOIN cust c ON c.customer_id = p.account_id
ORDER BY po.payout_date, po.payout_id, t.transaction_id
"""


@dataclass
class MewsPayoutBatch:
    """Un versement Adyen = un batch = une écriture Pennylane équilibrée."""

    payout_id: str
    payout_date: date
    payout_net: Decimal
    reservations: list = field(default_factory=list)

    @property
    def entry_date(self) -> date:
        # Les écritures sont datées du versement (comme la jambe bancaire),
        # pas du jour de run — lu par l'orchestrator via getattr.
        return self.payout_date


class MewsPaymentsSource(Source):
    name = "mews-payments"

    # Un payout posté est posté pour toujours, quelle que soit la fenêtre.
    journal_scope = "batch_key"

    # Frais Adyen observés 0.3-4 % — les bornes OTA (10-20 %) n'ont pas de sens ici.
    commission_rate_bounds = (Decimal("0"), Decimal("0.06"))

    @property
    def entries_kwargs(self) -> dict[str, Any]:
        return {
            "journal_code": MEWS_PAYMENTS_JOURNAL_CODE,
            "account_bank": MEWS_PAYMENTS_ACCOUNT_BANK,
            "account_supplier": MEWS_PAYMENTS_ACCOUNT_SUPPLIER,
            "account_client": MEWS_PAYMENTS_ACCOUNT_FALLBACK,
            # Lignes pur frais (Commission adjustment / Platform fee, gross NULL —
            # ère Stripe uniquement à date) → routées direct sur 401MEWS.
            "account_commission_adjustment": MEWS_PAYMENTS_ACCOUNT_SUPPLIER,
            "ota_label": "MEWS",
            "per_reservation_fees": False,
        }

    def __init__(self, bq_client: Optional[bigquery.Client] = None):
        self._bq = bq_client or bigquery.Client(project=_PROJECT)
        self.overlap_days = int(
            os.environ.get("MEWS_PAYMENTS_OVERLAP_DAYS", MEWS_PAYMENTS_OVERLAP_DAYS)
        )
        self.export_max_age_hours = int(
            os.environ.get("MEWS_PAYMENTS_EXPORT_MAX_AGE_HOURS", MEWS_PAYMENTS_EXPORT_MAX_AGE_HOURS)
        )

    # ------------------------------------------------------------------

    def fetch(self, processing_date: date) -> SourceFetchResult:
        anomalies: list[Anomaly] = []
        source_label = f"bq:mews_payments:{processing_date.isoformat()}-J{self.overlap_days}"

        stale = self._check_export_freshness(source_label)
        if stale:
            anomalies.append(stale)

        since_date = processing_date - timedelta(days=self.overlap_days)
        job_config = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("since_date", "DATE", since_date),
        ])
        rows = list(self._bq.query(_FETCH_QUERY, job_config=job_config).result())
        logger.info(
            "Mews Payments fetch: %d transaction(s) sur les payouts créés depuis %s",
            len(rows), since_date,
        )
        if not rows:
            return SourceFetchResult(
                batches=[], anomalies=anomalies, mapping={}, source_file=source_label,
            )

        apt_map = load_apartment_comptable_map(_APARTMENT_CODE_MAP_PATH)

        batches: dict[str, MewsPayoutBatch] = {}
        for row in rows:
            pid = row["payout_id"]
            batch = batches.get(pid)
            if batch is None:
                batch = batches[pid] = MewsPayoutBatch(
                    payout_id=pid,
                    payout_date=row["payout_date"],
                    payout_net=Decimal(row["payout_net"]),
                )
            batch.reservations.append(
                self._to_reservation(row, source_label, anomalies)
            )

        for batch in batches.values():
            self._check_payout_balance(batch, source_label, anomalies)

        batch_list = sorted(batches.values(), key=lambda b: (b.payout_date, b.payout_id))
        file_hash = hashlib.md5(
            "|".join(f"{b.payout_id}:{b.payout_net}" for b in batch_list).encode()
        ).hexdigest()

        return SourceFetchResult(
            batches=batch_list,
            anomalies=anomalies,
            mapping=apt_map,          # {apartment_code complet: code_comptable}
            source_file=source_label,
            file_hash=file_hash,
        )

    # ------------------------------------------------------------------

    def _to_reservation(self, row, source_label: str, anomalies: list) -> Reservation:
        tx_id = row["transaction_id"]

        if row["gross"] is None or row["transaction_type"] == "Reserve adjustment":
            # Ligne sans contrepartie client : pur frais (Commission adjustment /
            # Platform fee — ère Stripe, gross NULL) ou retenue/restitution de
            # réserve Adyen (Reserve adjustment — gross = net, commission NULL,
            # 1er cas observé 2026-08-05 : −17 901,20 €). Convention parser
            # Booking : amount = net, commission = 0, status "Commission
            # adjustment" → entries.py route en D/C direct sur 401MEWS, aucune
            # ligne 411. Pour la réserve : retenue = DEBIT (créance sur Adyen),
            # release futur = CREDIT symétrique — convention à valider avec
            # Philippe à la revue.
            is_reserve = row["transaction_type"] == "Reserve adjustment"
            anomalies.append(Anomaly(
                type="RESERVE_ADJUSTMENT" if is_reserve else "FEE_TRANSACTION",
                severity=Severity.WARNING,
                message=(
                    f"Retenue/restitution de réserve Adyen ({row['net']}€, tx {tx_id}) "
                    f"— routée sur {MEWS_PAYMENTS_ACCOUNT_SUPPLIER}, convention à "
                    f"valider avec Philippe."
                ) if is_reserve else (
                    f"Transaction pur frais '{row['transaction_type']}' "
                    f"({row['net']}€, tx {tx_id}) — routée sur "
                    f"{MEWS_PAYMENTS_ACCOUNT_SUPPLIER} (pas de contrepartie client)."
                ),
                source_file=source_label,
                reservation_ref=tx_id,
                details={"transaction_type": row["transaction_type"], "net": str(row["net"])},
            ))
            net = Decimal(row["net"])
            return Reservation(
                source_file=source_label,
                ref_appart="",
                payout_id=row["payout_id"],
                reference_number=tx_id,
                check_in=row["payout_date"],
                checkout=row["payout_date"],
                guest_name=row["transaction_type"],
                reservation_status="Commission adjustment",
                currency="EUR",
                payment_status="",
                city_tax=Decimal("0"),
                amount=net,
                commission=Decimal("0"),
                payment_charge=Decimal("0"),
                net=net,
                payout_date=row["payout_date"],
                code_comptable="HORS-RESA",
            )

        canal = row["canal"]
        account_client, canal_label = (None, None)
        code_comptable = None

        if canal in MEWS_PAYMENTS_CHANNEL_ACCOUNTS:
            account_client, canal_label = MEWS_PAYMENTS_CHANNEL_ACCOUNTS[canal]
            if canal in _FLUX1_CHANNELS:
                anomalies.append(Anomaly(
                    type="UNEXPECTED_CHANNEL",
                    severity=Severity.WARNING,
                    message=(
                        f"Paiement carte Adyen sur une résa {canal} ({row['guest_name']}, "
                        f"{row['gross']}€, tx {tx_id}) — canal normalement couvert par les "
                        f"payouts OTA (flux 1). Crédité sur {account_client}, à vérifier "
                        f"qu'il n'apparaît pas AUSSI dans un payout {canal}."
                    ),
                    source_file=source_label,
                    reservation_ref=tx_id,
                    details={"canal": canal, "gross": str(row["gross"])},
                ))
        elif not row["payment_matched"]:
            account_client, canal_label = MEWS_PAYMENTS_ACCOUNT_FALLBACK, "DIVERS"
            anomalies.append(Anomaly(
                type="PAYMENT_UNMATCHED",
                severity=Severity.WARNING,
                message=(
                    f"Transaction {tx_id} ({row['gross']}€) absente de stg_mews__payments "
                    f"— canal irrésolvable, créditée sur {MEWS_PAYMENTS_ACCOUNT_FALLBACK}."
                ),
                source_file=source_label,
                reservation_ref=tx_id,
                details={"gross": str(row["gross"])},
            ))
        else:
            # Paiement sans canal : compte Customer = guest direct (pratique
            # Philippe observée au grand livre : 411WEBSITE), sinon DIVERS.
            if row["account_type"] == "Customer":
                account_client, canal_label = MEWS_PAYMENTS_CHANNEL_ACCOUNTS["Site direct"]
            else:
                account_client, canal_label = MEWS_PAYMENTS_ACCOUNT_FALLBACK, "DIVERS"
            anomalies.append(Anomaly(
                type="CANAL_UNRESOLVED",
                severity=Severity.WARNING,
                message=(
                    f"Paiement {tx_id} ({row['guest_name'] or 'compte inconnu'}, "
                    f"{row['gross']}€) sans résa ni canal via bill — crédité sur "
                    f"{account_client} (compte {row['account_type']})."
                ),
                source_file=source_label,
                reservation_ref=tx_id,
                details={
                    "gross": str(row["gross"]),
                    "account_type": row["account_type"] or "",
                    "bill_number": row["bill_number"] or "",
                },
            ))

        if (
            row["original_currency"] and row["original_currency"] != "EUR"
            and row["mews_value"] is not None
            and abs(Decimal(row["mews_value"]) - Decimal(row["gross"])) > Decimal("0.02")
        ):
            delta = Decimal(row["gross"]) - Decimal(row["mews_value"])
            anomalies.append(Anomaly(
                type="FX_VALUE_MISMATCH",
                severity=Severity.WARNING,
                message=(
                    f"Paiement {tx_id} en {row['original_currency']} : réglé Adyen "
                    f"{row['gross']}€ vs TTC Mews {row['mews_value']}€ (markup FX "
                    f"{delta:+.2f}€ crédité sur le 411 — convention à figer avec Philippe)."
                ),
                source_file=source_label,
                reservation_ref=tx_id,
                details={
                    "original_currency": row["original_currency"],
                    "charged_eur": str(row["gross"]),
                    "mews_value_eur": str(row["mews_value"]),
                },
            ))

        apartment_code = row["apartment_code"] or ""
        if not apartment_code:
            # Pas d'appart rattachable (paiement hors résa) — code figé pour le
            # label, le mapping est bypassé (code_comptable pré-résolu).
            code_comptable = "HORS-RESA"

        checkout = row["checkout_date"] or row["charged_date"] or row["payout_date"]

        return Reservation(
            source_file=source_label,
            ref_appart=apartment_code,
            payout_id=row["payout_id"],
            reference_number=tx_id,
            check_in=checkout,
            checkout=checkout,
            guest_name=row["guest_name"] or "Compte client Mews",
            reservation_status=row["transaction_type"],
            currency="EUR",
            payment_status=row["account_type"] or "",
            city_tax=Decimal("0"),
            amount=Decimal(row["gross"]),
            # NULL défensif : un type inconnu sans commission ne doit pas tuer
            # tout le run — l'écart éventuel est rattrapé par le balance check.
            commission=Decimal(row["commission"] if row["commission"] is not None else 0),
            payment_charge=Decimal("0"),
            net=Decimal(row["net"]),
            payout_date=row["payout_date"],
            code_comptable=code_comptable,
            account_client=account_client,
            canal=canal_label,
            bill_id_mews=row["bill_id"],
            bill_number=row["bill_number"],
        )

    # ------------------------------------------------------------------

    def _check_export_freshness(self, source_label: str) -> Optional[Anomaly]:
        """Mews ne retente pas un POST webhook raté — on refuse de tourner sur
        des exports périmés (72h couvre le week-end sans payout)."""
        row = list(self._bq.query(_FRESHNESS_QUERY).result())[0]
        last_delivery = row["last_delivery"]
        if last_delivery is None:
            age_hours = float("inf")
        else:
            age_hours = (datetime.now(timezone.utc) - last_delivery).total_seconds() / 3600
        if age_hours > self.export_max_age_hours:
            return Anomaly(
                type="EXPORT_STALE",
                severity=Severity.BLOCKING,
                message=(
                    f"Dernier export payout Mews livré il y a {age_hours:.0f}h "
                    f"(seuil {self.export_max_age_hours}h) — livraison webhook "
                    f"probablement cassée (Mews ne retente pas). Re-tirer l'export "
                    f"manuellement côté Mews avant de re-runner."
                ),
                source_file=source_label,
                reservation_ref=None,
                details={"last_delivery": str(last_delivery)},
            )
        return None

    def _check_payout_balance(
        self, batch: MewsPayoutBatch, source_label: str, anomalies: list,
    ) -> None:
        tx_net = sum(r.net for r in batch.reservations)
        if abs(tx_net - batch.payout_net) > Decimal("0.02"):
            anomalies.append(Anomaly(
                type="PAYOUT_UNBALANCED",
                severity=Severity.BLOCKING,
                message=(
                    f"Payout {batch.payout_id} ({batch.payout_date}) : net versé "
                    f"{batch.payout_net}€ ≠ Σ transactions {tx_net}€ — document "
                    f"Transactions incomplet, rien n'est posté."
                ),
                source_file=source_label,
                reservation_ref=batch.payout_id,
                details={
                    "payout_net": str(batch.payout_net),
                    "tx_net": str(tx_net),
                    "n_tx": str(len(batch.reservations)),
                },
            ))
