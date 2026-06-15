"""Lookup BQ : OTA reservation ref -> Mews bill (id + Number) avec matching
par proximite de montant.

Chantier 2 du rapprochement Mews x PennyLane. Depuis le ChannelNumber OTA
(BookingID, AirbnbCode, ...) stocke dans stg_mews__reservations, on remonte
au bill Mews dont le total absolu est le plus proche du `gross` payout
recu de l'OTA. Cette heuristique est plus robuste que "Closed prioritaire"
qui choisissait parfois un bill annexe (REBATE, taxe) au lieu du bill
principal (chambre).

Pivot : ChannelNumber == ota_reservation_ref (cote booking-pipeline).
Le caller passe (ota_ref, gross_amount) — le gross sert au matching.
"""

import logging
from typing import Dict, Iterable, Optional, Tuple

from google.cloud import bigquery

logger = logging.getLogger(__name__)

_PROJECT = "merveil-data-warehouse"

# Lookup batch : pour chaque (ota_ref, gross), trouver le bill dont le total
# absolu est le plus proche du gross. Tie-breakers (par ordre) :
#   1. distance abs(bill_total - gross) la plus petite
#   2. accounting_state='Closed' prioritaire (= bill comptablement clos)
#   3. consumed_at le plus recent
#
# Filtre `having abs(sum) >= 1` : on ignore les bills "vides" (items qui se
# compensent a 0) pour eviter les faux match avec des petits postings.
_LOOKUP_QUERY = """
WITH targets AS (
    SELECT ota_ref, gross
    FROM UNNEST(@targets) AS t
),
res_map AS (
    SELECT r.channel_number AS ota_ref, r.reservation_id
    FROM `{project}.staging.stg_mews__reservations` r
    WHERE r.channel_number IN (SELECT ota_ref FROM targets)
),
bill_totals AS (
    SELECT
        rm.ota_ref,
        oi.bill_id,
        ABS(SUM(oi.amount_gross)) AS bill_ttc_abs,
        MAX(CASE WHEN oi.accounting_state = 'Closed' THEN 1 ELSE 0 END) AS has_closed_items,
        MAX(oi.consumed_at) AS last_consumed
    FROM res_map rm
    INNER JOIN `{project}.staging.stg_mews__order_items` oi
        ON oi.reservation_id = rm.reservation_id
    WHERE oi.bill_id IS NOT NULL AND NOT oi.is_canceled
    GROUP BY rm.ota_ref, oi.bill_id
    HAVING ABS(SUM(oi.amount_gross)) >= 1
),
bill_ranked AS (
    SELECT
        t.ota_ref,
        bt.bill_id,
        ROW_NUMBER() OVER (
            PARTITION BY t.ota_ref
            ORDER BY
                ABS(bt.bill_ttc_abs - t.gross) ASC,
                bt.has_closed_items DESC,
                bt.last_consumed DESC
        ) AS rn
    FROM targets t
    INNER JOIN bill_totals bt ON bt.ota_ref = t.ota_ref
)
SELECT
    br.ota_ref,
    br.bill_id,
    b.bill_number
FROM bill_ranked br
LEFT JOIN `{project}.staging.stg_mews__bills` b ON b.bill_id = br.bill_id
WHERE br.rn = 1
""".format(project=_PROJECT)


def lookup_bills(
    targets: Iterable[Tuple[str, float]],
) -> Dict[str, Tuple[Optional[str], Optional[str]]]:
    """
    Resout chaque (ota_ref, gross_amount) vers le meilleur bill Mews matchant.

    Args:
        targets: Iterable de (ota_reference_OTA, montant_TTC_attendu).
                 Le gross sert au matching par proximite. Doublons et None
                 dans ota_ref geres proprement.

    Returns:
        Dict ota_ref -> (bill_id, bill_number). Si bill_number est NULL,
        c'est que le bill existe cote items mais pas dans stg_mews__bills
        (ex: bill Open pre-polling UpdatedUtc).
        Refs sans aucun match n'apparaissent pas dans le dict.
    """
    # Deduplique par (ota_ref, gross) — un meme ota_ref peut avoir plusieurs
    # postings (rare mais possible) avec des gross differents.
    seen: Dict[Tuple[str, float], None] = {}
    for ota_ref, gross in targets:
        if ota_ref and gross is not None:
            seen[(ota_ref, float(gross))] = None
    if not seen:
        return {}

    logger.info("Bill lookup: %d targets (ota_ref, gross) distincts", len(seen))

    client = bigquery.Client(project=_PROJECT)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter(
                "targets",
                bigquery.StructQueryParameterType(
                    bigquery.ScalarQueryParameterType("STRING", name="ota_ref"),
                    bigquery.ScalarQueryParameterType("FLOAT64", name="gross"),
                ),
                [
                    bigquery.StructQueryParameter(
                        None,
                        bigquery.ScalarQueryParameter("ota_ref", "STRING", ota_ref),
                        bigquery.ScalarQueryParameter("gross", "FLOAT64", gross),
                    )
                    for (ota_ref, gross) in seen
                ],
            ),
        ]
    )
    rows = client.query(_LOOKUP_QUERY, job_config=job_config).result()

    out: Dict[str, Tuple[Optional[str], Optional[str]]] = {}
    for row in rows:
        out[row["ota_ref"]] = (row["bill_id"], row["bill_number"])

    matched = sum(1 for _, num in out.values() if num is not None)
    logger.info(
        "Bill lookup: %d/%d refs matched (%d avec bill_number, %d sans)",
        len(out), len(set(k[0] for k in seen)), matched, len(out) - matched,
    )
    return out
