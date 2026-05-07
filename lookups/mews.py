"""Lookup BQ : OTA reservation ref -> Mews bill (id + Number).

Chantier 2 du rapprochement Mews x PennyLane. Depuis le ChannelNumber OTA
(BookingID, AirbnbCode, ...) stocke dans stg_mews__reservations, on remonte
au bill Mews et son Number officiel pour enrichir les ecritures PennyLane
(`ref_piece`).

Pivot : ChannelNumber == ota_reservation_ref (cote booking-pipeline).
"""

import logging
from typing import Dict, List, Optional, Tuple

from google.cloud import bigquery

logger = logging.getLogger(__name__)

_PROJECT = "merveil-data-warehouse"

# Une seule requete batch : on passe la liste complete des refs OTA et on
# recupere d'un coup les bills associes. Plus efficace que N requetes.
#
# Strategie de matching quand 1 resa a plusieurs bills (cas rare = corrections
# Mews) : on privilegie le bill Closed le plus recent, qui est le bill
# comptablement "vivant" (= equivalent is_latest_in_chain dans int_compta__bills_net).
#
# is_correction est ignore volontairement : si le seul bill disponible est une
# correction (= bill original non encore importe parce que pre-backfill), c'est
# quand meme la meilleure reference qu'on a.
_LOOKUP_QUERY = """
WITH res_map AS (
    SELECT
        r.channel_number AS ota_ref,
        r.reservation_id
    FROM `{project}.staging.stg_mews__reservations` r
    WHERE r.channel_number IN UNNEST(@ota_refs)
),
bill_per_resa AS (
    SELECT
        rm.ota_ref,
        oi.bill_id,
        ROW_NUMBER() OVER (
            PARTITION BY rm.ota_ref
            ORDER BY
                CASE WHEN oi.accounting_state = 'Closed' THEN 0 ELSE 1 END,
                MAX(oi.consumed_at) DESC
        ) AS rn
    FROM res_map rm
    INNER JOIN `{project}.staging.stg_mews__order_items` oi
        ON oi.reservation_id = rm.reservation_id
    WHERE oi.bill_id IS NOT NULL
    GROUP BY rm.ota_ref, oi.bill_id, oi.accounting_state
)
SELECT
    bpr.ota_ref,
    bpr.bill_id,
    b.bill_number
FROM bill_per_resa bpr
LEFT JOIN `{project}.staging.stg_mews__bills` b
    ON b.bill_id = bpr.bill_id
WHERE bpr.rn = 1
""".format(project=_PROJECT)


def lookup_bills(ota_refs: List[str]) -> Dict[str, Tuple[Optional[str], Optional[str]]]:
    """
    Retourne un dict {ota_ref: (bill_id, bill_number)} pour chaque ref OTA en entree.

    Si une ref OTA n'a pas de bill matche (resa Mews introuvable, pas d'item lie,
    bill pas encore importe...), elle n'apparait PAS dans le dict retourne. Le
    caller est libre d'en deduire que ref_piece doit rester vide.

    Si bill_number est NULL cote BQ (cas tres rare : bill exists in
    raw_order_items.bill_id mais pas dans raw_bills), le tuple sera
    (bill_id, None) et on remplira bill_id_mews mais pas ref_piece.

    Args:
        ota_refs: Liste de references OTA (BookingID, AirbnbCode, ...). Peut
                  contenir des doublons et None — gere proprement.

    Returns:
        Dict ota_ref -> (bill_id, bill_number). Vide si pas de matching.
    """
    refs = sorted({r for r in ota_refs if r})
    if not refs:
        return {}

    logger.info("Bill lookup: %d ota_refs distinct", len(refs))

    client = bigquery.Client(project=_PROJECT)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("ota_refs", "STRING", refs),
        ]
    )
    rows = client.query(_LOOKUP_QUERY, job_config=job_config).result()

    out: Dict[str, Tuple[Optional[str], Optional[str]]] = {}
    for row in rows:
        out[row["ota_ref"]] = (row["bill_id"], row["bill_number"])

    matched = sum(1 for _, num in out.values() if num is not None)
    logger.info(
        "Bill lookup: %d/%d refs matched with a bill_number (%d with bill_id only)",
        matched, len(refs), len(out) - matched,
    )
    return out
