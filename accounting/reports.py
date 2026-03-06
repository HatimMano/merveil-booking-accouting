"""Local report exports: anomaly CSV and verification matrix."""

import csv
import json
import logging
from decimal import Decimal
from pathlib import Path
from typing import List

from models.reservation import Reservation
from validators.anomalies import Anomaly

logger = logging.getLogger(__name__)


def export_anomalies_csv(anomalies: List[Anomaly], output_path: Path) -> None:
    """
    Export all anomalies to a CSV file.

    Columns: Severity, Type, Message, Source File, Reservation Ref, Details
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Severity",
            "Type",
            "Message",
            "Source File",
            "Reservation Ref",
            "Details",
        ])
        for a in anomalies:
            writer.writerow([
                a.severity,
                a.type,
                a.message,
                a.source_file,
                a.reservation_ref or "",
                json.dumps(a.details, ensure_ascii=False) if a.details else "",
            ])
    logger.info(
        "Anomaly report written to: %s (%d anomaly/ies)",
        output_path, len(anomalies),
    )


def export_verification_matrix(
    reservations: List[Reservation], output_path: Path
) -> None:
    """
    Export the verification matrix (matrice de vérification).

    One row per apartment, sorted by ID Appartement, with:
      - ID Appartement  (code_comptable, e.g. "MER21-0G")
      - Ref Appart      (Booking.com ref, e.g. "3015679")
      - SUM Débit       (sum of Amount   — the 411BOOKING debit lines)
      - SUM Crédit      (sum of |Commission| + |Payment_charge| — the 401BOOKING credit lines)
      - SUM Net         (sum of Net)

    Ends with a TOTAL row.
    """
    # Group by (code_comptable, ref_appart)
    groups: dict = {}
    for r in reservations:
        key = (r.code_comptable or "", r.ref_appart)
        if key not in groups:
            groups[key] = {
                "debit": Decimal("0"),
                "credit": Decimal("0"),
                "net": Decimal("0"),
            }
        groups[key]["debit"] += r.amount
        groups[key]["credit"] += abs(r.commission) + abs(r.payment_charge)
        groups[key]["net"] += r.net

    sorted_groups = sorted(groups.items(), key=lambda x: x[0][0])

    total_debit = Decimal("0")
    total_credit = Decimal("0")
    total_net = Decimal("0")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "ID Appartement",
            "Ref Appart",
            "SUM Débit",
            "SUM Crédit",
            "SUM Net",
        ])
        for (code_comptable, ref_appart), totals in sorted_groups:
            writer.writerow([
                code_comptable,
                ref_appart,
                f"{totals['debit']:.2f}",
                f"{totals['credit']:.2f}",
                f"{totals['net']:.2f}",
            ])
            total_debit += totals["debit"]
            total_credit += totals["credit"]
            total_net += totals["net"]

        # TOTAL row
        writer.writerow([
            "TOTAL",
            "",
            f"{total_debit:.2f}",
            f"{total_credit:.2f}",
            f"{total_net:.2f}",
        ])

    logger.info(
        "Verification matrix written to: %s (%d apartment(s))",
        output_path, len(sorted_groups),
    )
