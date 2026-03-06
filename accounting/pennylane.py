"""Export accounting entries to the PennyLane CSV import format."""

import csv
import logging
from decimal import Decimal
from pathlib import Path
from typing import List, Optional

from accounting.entries import AccountingEntry

logger = logging.getLogger(__name__)

# Column headers as expected by PennyLane
PENNYLANE_HEADERS = [
    "Journal",
    "Date",
    "Réf. pièce",
    "Compte",
    "Libellé",
    "Débit",
    "Crédit",
]


def _fmt_amount(amount: Optional[Decimal]) -> str:
    """
    Format a Decimal amount for PennyLane.

    Returns a string with 2 decimal places and a dot separator,
    or an empty string if amount is None.
    """
    if amount is None:
        return ""
    return f"{amount:.2f}"


def export_to_csv(entries: List[AccountingEntry], output_path: Path) -> None:
    """
    Write accounting entries to a PennyLane-compatible CSV file.

    The output file is UTF-8, comma-separated, with UNIX line endings.
    Debit and Credit columns use a dot as the decimal separator and are
    left empty (not "0.00") when not applicable.

    Args:
        entries:     List of AccountingEntry objects to write.
        output_path: Destination file path. Parent directories are created
                     automatically.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(PENNYLANE_HEADERS)

        for entry in entries:
            writer.writerow([
                entry.journal,
                entry.date.strftime("%d/%m/%Y"),
                entry.ref_piece,
                entry.account,
                entry.label,
                _fmt_amount(entry.debit),
                _fmt_amount(entry.credit),
            ])

    logger.info("Wrote %d entries to %s", len(entries), output_path)
