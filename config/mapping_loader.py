"""Loads apartment → accounting-code mappings for Booking.com and Airbnb."""

import csv
import logging
from pathlib import Path
from typing import Dict

logger = logging.getLogger(__name__)

import re

def _normalize_key(name: str) -> str:
    """Normalize whitespace around hyphens so 'A -B' == 'A - B' == 'A- B'."""
    return re.sub(r'\s*-\s*', ' - ', name.strip())


def load_mapping(path: Path) -> Dict[str, str]:
    """
    Load the CodeAppart_Compta mapping file.

    File format (semicolon-separated, UTF-8 with optional BOM, CRLF):
      - Lines 1-4: title / header rows to skip
      - Line 5:    column headers ("Code Appartement;Code BOOKING Annonce;Code Comptable")
      - Lines 6+:  data rows

    Rows are skipped when:
      - The Booking code is empty
      - The Booking code is not purely numeric (e.g. "L'appartement n'est pas lancé")
      - The accounting code (Code Comptable) is empty

    Returns:
        Dict mapping Booking code (str) → accounting code (str).
        Example: {"3015679": "MER21-0G"}
    """
    mapping: Dict[str, str] = {}

    try:
        raw = path.read_bytes()
        # Decode with UTF-8-sig to strip the BOM if present
        content = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        content = path.read_bytes().decode("latin-1")

    # Normalise line endings
    lines = content.replace("\r\n", "\n").replace("\r", "\n").split("\n")

    # Lines 1-4 are titles/headers, line 5 is the column header row.
    # Data starts at line 6 → index 5 in 0-based list.
    data_lines = lines[5:]

    for line in data_lines:
        line = line.strip()
        if not line:
            continue

        parts = line.split(";")
        if len(parts) < 3:
            continue

        code_appart = parts[0].strip()
        code_booking = parts[1].strip()
        code_comptable = parts[2].strip()

        # Skip entries whose Booking code is empty or non-numeric
        if not code_booking or not code_booking.isdigit():
            if code_booking:
                logger.debug(
                    "Skipping '%s': Booking code '%s' is not numeric",
                    code_appart, code_booking,
                )
            continue

        # Skip entries with an empty accounting code
        if not code_comptable:
            logger.debug(
                "Skipping '%s' (Booking %s): empty accounting code",
                code_appart, code_booking,
            )
            continue

        mapping[code_booking] = code_comptable

    logger.info("Loaded %d mapping entries from %s", len(mapping), path)
    return mapping


def load_airbnb_mapping(path: Path) -> Dict[str, str]:
    """
    Load the AirbnbLogement_Compta mapping file.

    File format (comma-separated, UTF-8 with optional BOM):
      - Line 1: column headers ("Logement,CodeComptable")
      - Lines 2+: data rows

    Rows are skipped when:
      - The Logement name is empty
      - The accounting code (CodeComptable) is empty

    Returns:
        Dict mapping Airbnb listing name (str) → accounting code (str).
        Example: {"Merveil - Luxury Suite - Champs Elysées - Mermoz": "MER21-0G"}
    """
    mapping: Dict[str, str] = {}

    try:
        raw = path.read_bytes()
        content = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        content = path.read_bytes().decode("latin-1")

    lines = content.replace("\r\n", "\n").replace("\r", "\n").split("\n")

    # Line 1 is the header ("Logement,CodeComptable"); data starts at line 2
    data_lines = lines[1:]

    for line in data_lines:
        line = line.strip()
        if not line:
            continue

        parts = line.split(",")
        if len(parts) < 2:
            continue

        # The listing name may contain commas → join all parts except the last
        code_comptable = parts[-1].strip()
        logement = ",".join(parts[:-1]).strip()

        if not logement or not code_comptable:
            continue

        mapping[_normalize_key(logement)] = code_comptable

    logger.info("Loaded %d Airbnb mapping entries from %s", len(mapping), path)
    return mapping
