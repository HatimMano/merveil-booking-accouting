"""Abstract base class that all OTA parsers must implement."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Tuple

from models.reservation import Reservation
from validators.anomalies import Anomaly


class OTAParser(ABC):
    """
    Interface for parsing OTA (Online Travel Agency) CSV exports.

    Each OTA has its own CSV format, but every parser must return the same
    normalized Reservation objects.  The accounting, validation, and reporting
    modules are completely OTA-agnostic — they only depend on this contract.

    To add a new OTA (e.g. Airbnb, Expedia):
      1. Create parsers/airbnb.py
      2. Subclass OTAParser and implement parse_file() and parse_directory()
      3. The rest of the pipeline works without modification
    """

    @abstractmethod
    def parse_file(self, path: Path) -> Tuple[List[Reservation], List[Anomaly]]:
        """
        Parse a single OTA CSV file.

        Args:
            path: Path to the CSV file.

        Returns:
            A tuple (reservations, anomalies) where:
            - reservations: list of normalized Reservation objects (may be empty
              if the file is invalid or contains only non-reservation rows)
            - anomalies: list of Anomaly objects detected during parsing
        """
        ...

    @abstractmethod
    def parse_directory(self, path: Path) -> Tuple[List[Reservation], List[Anomaly]]:
        """
        Parse all OTA CSV files found in a directory.

        Args:
            path: Directory containing CSV files.

        Returns:
            A tuple (reservations, anomalies) aggregated across all files.
        """
        ...
