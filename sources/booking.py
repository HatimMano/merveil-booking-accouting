"""Source Booking — lit l'export Excel hebdomadaire Booking depuis Drive."""

import hashlib
import logging
from datetime import date
from pathlib import Path
from typing import Any

from config.mapping_loader import load_mapping
from config.settings import ACCOUNT_SUPPLIER
from parsers.booking import BookingExcelParser
from validators.anomalies import check_duplicate_reservations
from .base import Source, SourceFetchResult

logger = logging.getLogger(__name__)


class BookingDriveSource(Source):
    name = "booking"

    @property
    def entries_kwargs(self) -> dict[str, Any]:
        # Booking : commission éclatée par réservation (demande expert-comptable 2026-04-13)
        # + ajustements commission routés vers 401BOOKING (demande Philippe 2026-06-08)
        return {
            "per_reservation_fees": True,
            "account_commission_adjustment": ACCOUNT_SUPPLIER,
        }

    def __init__(self, drive_client, folder_id: str, mapping_path: Path, tmp_dir: Path):
        self.drive = drive_client
        self.folder_id = folder_id
        self.mapping_path = mapping_path
        self.tmp_dir = tmp_dir

    def fetch(self, processing_date: date) -> SourceFetchResult:
        xlsx_files = self.drive.list_excel_files(self.folder_id)
        if not xlsx_files:
            return SourceFetchResult(
                batches=[], anomalies=[], mapping={},
                source_file="", drive_folder_id=self.folder_id,
            )
        if len(xlsx_files) > 1:
            names = ", ".join(f["name"] for f in xlsx_files)
            raise ValueError(
                f"{len(xlsx_files)} fichiers xlsx trouvés ({names}) — déposez un seul fichier à la fois."
            )

        xlsx_meta = xlsx_files[0]
        local_name = xlsx_meta["name"] if xlsx_meta["name"].endswith(".xlsx") else xlsx_meta["name"] + ".xlsx"
        local_xlsx = self.tmp_dir / local_name
        self.drive.download_file(xlsx_meta["id"], local_xlsx, mime_type=xlsx_meta.get("mimeType"))
        logger.info("Downloaded Booking file: %s (type: %s)", xlsx_meta["name"], xlsx_meta.get("mimeType"))

        mapping = load_mapping(self.mapping_path)

        parser = BookingExcelParser()
        batches, anomalies = parser.parse_into_batches(local_xlsx)

        all_reservations = [r for b in batches for r in b.reservations]
        anomalies.extend(check_duplicate_reservations(all_reservations))

        return SourceFetchResult(
            batches=batches,
            anomalies=anomalies,
            mapping=mapping,
            source_file=xlsx_meta["name"],
            archive_file_ids=[xlsx_meta["id"]],
            drive_folder_id=self.folder_id,
            file_hash=hashlib.md5(local_xlsx.read_bytes()).hexdigest(),
        )
