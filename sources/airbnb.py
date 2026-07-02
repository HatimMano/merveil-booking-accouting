"""Source Airbnb — lit l'export Excel mensuel Airbnb depuis Drive."""

import hashlib
import logging
from datetime import date
from pathlib import Path
from typing import Any

from config.mapping_loader import load_airbnb_mapping
from config.settings import (
    AIRBNB_JOURNAL_CODE,
    AIRBNB_ACCOUNT_BANK,
    AIRBNB_ACCOUNT_CLIENT,
    AIRBNB_ACCOUNT_SUPPLIER,
    AIRBNB_ACCOUNT_CANCELLATION_FEE,
)
from parsers.airbnb import AirbnbParser
from .base import Source, SourceFetchResult

logger = logging.getLogger(__name__)


class AirbnbDriveSource(Source):
    name = "airbnb"

    @property
    def entries_kwargs(self) -> dict[str, Any]:
        return {
            "journal_code": AIRBNB_JOURNAL_CODE,
            "account_bank": AIRBNB_ACCOUNT_BANK,
            "account_client": AIRBNB_ACCOUNT_CLIENT,
            "account_supplier": AIRBNB_ACCOUNT_SUPPLIER,
            "account_cancellation_fee": AIRBNB_ACCOUNT_CANCELLATION_FEE,
            "ota_label": "AIRBNB",
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
                f"{len(xlsx_files)} fichiers xlsx trouvés dans le dossier ({names}) — déposez un seul fichier à la fois."
            )

        xlsx_meta = xlsx_files[0]
        local_name = xlsx_meta["name"] if xlsx_meta["name"].endswith(".xlsx") else xlsx_meta["name"] + ".xlsx"
        local_xlsx = self.tmp_dir / local_name
        self.drive.download_file(xlsx_meta["id"], local_xlsx, mime_type=xlsx_meta.get("mimeType"))
        logger.info("Downloaded Airbnb file: %s (type: %s)", xlsx_meta["name"], xlsx_meta.get("mimeType"))

        mapping = load_airbnb_mapping(self.mapping_path)

        parser = AirbnbParser()
        batches, anomalies = parser.parse_into_batches(local_xlsx)

        return SourceFetchResult(
            batches=batches,
            anomalies=anomalies,
            mapping=mapping,
            source_file=xlsx_meta["name"],
            archive_file_ids=[xlsx_meta["id"]],
            drive_folder_id=self.folder_id,
            file_hash=hashlib.md5(local_xlsx.read_bytes()).hexdigest(),
        )

    def enrich_anomalies(self, result: SourceFetchResult) -> None:
        # NON_EUR_CURRENCY : enrichit avec code_comptable + libellé PennyLane prêt à coller.
        # Permet à l'expert-comptable de saisir manuellement les non-EUR depuis la sheet Anomalies.
        for a in result.anomalies:
            if a.type == "NON_EUR_CURRENCY":
                logement = a.details.get("logement", "")
                code = result.mapping.get(logement, logement)
                checkout = a.details.get("checkout_date", "")
                voyageur = a.details.get("voyageur", "")
                row_type = a.details.get("row_type", "")
                ref = a.reservation_ref or ""
                a.details["code_comptable"] = code
                a.details["label_pennylane"] = (
                    f"{code} - AIRBNB - CO : {checkout} - {voyageur} - {row_type} - {ref}"
                )
