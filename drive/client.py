"""Google Drive API client — download CSVs from a folder, upload the output Excel."""

import io
import logging
import os
from pathlib import Path
from typing import List, Optional, Tuple

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

logger = logging.getLogger(__name__)

_SCOPES = ["https://www.googleapis.com/auth/drive"]

# MIME types
_MIME_CSV    = "text/csv"
_MIME_XLSX   = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
_MIME_GSHEET = "application/vnd.google-apps.spreadsheet"
_MIME_FOLDER = "application/vnd.google-apps.folder"


# ---------------------------------------------------------------------------
# DriveClient
# ---------------------------------------------------------------------------

class DriveClient:
    """
    Thin wrapper around the Google Drive API v3.

    Authentication uses a service account JSON key file.
    In Cloud Run the key path defaults to the ADC environment variable
    (GOOGLE_APPLICATION_CREDENTIALS); locally you pass the path explicitly.
    """

    def __init__(self, service_account_file: Optional[str] = None) -> None:
        """
        Args:
            service_account_file: Path to the service account JSON key file.
                                  Falls back to the GOOGLE_APPLICATION_CREDENTIALS
                                  environment variable, then to Application Default
                                  Credentials (ADC) if neither is supplied.
        """
        key_file = service_account_file or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        if key_file:
            creds = service_account.Credentials.from_service_account_file(
                key_file, scopes=_SCOPES
            )
        else:
            # Running inside Cloud Run / GCE — use ADC
            from google.auth import default as google_auth_default
            creds, _ = google_auth_default(scopes=_SCOPES)

        self._service = build("drive", "v3", credentials=creds, cache_discovery=False)
        logger.info("Google Drive client initialised.")

    # -----------------------------------------------------------------------
    # Listing
    # -----------------------------------------------------------------------

    def list_csv_files(self, folder_id: str) -> List[dict]:
        """
        Return metadata for all CSV files in *folder_id*.

        Returns a list of dicts with keys: id, name, createdTime, modifiedTime.
        """
        query = (
            f"'{folder_id}' in parents"
            f" and mimeType='{_MIME_CSV}'"
            f" and trashed=false"
        )
        results = (
            self._service.files()
            .list(
                q=query,
                fields="files(id, name, createdTime, modifiedTime)",
                orderBy="name",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )
        files = results.get("files", [])
        logger.info("Found %d CSV file(s) in Drive folder %s", len(files), folder_id)
        return files

    def list_excel_files(self, folder_id: str) -> List[dict]:
        """
        Return metadata for spreadsheet files in *folder_id*.

        Returns both native .xlsx files and Google Sheets documents,
        ordered by most recently modified first.
        Each dict has keys: id, name, modifiedTime, mimeType.
        """
        query = (
            f"'{folder_id}' in parents"
            f" and (mimeType='{_MIME_XLSX}' or mimeType='{_MIME_GSHEET}')"
            f" and trashed=false"
        )
        results = (
            self._service.files()
            .list(
                q=query,
                fields="files(id, name, modifiedTime, mimeType)",
                orderBy="modifiedTime desc",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )
        return results.get("files", [])

    # -----------------------------------------------------------------------
    # Download
    # -----------------------------------------------------------------------

    def download_file(self, file_id: str, dest_path: Path, mime_type: Optional[str] = None) -> None:
        """
        Download a Drive file by ID to *dest_path*.

        If *mime_type* is _MIME_GSHEET (Google Sheets), the file is exported
        as .xlsx automatically.  For all other types, a standard binary download
        is performed.
        """
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        if mime_type == _MIME_GSHEET:
            request = self._service.files().export_media(
                fileId=file_id, mimeType=_MIME_XLSX
            )
            logger.debug("Exporting Google Sheet %s → %s", file_id, dest_path)
        else:
            request = self._service.files().get_media(fileId=file_id, supportsAllDrives=True)
            logger.debug("Downloading %s → %s", file_id, dest_path)

        with open(dest_path, "wb") as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()

    def download_all_csvs(self, folder_id: str, dest_dir: Path) -> List[Path]:
        """
        Download all CSV files from *folder_id* into *dest_dir*.

        Returns the list of local file paths.
        """
        dest_dir.mkdir(parents=True, exist_ok=True)
        csv_files = self.list_csv_files(folder_id)
        local_paths: List[Path] = []
        for f in csv_files:
            dest = dest_dir / f["name"]
            self.download_file(f["id"], dest)
            local_paths.append(dest)
        logger.info("Downloaded %d CSV(s) to %s", len(local_paths), dest_dir)
        return local_paths

    # -----------------------------------------------------------------------
    # Upload
    # -----------------------------------------------------------------------

    def upload_file(
        self,
        local_path: Path,
        folder_id: str,
        mime_type: str = _MIME_XLSX,
        existing_file_id: Optional[str] = None,
    ) -> str:
        """
        Upload *local_path* to *folder_id*.

        If *existing_file_id* is given, the file is updated in-place (same Drive
        URL/ID preserved).  Otherwise a new file is created.

        Returns the Drive file ID of the created/updated file.
        """
        # supportsAllDrives=True is required to write into Shared Drives
        media = MediaFileUpload(str(local_path), mimetype=mime_type, resumable=True)

        if existing_file_id:
            result = (
                self._service.files()
                .update(
                    fileId=existing_file_id,
                    media_body=media,
                    supportsAllDrives=True,
                )
                .execute()
            )
            logger.info(
                "Updated existing Drive file %s (%s)", existing_file_id, local_path.name
            )
        else:
            metadata = {"name": local_path.name, "parents": [folder_id]}
            result = (
                self._service.files()
                .create(
                    body=metadata,
                    media_body=media,
                    fields="id",
                    supportsAllDrives=True,
                )
                .execute()
            )
            logger.info(
                "Uploaded new Drive file %s → id=%s", local_path.name, result["id"]
            )
        return result["id"]

    def get_or_create_folder(self, parent_id: str, name: str) -> str:
        """
        Return the Drive ID of a subfolder named *name* inside *parent_id*.
        Creates it if it does not exist.  Works with Shared Drives.
        """
        query = (
            f"'{parent_id}' in parents"
            f" and name='{name}'"
            f" and mimeType='{_MIME_FOLDER}'"
            f" and trashed=false"
        )
        results = (
            self._service.files()
            .list(
                q=query,
                fields="files(id, name)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
            .get("files", [])
        )
        if results:
            folder_id = results[0]["id"]
            logger.debug("Found existing subfolder '%s' → %s", name, folder_id)
            return folder_id

        # Create the subfolder
        metadata = {
            "name": name,
            "mimeType": _MIME_FOLDER,
            "parents": [parent_id],
        }
        folder = (
            self._service.files()
            .create(body=metadata, fields="id", supportsAllDrives=True)
            .execute()
        )
        logger.info("Created subfolder '%s' → %s", name, folder["id"])
        return folder["id"]

    def upload_excel(self, local_path: Path, folder_id: str, filename: str) -> str:
        """
        Upload *local_path* as *filename* to *folder_id*.

        Works with both personal Drive folders (when the SA has been granted
        write access via domain-wide delegation) and Shared Drive folders
        (recommended — the SA is added as a member of the Shared Drive).

        If a file with the same name already exists in the folder it is replaced
        (same Drive ID preserved — no duplicate files accumulate).
        """
        # supportsAllDrives + includeItemsFromAllDrives needed for Shared Drives
        query = (
            f"'{folder_id}' in parents"
            f" and name='{filename}'"
            f" and mimeType='{_MIME_XLSX}'"
            f" and trashed=false"
        )
        existing = (
            self._service.files()
            .list(
                q=query,
                fields="files(id, name)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
            .get("files", [])
        )
        existing_id = existing[0]["id"] if existing else None
        return self.upload_file(local_path, folder_id, existing_file_id=existing_id)
