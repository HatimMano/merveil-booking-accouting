"""
Flask HTTP server pour Google Cloud Run.

Routes :
  - `GET /health` : ping
  - `POST /process` : déclenche un run pour l'OTA donné

Body attendu (POST /process) :
    {
        "folder_id": "1abc...xyz",    // Drive folder (default = config par OTA)
        "date":      "2026-05-11",    // YYYY-MM-DD ou "AUTO"
        "ota":       "booking",       // "booking" ou "airbnb"
        "dry_run":   false,           // optionnel
        "test":      false,           // optionnel (préfixe [TEST])
        "bq_only":   false,           // optionnel (skip PennyLane)
        "run_id":    "uuid",          // optionnel (auto-généré)
        "force":     false            // optionnel — ignore le journal d'idempotence
                                      // en lecture (re-poste TOUT). Uniquement après
                                      // nettoyage manuel Pennylane d'un état incertain.
    }

Le serveur ne fait que :
  1. Parser le body
  2. Instancier la bonne Source
  3. Déléguer à `orchestrator.run_pipeline()`

Toute la logique métier est dans `orchestrator.py` + `sources/*.py`.
"""

import logging
import os
import sys
import tempfile
import uuid
from datetime import datetime
from pathlib import Path

from flask import Flask, jsonify, request

sys.path.insert(0, str(Path(__file__).parent))

from config.settings import DRIVE_FOLDER_BOOKING, DRIVE_FOLDER_AIRBNB
from drive.client import DriveClient
from orchestrator import run_pipeline
from sources import AirbnbDriveSource, BookingDriveSource, MewsPaymentsSource

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

_BOOKING_MAPPING_PATH = Path(__file__).parent / "config" / "mapping" / "CodeAppart_Compta.csv"
_AIRBNB_MAPPING_PATH  = Path(__file__).parent / "config" / "mapping" / "AirbnbLogement_Compta.csv"

# Registry Source : ajouter une entrée ici quand on ajoute une nouvelle Source.
# Chaque factory prend (drive_client, folder_id, tmp_dir) et retourne une Source.
_SOURCE_FACTORIES = {
    "booking": lambda drive, folder_id, tmp: BookingDriveSource(drive, folder_id, _BOOKING_MAPPING_PATH, tmp),
    "airbnb":  lambda drive, folder_id, tmp: AirbnbDriveSource(drive, folder_id, _AIRBNB_MAPPING_PATH, tmp),
    # Source BQ (flux 2) — pas de Drive : folder_id/tmp ignorés
    "mews-payments": lambda drive, folder_id, tmp: MewsPaymentsSource(),
}

_DEFAULT_FOLDERS = {
    "booking": DRIVE_FOLDER_BOOKING,
    "airbnb":  DRIVE_FOLDER_AIRBNB,
    "mews-payments": "",
}


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


@app.route("/process", methods=["POST"])
def process():
    body = request.get_json(force=True, silent=True) or {}
    ota       = body.get("ota", "booking")
    test_mode = bool(body.get("test", False))
    dry_run   = bool(body.get("dry_run", False))
    bq_only   = bool(body.get("bq_only", False))
    force     = bool(body.get("force", False))
    date_str  = body.get("date")
    run_id    = body.get("run_id") or str(uuid.uuid4())

    if ota not in _SOURCE_FACTORIES:
        supported = ", ".join(_SOURCE_FACTORIES.keys())
        return jsonify({"error": f"Unsupported OTA '{ota}'. Supported: {supported}."}), 400

    folder_id = body.get("folder_id") or _DEFAULT_FOLDERS[ota]

    if not date_str or date_str == "AUTO":
        import zoneinfo
        date_str = datetime.now(zoneinfo.ZoneInfo("Europe/Paris")).strftime("%Y-%m-%d")

    try:
        processing_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        return jsonify({"error": f"Invalid date format '{date_str}'. Expected YYYY-MM-DD"}), 400

    logger.info(
        "Processing request: run_id=%s folder_id=%s date=%s ota=%s bq_only=%s test_mode=%s dry_run=%s force=%s",
        run_id, folder_id, date_str, ota, bq_only, test_mode, dry_run, force,
    )

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            drive = DriveClient()
            source = _SOURCE_FACTORIES[ota](drive, folder_id, Path(tmpdir))
            result = run_pipeline(
                source=source,
                processing_date=processing_date,
                drive_client=drive,
                test_mode=test_mode,
                dry_run=dry_run,
                bq_only=bq_only,
                run_id=run_id,
                force=force,
            )
    except Exception as exc:
        logger.exception("Pipeline failed: %s", exc)
        return jsonify({"error": str(exc), "run_id": run_id}), 500

    result["run_id"] = run_id
    return jsonify(result), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
