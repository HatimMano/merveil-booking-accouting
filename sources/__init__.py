"""
Sources de données pour le pipeline compta.

Chaque Source produit des `SourceFetchResult` (= batches de réservations + mapping
+ métadonnées d'archivage) que l'orchestrator transforme en écritures PennyLane.

Ajouter une nouvelle source = écrire une classe qui hérite de Source dans ce package,
sans toucher au reste du code.
"""

from .base import Source, SourceFetchResult
from .booking import BookingDriveSource
from .airbnb import AirbnbDriveSource

__all__ = ["Source", "SourceFetchResult", "BookingDriveSource", "AirbnbDriveSource"]
