"""Source abstract base — interface commune entre les pipelines.

L'orchestrator ne dépend que de cette interface. Pour ajouter un nouveau flux
(ex: Mews Bills, Mews Payments), il suffit d'implémenter une nouvelle Source
sans toucher au reste du pipeline.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date
from typing import Any


@dataclass
class SourceFetchResult:
    """Données prêtes à être consommées par l'orchestrator.

    Une Source produit ce résultat à partir de son origine (Drive xlsx, BQ query, etc.).
    L'orchestrator est ensuite agnostique à la provenance.
    """

    batches: list                                  # list[PayoutBatch] — batches de réservations
    anomalies: list                                # list[Anomaly] — anomalies détectées au parsing
    mapping: dict                                  # logement/code → code_comptable
    source_file: str                               # libellé tracé en BQ (= nom du fichier ou requête)
    archive_file_ids: list = field(default_factory=list)  # IDs Drive à déplacer dans Archive (vide si non-Drive)
    drive_folder_id: str = ""                      # Dossier Drive racine (vide si la source n'utilise pas Drive)
    file_hash: str = ""                            # md5 du contenu source — clé du journal d'idempotence


class Source(ABC):
    """Abstract base class pour toutes les sources de données du pipeline compta.

    Chaque sous-classe implémente :
      - `name` : identifiant court ('booking', 'airbnb', 'mews-bills', 'mews-payments')
      - `entries_kwargs` : kwargs à passer à `generate_entries()` (journal, comptes, label OTA, etc.)
      - `fetch(date)` : récupère les batches + anomalies + mapping
      - `enrich_anomalies(result)` : hook optionnel pour enrichir les anomalies (no-op par défaut)
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Identifiant court de la source (utilisé pour les logs, l'archivage, BQ trace)."""

    @property
    @abstractmethod
    def entries_kwargs(self) -> dict[str, Any]:
        """Kwargs passés à `generate_entries()` — diffèrent selon le canal d'encaissement."""

    @abstractmethod
    def fetch(self, processing_date: date) -> SourceFetchResult:
        """Récupère les données brutes et les transforme en batches prêts à consommer."""

    def enrich_anomalies(self, result: SourceFetchResult) -> None:
        """Hook optionnel pour enrichir les anomalies (libellés PennyLane, codes appart…).

        Par défaut : no-op. À override si la source a besoin d'ajouter du contexte
        aux anomalies après le parsing initial.
        """
        return
