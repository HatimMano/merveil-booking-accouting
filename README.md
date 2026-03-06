# merveil-booking-accounting

Transforme les exports CSV de l'extranet Booking.com en écritures comptables importables dans PennyLane, avec un rapport de validation optionnel dans Google Sheets.

## Installation

```bash
pip install -r requirements.txt
```

Python 3.11+ requis.

## Utilisation

```bash
# Usage de base
python main.py --input ./csv_booking/ --date 2025-10-03

# Avec rapport Google Sheets
python main.py --input ./csv_booking/ --date 2025-10-03 --sheet-id "1abc...xyz"

# Sans Google Sheets (CSV uniquement)
python main.py --input ./csv_booking/ --date 2025-10-03 --no-sheets

# Dry-run (validation sans export)
python main.py --input ./csv_booking/ --date 2025-10-03 --dry-run

# Aide
python main.py --help
```

### Paramètres

| Paramètre | Défaut | Description |
|-----------|--------|-------------|
| `--input` | *(requis)* | Dossier contenant les CSV Booking.com |
| `--date` | *(requis)* | Date de traitement au format YYYY-MM-DD |
| `--output` | `./output/pennylane_booking_{date}.csv` | Chemin du CSV PennyLane en sortie |
| `--mapping` | `config/mapping/CodeAppart_Compta.csv` | Table de mapping appartements |
| `--sheet-id` | — | ID du Google Sheet de validation |
| `--service-account` | `service_account.json` | Clé du service account GCP |
| `--no-sheets` | — | Désactiver Google Sheets |
| `--dry-run` | — | Valider sans générer de fichiers |

## Format des fichiers d'entrée

Les CSV Booking.com doivent être nommés `{ref_appart}-{payout_id}.csv` (ex: `3015679-7oaOsO2VGKHbvBNQ.csv`). Le `ref_appart` est utilisé pour le lookup dans la table de mapping — il n'est **pas** dans le CSV lui-même.

## Format de sortie PennyLane

```
Journal,Date,Réf. pièce,Compte,Libellé,Débit,Crédit
BOOK,03/10/2025,,51105000,Encaissement BOOKING - 01/10/2025,427.42,
BOOK,03/10/2025,,401BOOKING,Frais + Payment Charge - 01/10/2025,,96.38
BOOK,03/10/2025,,411BOOKING,MER21-0G - BOOKING - Momin Bashir - CO:01/09/2025,544.60,
BOOK,03/10/2025,,401BOOKING,MER21-0G - 3015679 - FEE BOOKING - Momin Bashir - CO:01/09/2025,,96.38
```

## Anomalies détectées

### Passe 1 — validation des données source

| Code | Sévérité | Description |
|------|----------|-------------|
| `FILE_BAD_NAME` | BLOQUANTE | Nom de fichier invalide |
| `FILE_EMPTY` | WARNING | Fichier vide |
| `MAPPING_NOT_FOUND` | BLOQUANTE | Ref appartement absent du mapping |
| `DUPLICATE_RESERVATION` | WARNING | Même numéro de réservation dans plusieurs fichiers |
| `NON_EUR_CURRENCY` | BLOQUANTE | Devise autre que EUR |
| `CANCELLED_WITH_AMOUNT` | WARNING | Réservation annulée avec un montant non nul |
| `NON_RESERVATION_TYPE` | INFO | Ligne avec Type ≠ "Reservation" |

### Passe 2 — validation des résultats

| Code | Sévérité | Description |
|------|----------|-------------|
| `AMOUNT_MISMATCH` | WARNING | Net ≠ Amount + Commission + Fees + CityTax |
| `COMMISSION_RATE_HIGH` | WARNING | Commission > 20 % du montant |
| `COMMISSION_RATE_LOW` | WARNING | Commission < 10 % du montant |
| `BALANCE_ERROR` | BLOQUANTE | Déséquilibre global (tolérance ±0,05 €) |

## Tests

```bash
pytest tests/ -v
```

## Architecture

```
booking_accounting/
├── config/
│   ├── settings.py          # Constantes (comptes, seuils, formats)
│   ├── mapping_loader.py    # Chargement de la table de mapping
│   └── mapping/
│       └── CodeAppart_Compta.csv
├── parsers/
│   ├── base.py              # OTAParser — interface abstraite
│   └── booking.py           # BookingParser (Booking.com)
├── models/
│   └── reservation.py       # Dataclass Reservation (données normalisées)
├── accounting/
│   ├── entries.py           # Génération des écritures
│   └── pennylane.py         # Export CSV PennyLane
├── validators/
│   └── anomalies.py         # Détection d'anomalies (2 passes)
├── reports/
│   └── google_sheets.py     # Rapport Google Sheets
├── main.py                  # CLI (click)
├── requirements.txt
└── tests/
    ├── conftest.py
    ├── test_parser.py
    ├── test_accounting.py
    ├── test_validators.py
    └── fixtures/            # CSV de test
```

Pour ajouter un nouveau canal (Airbnb, Expedia…) : créer `parsers/airbnb.py` héritant de `OTAParser`. Le reste du pipeline (accounting, validators, reports) fonctionne sans modification.

## Google Sheets — rapport de validation

Le Google Sheet doit être créé manuellement et partagé avec l'email du service account GCP. Le script met à jour trois onglets à chaque exécution :

- **Résumé** — métriques clés (fichiers traités, montants, anomalies, équilibre)
- **Détail** — une ligne par réservation
- **Anomalies** — une ligne par anomalie détectée
