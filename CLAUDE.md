# merveil-booking-accounting — Context & Evolution

## Purpose
Python pipeline running on **Cloud Run** that transforms OTA payment exports (Booking.com, Airbnb)
into PennyLane accounting entries, posted directly via API.

Triggered via HTTP POST by Cloud Scheduler (or manually).

## Infrastructure
- **Service**: `booking-pipeline` on Cloud Run, region `europe-west1`, project `merveil-data-warehouse`
- **Endpoint**: `POST /process`
- **Deploy**: `gcloud run services update --image eu.gcr.io/merveil-data-warehouse/booking-pipeline --region europe-west1 --project merveil-data-warehouse`
  - Always push to GitHub first — the image is built from GitHub source.
- **Secret**: `PENNYLANE_TOKEN` via Secret Manager

## Request format
```json
{
  "folder_id": "1abc...xyz",
  "ota": "booking",          // "booking" or "airbnb"
  "date": "2026-03-27",      // or "AUTO" for today (Paris timezone)
  "dry_run": true,           // optional — validate without posting or archiving
  "test": true               // optional — post with [TEST] prefix, no archiving
}
```

## Entrypoints
| File | Role |
|---|---|
| `server.py` | Flask HTTP server — main entrypoint for Cloud Run |
| `parsers/booking.py` | `BookingExcelParser` — parses weekly Booking Excel into `BookingPayoutBatch` objects |
| `parsers/airbnb.py` | `AirbnbParser` — parses monthly Airbnb Excel into payout batches |
| `accounting/entries.py` | `generate_entries()` — builds PennyLane accounting lines from reservations |
| `pennylane/client.py` | `PennyLaneClient` — posts batches to PennyLane API |
| `drive/client.py` | `DriveClient` — downloads xlsx, creates folders, moves files, creates Sheets |
| `config/settings.py` | Account codes, journal IDs, thresholds |
| `config/mapping_loader.py` | `load_mapping()` (Booking), `load_airbnb_mapping()` (Airbnb) |

## Mapping files
| File | Format | Key |
|---|---|---|
| `config/mapping/CodeAppart_Compta.csv` | semicolons, skip first 5 rows | Booking numeric ID → accounting code |
| `config/mapping/AirbnbLogement_Compta.csv` | comma-separated | Airbnb listing name → accounting code |
| `config/mapping/Mapping_appart_code.csv` | master file (semicolons) | col 2 = CodeComptable, col 7 = Airbnb listing name |

## Input format — Booking (Excel)
Flat weekly Excel export from Booking.com extranet. One row per reservation.
- **Column layout** (0-indexed): `RefAppart(0)` `Type(1)` `RefNum(2)` `Checkout(3)` `GuestName(4)` `Status(5)` `Currency(6)` `PaymentStatus(7)` `Amount(8)` `Commission(9)` `PaymentCharge(10)` `CityTax(11)` — `Net(13)` `PayoutDate(14)` `PayoutId(15)`
- Rows grouped by `PayoutId` (col 15) → one PennyLane batch per payout
- "Commission adjustment" rows: `net` = net_raw, `amount` = net, all other fields = 0

## Input format — Airbnb (Excel)
Monthly Excel from Airbnb. Contains "Payout" header rows followed by their reservations.
- **Row types processed**: `Réservation`, `Régularisation de la résolution`, `Hors réservation`, `Frais d'annulation`
- Payout rows mark the start of a new batch

## Accounting logic (both OTAs)
```
DEBIT  51105000          = Sum(Net)              — bank receipt
DEBIT  401BOOKING/604600 = Sum(|Commission| + |PaymentCharge|)  — OTA fees
CREDIT 411BOOKING/411AIRBNB = Amount − |CityTax| per reservation
```
Journal balanced: `DEBIT = CREDIT` always.

**Special case — Airbnb "Frais d'annulation"**: routed directly to `604610` (no 411AIRBNB).

## Archive & anomaly flow (real run only)
After a successful real run:
1. `Archive {date}/` subfolder created in the Drive root folder
2. Source xlsx moved into it
3. If any anomalies → Google Sheet `Anomalies BOOKING` or `Anomalies AIRBNB` created in the same subfolder

`dry_run=true` → no posting, no archiving.
`test=true` → posts with `[TEST]` prefix, no archiving.

## Anomaly severity
| Severity | Behavior |
|---|---|
| `BLOCKING` | Pipeline halts, nothing posted to PennyLane |
| `WARNING` | Pipeline continues, anomaly logged to sheet |

- `NON_EUR_CURRENCY` (e.g. CAD reservations on Airbnb) → **WARNING** (pipeline continues for EUR rows)
- `CANCELLED_WITH_AMOUNT` → **WARNING** (informational only — treated as normal reservation per accountant)
- `MAPPING_NOT_FOUND` → **BLOCKING**

## PennyLane account IDs
See `config/settings.py` for full mapping. Key codes:
- `BOOK` journal: `3621237` / `AIRB` journal: `3621262`
- `411BOOKING`: `760098756` / `411AIRBNB`: `671112489`
- `401BOOKING`: `756231429` / `604600`: `671113615` / `604610`: `671113726`
- `51105000_BOOK`: `671113821` / `51105000_AIRB`: `671113820`

## Drive folder structure
```
{folder_id}/                     ← root — must contain exactly 1 xlsx at run time
    Archive 2026-03-26/
        2603 Imports AirBnb.xlsx
        Anomalies AIRBNB         ← Google Sheet (warnings only)
    Archive 2026-03-27/
        260327 - Import Paiements Booking.xlsx
```

## Cloud Scheduler jobs
Deux jobs GCP Cloud Scheduler dans `europe-west1`, projet `merveil-data-warehouse` :
| Job | Schedule | OTA |
|---|---|---|
| `airbnb-pipeline-daily` | Tous les jours à 8h Paris | Airbnb |
| `booking-pipeline-weekly` | Tous les lundis à 8h Paris | Booking |

Les deux jobs sont **normalement en PAUSE** entre les runs mensuels/hebdomadaires. Workflow :
1. Déposer le fichier xlsx dans le bon dossier Drive
2. `gcloud scheduler jobs resume <job> --location=europe-west1`
3. `gcloud scheduler jobs run <job> --location=europe-west1`
4. Vérifier les logs Cloud Run
5. `gcloud scheduler jobs pause <job> --location=europe-west1`

Pour redéployer : `gcloud run deploy booking-pipeline --source . --region=europe-west1 --project=merveil-data-warehouse --quiet`
Toujours commiter/pusher le mapping avant de déployer.

## Known issues / notes
- SA must have **Organizer** role on the Shared Drive to move files uploaded by others
- `list_excel_files()` in `drive/client.py` also returns Google Sheets natively converted → filter by extension handled at download time
- `gcloud run services update-traffic` does NOT deploy a new image — always use `update --image`
- Le mapping `CodeAppart_Compta.csv` doit être mis à jour et redéployé si un nouvel appartement Booking apparaît (`MAPPING_NOT_FOUND` = anomalie bloquante)

---

## Changelog

### 2026-04-14 — Fixes parsing Booking + Airbnb (Google Sheets)
- Booking : support types français (`Réservation`, `Ajustement de la commission`, `customer_complaint`)
- Booking : parsing dates françaises (`9 avr. 2026`) + dates Excel datetime objects
- Airbnb : parser robuste aux colonnes variables via `col_map` par nom (extra colonne null détectée en col 3)
- Airbnb : parsing dates string DD/MM/YYYY + montants format français (virgule décimale)
- `folder_id` Drive hardcodé dans `config/settings.py` — plus besoin de le passer dans le body
- `customer_complaint` traité comme ajustement : net = montant déduit, commission = 0

### 2026-04-13 — Libellés frais Booking par réservation
- Débit `401BOOKING` éclaté en une ligne par réservation : `{code_comptable} - {ref_num} - FEE BOOKING - {date}`
- Nécessaire pour lettrage avec les factures de commission Booking (demande expert comptable)
- Airbnb inchangé (une ligne agrégée par batch)
- Paramètre `per_reservation_fees=True` dans `generate_entries()` activé uniquement pour Booking

### 2026-04-08 — Run mensuel Airbnb + Booking
- Airbnb : 72 réservations, 11 batches, 0 warnings, balance OK
- Booking : 59 réservations, 42 batches, 1 warning, balance OK
- Fix mapping `CodeAppart_Compta.csv` (nouvel appartement) → redéploiement nécessaire avant run
- Deploy cmd confirmé : `gcloud run deploy booking-pipeline --source . --region=europe-west1 --project=merveil-data-warehouse --quiet`

### 2026-03-27 — Booking pipeline migrated to Excel input
- `BookingExcelParser` replaces the old multi-CSV `BookingParser`
- Input: single weekly xlsx from Booking.com extranet (same Drive flow as Airbnb)
- Payout batches grouped by `PayoutId` (col 15)
- Added 2 missing Booking IDs in mapping: `TUR64-1D` → `13730199`, `DES3-5G` → `13730638`

### 2026-03-26 — First real Airbnb run
- 262 reservations, 25 batches, balance OK, 2 CAD warnings
- `NON_EUR_CURRENCY` downgraded from BLOCKING → WARNING
- Anomaly sheet enriched with `label_pennylane` for manual PennyLane correction

### 2026-03-20 — Drive archive + anomaly sheet
- `_archive_run()`: creates `Archive {date}/`, moves source file, creates anomaly sheet
- `dry_run` flag: full validation without posting or archiving
- Strict 1-xlsx validation in Drive root (prevents double imports)

### 2026-03-17 — PennyLane API IDs resolved
- Resolved numeric IDs for all journals and ledger accounts via API
- Added `PENNYLANE_JOURNAL_IDS` and `PENNYLANE_ACCOUNT_IDS` to `config/settings.py`

### 2026-03-10 — Airbnb "Frais d'annulation" reclassification
- Cancellation fee rows routed to `604610` instead of `411AIRBNB`
- `account_cancellation_fee` parameter added to `generate_entries()`

### Initial — Booking.com CSV pipeline
- Multi-CSV input (1 file per apartment, `{id}-{payout_id}.csv`)
- Single PennyLane entry per payout batch
- Airbnb Excel pipeline (monthly, grouped by Payout rows)
