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
- **Secret**: `PENNYLANE_TOKEN` via Secret Manager — secret `pennylane-token`, monté en `:latest` (depuis 2026-06-30 ; était épinglé sur une version fixe → rotations non prises en compte). Pour rotation : `echo -n "<token>" | gcloud secrets versions add pennylane-token --data-file=-` puis relancer (pas de redeploy). ⚠️ Le token PennyLane doit avoir le **scope écriture** : un token read-only laisse passer les GET mais renvoie **403 Forbidden sur POST `/ledger_entries`** (corps vide → 400 « champs manquants », corps complet → 403 = pas de droit d'écriture). Incident 2026-06-30 : token régénéré sans cocher l'écriture → pipeline bloquée au batch 1/32 (rien posté, pas d'état partiel).

## Request format
```json
{
  "folder_id": "1abc...xyz",
  "ota": "booking",          // "booking", "airbnb" ou "mews-payments" (source BQ, folder_id ignoré)
  "date": "2026-03-27",      // or "AUTO" for today (Paris timezone)
  "dry_run": true,           // optional — validate, no PennyLane post, no BQ write, no archiving
  "test": true,              // optional — post with [TEST] prefix
  "bq_only": true,           // optional — skip PennyLane, write trace to BQ only (validation mode)
  "run_id": "uuid",          // optional — auto-generated UUID per run if absent
  "force": false             // optional — ignore le journal d'idempotence en LECTURE
                             // (re-poste TOUT). Uniquement après nettoyage manuel
                             // Pennylane d'un état incertain. Les phases restent journalisées.
}
```

### Operating modes
| Mode | PennyLane | BQ trace | Archive Drive |
|---|---|---|---|
| Normal | POST | INSERT (bq_only=false) | Yes |
| `dry_run=true` | skip | skip | skip |
| `bq_only=true` | **skip** | INSERT (bq_only=true, ledger ids NULL) | Yes (suffixe `[BQ-ONLY]`) |
| `test=true` | POST avec `[TEST]` préfixe | INSERT (test_mode=true) | Yes |

## Architecture (refacto 2026-05-11 — Open/Closed)

Le module est structuré pour qu'ajouter un nouveau flux (Mews Bills V1, Mews Payments V2) = écrire 1 classe `Source` sans toucher au reste.

```
booking-accounting/
├── server.py              # Flask routes (~110 lignes) — instancie Source → appelle orchestrator
├── orchestrator.py        # run_pipeline(source, ...) — fonction unique générique (~250 lignes)
├── sources/               # Une classe par flux
│   ├── base.py            # Source (ABC) + SourceFetchResult (dataclass)
│   ├── booking.py         # BookingDriveSource ← Flux 1 Booking (Drive xlsx)
│   ├── airbnb.py          # AirbnbDriveSource  ← Flux 1 Airbnb (Drive xlsx)
│   ├── mews_bills.py      # ❌ ABANDONNÉ 2026-07-26 — les ventes Mews sont déversées par l'intégration NATIVE Mews→Pennylane à J+3 (confirmé journal réception Philippe 22/07, cf. ADR). Philippe garde les ventes natives, écrase les encaissements natifs (remplacés par flux 1+2)
│   └── mews_payments.py   # ✅ Flux 2 — MewsPaymentsSource (BQ stg_mews_exports__* → 511035 + 401MEWS + 411<canal>) — bq_only en cours
└── (autres modules réutilisés par tous : accounting, pennylane, bigquery, drive, lookups, validators)
```

**Interface `Source` (ABC)** :
- `name: str` (`'booking'`, `'airbnb'`, etc.)
- `entries_kwargs: dict` — kwargs pour `generate_entries()` (journal_code, account_*, ota_label, per_reservation_fees)
- `fetch(processing_date) -> SourceFetchResult` — récupère batches + anomalies + mapping
- `enrich_anomalies(result)` — hook optionnel (no-op par défaut)

**Orchestrator** (`run_pipeline`) : 1 fonction, 11 étapes (fetch → enrich → bill lookup → generate_entries → validate → balance check → blocking handling → dry_run → test mode → POST PennyLane → BQ trace → archive Drive). Aucune logique OTA-spécifique.

`_archive_run()` no-op si `drive_folder_id` vide (= Sources BQ comme `MewsBillsSource` n'archivent pas).

## Entrypoints
| File | Role |
|---|---|
| `server.py` | Flask routes — parse body, instancie la bonne Source via `_SOURCE_FACTORIES`, délègue à `orchestrator.run_pipeline()` |
| `orchestrator.py` | `run_pipeline()` — orchestrator unique générique |
| `sources/base.py` | `Source` (ABC) + `SourceFetchResult` (dataclass) — interface commune |
| `sources/booking.py` | `BookingDriveSource` — Flux 1 Booking depuis Drive |
| `sources/airbnb.py` | `AirbnbDriveSource` — Flux 1 Airbnb depuis Drive (avec hook `enrich_anomalies` pour NON_EUR_CURRENCY) |
| `parsers/booking.py` | `BookingExcelParser` — parses weekly Booking Excel into `BookingPayoutBatch` objects |
| `parsers/airbnb.py` | `AirbnbParser` — parses monthly Airbnb Excel into payout batches |
| `accounting/entries.py` | `generate_entries()` — builds PennyLane accounting lines from reservations |
| `pennylane/client.py` | `PennyLaneClient` — posts batches to PennyLane API, returns `ledger_entry_line_id` per line |
| `bigquery/postings.py` | `write_postings()` — append-only trace de chaque ligne postée vers `pennylane.raw_postings` (chantier 1 rapprochement) |
| `lookups/mews.py` | `lookup_bills()` — query BQ batch : `(ota_ref, gross)` → (bill_id, bill_number) matching par proximité (chantier 2). `resolve_apartments_by_channel()` — code de confirmation → apartment_code Mews → code_comptable (fallback libellé, cf. ci-dessous) |
| `drive/client.py` | `DriveClient` — downloads xlsx, creates folders, moves files, creates Sheets |
| `config/settings.py` | Account codes, journal IDs, thresholds |
| `config/mapping_loader.py` | `load_mapping()` (Booking), `load_airbnb_mapping()` (Airbnb), `load_apartment_comptable_map()` (apartment_code complet → code_comptable, pivot fallback Mews) |

## Mapping files
| File | Format | Key |
|---|---|---|
| `config/mapping/CodeAppart_Compta.csv` | semicolons, skip first 5 rows | Booking numeric ID → accounting code |
| `config/mapping/AirbnbLogement_Compta.csv` | comma-separated | Airbnb listing name → accounting code |
| `config/mapping/Mapping_appart_code.csv` | master file (semicolons) | col 0 = code appart complet, col 2 = CodeComptable, col 1 = Airbnb listing name |

### Fallback Mews (résolution appart, 2026-07-19)
Le mapping appart repose sur le **libellé** de l'annonce OTA (`Logement`), mutable → renommage = résa BLOCKING (récurrent). Le fichier de versement ne porte aucun code appart stable, seulement le **code de confirmation** (par résa). Fix : quand le libellé échoue (exact + normalisé), `orchestrator.py` Step 2.5 résout via le code de confirmation → `fct_reservations.channel_number` → apartment_code → code_comptable (`Mapping_appart_code.csv`). **Libellé reste primaire** (0 régression) ; le fallback ne touche que les résas déjà vouées au BLOCKING → auto-heal des renommages. Non-fatal (retombe sur BLOCKING si Mews ne résout pas). Désambiguïsation collisions (changement d'appart) = non-annulée + CI récent. Pas 100 % stable (90,9 % couverture, 3 collisions/12 mois) → jamais en primaire. Cf. ADR `decisions.md` 2026-07-19.

## Input format — Booking (Excel)
Flat weekly Excel export from Booking.com extranet. One row per reservation.
- **Column layout** (0-indexed): `RefAppart(0)` `Type(1)` `RefNum(2)` `Checkout(3)` `GuestName(4)` `Status(5)` `Currency(6)` `PaymentStatus(7)` `Amount(8)` `Commission(9)` `PaymentCharge(10)` `CityTax(11)` — `Net(13)` `PayoutDate(14)` `PayoutId(15)`
- Rows grouped by `PayoutId` (col 15) → one PennyLane batch per payout
- "Commission adjustment" rows: `net` = net_raw, `amount` = net, all other fields = 0

## Input format — Airbnb (Excel)
Monthly Excel from Airbnb. Contains "Payout" header rows followed by their reservations.
- **Row types processed**: `Réservation`, `Régularisation de la résolution`, `Hors réservation`, `Frais d'annulation`, `Remboursement des frais d'annulation`
- Payout rows mark the start of a new batch

## Accounting logic (both OTAs)
```
DEBIT  51105000          = Sum(Net)              — bank receipt
DEBIT  401BOOKING/604600 = Sum(|Commission| + |PaymentCharge|)  — OTA fees
CREDIT 411BOOKING/411AIRBNB = Amount − |CityTax| per reservation
```
Journal balanced: `DEBIT = CREDIT` always.

**Special case — Airbnb "Frais d'annulation" / "Remboursement des frais d'annulation"**: tous deux routés directement vers `604610` (pas de 411AIRBNB), débit/crédit selon le signe du Montant : `Frais d'annulation` (Montant négatif) → **DEBIT** 604610 (charge), `Remboursement des frais d'annulation` (Montant positif) → **CREDIT** 604610 (Airbnb rembourse — demande Philippe 2026-06-15). Label = `{code} - AIRBNB - {type} - {guest} - {ref}`.

**Special case — Booking "Commission adjustment"** (depuis 2026-06-11) : routé directement à `401BOOKING` (compte fournisseur), pas `411BOOKING`. Demande Philippe 2026-06-08 — ces ajustements correspondent à des corrections de commission Booking sans contrepartie résa (col M déductible mais col J vide). Label : `BOOKING - {code_comptable} - Comm ajustement`. DEBIT 401 si net négatif (Booking prélève plus), CREDIT 401 si net positif (rétrocession). Branché côté `sources/booking.py` via `entries_kwargs.account_commission_adjustment = ACCOUNT_SUPPLIER`.

## Archive & anomaly flow (real run only)
After a successful real run:
1. `Archive {date}/` subfolder created in the Drive root folder
2. Source xlsx moved into it
3. If any anomalies → Google Sheet `Anomalies BOOKING` or `Anomalies AIRBNB` created in the same subfolder

`dry_run=true` → no posting, no archiving.
`test=true` → posts with `[TEST]` prefix, no archiving.

## Gardes anomalies ajoutées 2026-07-03 (audit, rev 00072)
- **`NET_DERIVED`** (WARNING, 1/fichier) : fichiers Booking « Paid Online » sans colonne Net → net dérivé (`Amount+Commission+Charge+CityTax`). L'anomalie porte compte + total dérivé — **à recouper avec le montant du virement réel** (le check Sum(Net) est tautologique pour ces lignes).
- **`CITY_TAX_POSITIVE`** (WARNING, 1/résa) : convention = city_tax stockée ≤ 0 (Airbnb toujours 0). Une valeur positive inverse le calcul `gross_excl_city_tax = amount + city_tax`. WARNING et pas BLOCKING : un avoir légitime ne doit pas bloquer le run ; l'inversion systématique reste couverte par le sanity check anti-décalage BLOCKING du parser.

## Tests (réécrits 2026-07-03)
`tests/test_accounting.py` : assertions re-dérivées de la logique prod (les anciennes codaient la structure pré-refacto 2026-04-13 → 8 échecs pour de mauvaises raisons, zéro filet). Couvre les 2 configs prod (Booking `per_reservation_fees=True` + comm adjustment 401 ; Airbnb agrégé + cancellation fees 604610), l'**invariant débit=crédit** sur chaque scénario, refunds, bill_lookup, clé mapping normalisée. ⚠ Les kwargs prod y sont dupliqués sciemment depuis `sources/*.py` : si la config prod change, les tests DOIVENT casser. Suite complète : `python3 -m pytest tests/` → 62 tests. Pas de CI — à lancer avant tout deploy.

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
Jobs GCP Cloud Scheduler dans `europe-west1`, projet `merveil-data-warehouse` :
| Job | Schedule | Cible |
|---|---|---|
| `airbnb-pipeline-daily` | (en pause) | Cloud Run service `booking-pipeline` /process (Airbnb) |
| `booking-pipeline-weekly` | (en pause) | Cloud Run service `booking-pipeline` /process (Booking) |
| `lettering-sim-daily` | Tous les jours à 6h Paris ✅ | Cloud Run **Job** `lettering-sim` (simulation lettrage Airbnb+Booking sur 60j) |
| `grand-livre-pull-monthly` | 5 du mois à 7h Paris ✅ | Cloud Run **Job** `grand-livre-pull` (pull comptes 6041/6042/60472 du mois précédent → `pennylane.raw_grand_livre`) |
| `ledger-full-pull-daily` | Tous les jours à 6h Paris ✅ | Cloud Run **Job** `ledger-full-pull` (grand livre COMPLET tous comptes, daily incr. overlap 45j → `pennylane.raw_ledger_lines`) — cf. Lot A ci-dessous |
| `invoices-full-pull-daily` | Tous les jours à 6h15 Paris ✅ | Cloud Run **Job** `invoices-full-pull` (factures clients + fournisseurs, daily incr. overlap 90j → `pennylane.raw_{customer,supplier}_invoices`) — cf. Lot B ci-dessous |
| `bank-accounts-pull-daily` | Tous les jours à 6h30 Paris ✅ | Cloud Run **Job** `bank-accounts-pull` (soldes bancaires réels, snapshot append-only → `pennylane.raw_bank_accounts`) — cf. Trésorerie ci-dessous |
| `mews-payments-daily` | Tous les jours à 7h Paris ✅ | Cloud Run service `booking-pipeline` /process — **flux 2, mode `bq_only=true`** (phase validation : trace BQ seule, ZÉRO POST Pennylane). Au GO Philippe : retirer `bq_only` du body |

Les jobs Airbnb/Booking sont **en PAUSE permanent** (décision 2026-05-11, **réellement appliquée le 2026-07-27**). Le schedule fixe n'est pas adapté : les fichiers arrivent à intervalles irréguliers (hebdo Booking, mensuel Airbnb selon dépôt).

⚠ **La pause n'avait jamais été appliquée** (constaté 2026-07-27 : aucun `PauseJob` dans l'audit log, rétention 400 j). Les deux crons tiraient donc à blanc — `run_pipeline` sort en `{"status": "skipped"}` quand le dossier Drive est vide, en HTTP 200, **sans aucun log applicatif** : un run à vide est indistinguable d'un run réussi dans les logs. C'est ce qui a rendu le drift invisible pendant 2 mois. Si tu remets un schedule un jour, logue explicitement le skip.

### Cloud Run Job `lettering-sim`
- **Image** : partagée avec service `booking-pipeline` (entrypoint override `python -m pennylane.lettering_sim 60`)
- **Trigger** : `lettering-sim-daily` à 6h Paris (après dbt 6h + saisie manuelle comptable des ventes Mews dans PennyLane)
- **Endpoint manual** : `gcloud run jobs execute lettering-sim --region=europe-west1`
- **Sortie** : `pennylane.lettering_simulation` (BQ append-only, 1 row par ligne 411 inspectée)
- **Conso côté dash** : `dashboard_finance.dash_finance_lettering_sim` → tab 9.5 Simulation lettrage Workflow manuel actuel :
1. Déposer le fichier xlsx dans le bon dossier Drive
2. `gcloud scheduler jobs resume <job> --location=europe-west1`
3. `gcloud scheduler jobs run <job> --location=europe-west1`
4. Vérifier les logs Cloud Run
5. `gcloud scheduler jobs pause <job> --location=europe-west1`

### Backlog — trigger event-based (à la place du scheduler)
Remplacer les schedulers par un déclenchement sur l'**event Drive "nouveau fichier dans le dossier"** :
- **Option A — Drive Push Notifications (`changes.watch`)** : webhook Drive → Cloud Function relai → POST `/process` sur Cloud Run avec le bon `ota` selon le `folder_id`. Inconvénient : abonnement à renouveler toutes les 24h (TTL max).
- **Option B — Eventarc + Cloud Storage relai** : un Apps Script côté Drive copie le nouveau fichier sur un bucket GCS dédié → trigger Eventarc `google.cloud.storage.object.v1.finalized` → Cloud Run. Plus stable, mais ajoute un hop.
- **Option C — Poll léger** : Cloud Scheduler 1×/h qui appelle un endpoint `/check` qui liste le dossier Drive, et lance le pipeline si nouveau xlsx détecté. Moins event-driven mais zero infra additionnelle.
Recommandation initiale : Option A (le plus event-driven et stable côté GCP), avec fallback C si renouvellement TTL Drive devient un problème.

Pour redéployer : `gcloud run deploy booking-pipeline --source . --region=europe-west1 --project=merveil-data-warehouse --quiet`
Toujours commiter/pusher le mapping avant de déployer.

## Backlog — Tests
- **`tests/test_accounting.py` — 8 tests obsolètes** : ces tests datent d'avant la refacto du 13/04/2026 (`feat: Libellés frais Booking par réservation`) qui a éclaté le DEBIT 401BOOKING en une ligne par réservation au lieu d'une ligne agrégée par batch. Les assertions codent en dur l'ancienne structure (`entries[1].account == "401BOOKING"`, `len(entries) == 4` pour 1 résa, etc.). À mettre à jour pour refléter la structure actuelle (1 header 51105000 + 2 lignes par résa avec `per_reservation_fees=True`). 7 tests passent encore.
- **Tests manquants sur l'orchestrator + Sources** : pas de tests unitaires sur `orchestrator.run_pipeline()` ni sur les classes `BookingDriveSource` / `AirbnbDriveSource`. À écrire en même temps que la mise à jour de `test_accounting.py`.

## ⭐ Flux 2 (Mews Payments) DÉBLOQUÉ — exports webhook quotidiens (2026-07-16)

Le blocage historique du flux 2 (« pas d'endpoint Mews pour les commissions/versements », visio 12/05) est **levé** : les **exports planifiés Mews** poussent chaque jour par **webhook** (Export target → webhook-gateway `/webhooks/mews-exports`) le **Payout report** (~2h : versements Adyen + document `Transactions` = commission réelle PAR paiement + `Transaction Identifier`) et le **Payment report** (~12h45 : ventilation frais commission/interchange/scheme + type carte). Intervalle « Last day », dédup au staging.

- **Données en BQ** (parse SQL dbt, cf. ADR `decisions.md` 2026-07-16) : `staging.stg_mews_exports__payouts` / `__payout_transactions` / `__card_payments`.
- **Jointures validées au centime** : `transaction_id` = `stg_mews__payments.identifier` (→ payment_id, bill_id, reservation_id, canal) ; sum(transactions) = payout net ET commission ; refunds inclus (négatifs, commission 0).
- **Conséquence pour ce repo** : la future `MewsPaymentsSource` lira ces vues BQ (plus « bloqué côté commissions », plus de taux figés) → écritures par versement : DEBIT 511035 (net), DEBIT commissions Mews Payments (réelles), CREDIT 411<canal> par résa (gross). Pattern : `bq_only=true` en parallèle de la saisie comptable → revue Philippe → live. Remplace les 2-3h/5j de saisie manuelle du « Bilan Mews Payments ».
- **Prérequis avant de coder** : quelques jours/semaines d'accumulation + `dash_finance_payouts` (réco versements ↔ paiements ↔ relevé BNP) pour valider les chiffres.
- ⚠ Si une livraison webhook rate, Mews ne retente pas (trou 24h silencieux) → **alerte de continuité câblée** : dbt `trigger_finance_flow_stale` surveille la livraison (`stg_mews_exports__deliveries`, 30h), le contenu (rapports vides, 120h) ET les payouts non postés (âge du plus vieux payout Adyen sans `posted` dans `posting_journal`, 48h — cf. ADR `decisions.md` 2026-08-01). Le garde-fou `EXPORT_STALE` (BLOCKING à 72h) reste dans la source.

### ✅ `MewsPaymentsSource` codée + validée en dry-run (2026-07-26)

`sources/mews_payments.py` — source BQ (pas de Drive), `POST /process {"ota": "mews-payments"}`. Réponses Philippe 22/07 intégrées : journal **MEWS** existant (`3605247`), frais Adyen → **401MEWS** (lettrés contre la facture mensuelle Mews). 1 batch = 1 versement, écriture **datée du payout** (`MewsPayoutBatch.entry_date`, lu par l'orchestrator) :
```
DEBIT  511035     = net versé          DEBIT 401MEWS = Σ frais Adyen (signés)
CREDIT 411<canal> = gross par paiement (411WEBSITE/EXPEDIA/VRBO/MARRIOTT/PLUM/HOPPER/HOMETOGO — ids relevés dans raw_ledger_lines)
```
- **Résolution canal** (4 niveaux) : résa → bill (si 1 canal distinct) → compte Customer = 411WEBSITE (pratique Philippe) → 411DIVERS. Chaque fallback = WARNING (`CANAL_UNRESOLVED`). Booking/Airbnb en paiement carte = `UNEXPECTED_CHANNEL` (doublon potentiel avec flux 1).
- **Idempotence** : `journal_scope = 'batch_key'` (`read_journal_by_keys` dans `bigquery/journal.py`) — un payout_id posté une fois est posté pour toujours, quelle que soit la fenêtre de fetch (overlap 7j, `MEWS_PAYMENTS_OVERLAP_DAYS`).
- ⚠ **Périmètre = `DATE(created_at)` du payout, PAS `delivered_at`** : la livraison webhook du 16/07 était un dump historique one-shot (365 payouts Stripe depuis 2025-01, déjà comptabilisés manuellement). Filtrer sur delivered_at les ré-embarquerait tous.
- **Convention export validée** : `net = gross + commission`, commission **SIGNÉE** (négative = frais prélevés ; positive = frais restitués sur refunds/chargebacks — 17 cas depuis juin). `entries.py` gère déjà (Σ signée, bascule D/C). Types ère Adyen : Charge / Refund / Chargeback (chemin nominal, DEBIT 411). Types ère Stripe uniquement : `Commission adjustment` + `Platform fee` (**gross NULL**) → filet de sécurité : routés 401MEWS via la branche commission adjustment + WARNING `FEE_TRANSACTION`.
- **Garde-fous BLOCKING** : `EXPORT_STALE` (dernier export > 72h — Mews ne retente pas un POST raté) + `PAYOUT_UNBALANCED` (net payout ≠ Σ transactions). Bornes commission par source : `commission_rate_bounds = (0, 6%)` (vs 10-20% OTA).
- **Dry-run validé 2026-07-26** (local, 0 POST) : 8 payouts Adyen 16-24/07, 101 tx, 117 lignes, **8/8 équilibrés au centime**, 0 bloquant, 16 warnings légitimes (8 CANAL_UNRESOLVED dont Jason Paez 5 984 € + chargeback Oliver Bryan −808,80 € ; 8 FX multi-devises — convention markup à figer avec Philippe). `ref_piece` (bill Mews pré-résolu par la source, fallback du lookup heuristique) rempli ~35% — structurel, bills pas encore clos au payout.
- **Tests** : `tests/test_mews_payments.py` (9 tests synthétiques, invariant D=C). Suite complète 74/74.
- **Déployé 2026-07-26** (rev `00078-9kr`, commit `9a7a044`) : 1er run `bq_only` prod OK (78 paiements, 90 lignes BQ, 0 Pennylane) + scheduler `mews-payments-daily` 7h Paris **en mode bq_only** (accumulation ~2 sem). IAM : `booking-pipeline-sa` a reçu READER sur `mews_exports_raw` + `marts` (dataset ACL — `bq add-iam-policy-binding` nécessite un allowlisting, passer par `access_entries`).
- **Reste** : comparaison saisies Philippe au centime — outillée par le **tab 9.8 Flux 2 Adyen** (dashboards-v2, modèle `dash_finance_flux2_validation`, appariement paiement par paiement vs son journal 5488931), accès Philippe ouvert (26/07). **Revue fin août / début septembre** (3-4 sem d'accumulation, ~25-30 payouts) → figer convention frais (lui : TTC banque, frais dans le solde 511035 ; nous : net + 401MEWS) → GO → retirer `bq_only` du body scheduler → il arrête sa saisie. Mail de contexte : `Archides/mail-philippe-flux2-2026-07-26.md`.

## Known issues / notes
- SA must have **Organizer** role on the Shared Drive to move files uploaded by others
- `list_excel_files()` in `drive/client.py` also returns Google Sheets natively converted → filter by extension handled at download time
- `gcloud run services update-traffic` does NOT deploy a new image — always use `update --image`
- Le mapping `CodeAppart_Compta.csv` doit être mis à jour et redéployé si un nouvel appartement Booking apparaît (`MAPPING_NOT_FOUND` = anomalie bloquante). Idem `Mapping_appart_code.csv` pour le **flux 2** (mews-payments) : c'est le seul fichier qu'il lit, et une entrée manquante bloque TOUT le run (incident KLE40 du 29-31/07, cf. changelog 2026-08-01). ⚠ Les 3 CSV de mapping recopient à la main un code dérivable du code Mews (`strip(^[NP]\d+-)`, 132/133 conformes, seule exception SEB23-3FG) → dérive silencieuse entre fichiers + typos indétectables. Backlog : dériver + CSV réduit aux exceptions.

## ⭐ Journal write-ahead — `pennylane.posting_journal` (idempotence rejeu, 2026-07-02)

**Problème résolu** : l'idempotence reposait sur l'archive Drive (déplacée APRÈS tous les POST) et la trace `raw_postings` était écrite en bulk final non-fatal → crash mi-run (timeout, OOM, réseau) = batches postés dans Pennylane sans aucune trace, fichier toujours en place → le run suivant (lundi 8h auto ou replay manuel) **re-postait tout** = écritures dupliquées, sans DELETE possible (API v2). Pennylane n'a **aucune idempotence native** (doc officielle : les doublons créent des doublons).

**Mécanique** (`bigquery/journal.py` + boucle dans `orchestrator.py`) — séquence PAR batch :
1. INSERT `phase='intent'` dans `posting_journal` (**fatal** — on ne poste pas d'argent sans trace)
2. POST Pennylane (timeout 30 s)
3. INSERT trace `raw_postings` du batch (**fatal**)
4. INSERT `phase='posted'` + `ledger_entry_id` (**fatal**) → `posted` ⇒ tracé, garanti par l'ordre

**Clé du journal** : `(ota, source_file, file_hash md5, batch_key, mode)`.
- `batch_key` = `payout_id` Booking / `payout_reference` Airbnb (sémantique, robuste à un changement d'ordre du parser ; unicité vérifiée, run refusé sinon).
- `file_hash` : un fichier **corrigé** ré-uploadé (même nom, contenu différent) = nouveau journal → re-posté normalement (cas replay Iavotsoa).
- `mode` (`live`/`test`/`bq_only`) : les runs de validation ne shadowent jamais les runs réels.

**Au replay** : batch `posted` → **SKIP** ; batch `intent` sans `posted` = POST au résultat inconnu → **run `journal_blocked`** : rien n'est re-posté, fichier NON archivé, sheet d'anomalie `JOURNAL_UNCERTAIN_STATE` créée dans le dossier Drive racine. Résolution manuelle (procédure complète en docstring de `bigquery/journal.py`) : vérifier dans Pennylane par date+label → si l'écriture existe, INSERT `posted` manuel ; sinon nettoyer Pennylane et relancer avec `{"force": true}`.

**Table** : `pennylane.posting_journal` (append-only, DDL dans `bigquery/create_posting_journal.sql`). Aucun impact sur les consommateurs de `raw_postings` (`dash_finance_postings`, `sem_finance`) — table séparée.

**Timeouts associés (2026-07-02)** : Cloud Run service 120 s → **600 s** (la cause n°1 de crash mi-run était le kill à 120 s sur gros fichier), scheduler `attemptDeadline` 180 s → **600 s**, POST requests `timeout=30`.

**Tests** : `tests/test_orchestrator_journal.py` (8 tests — nominal, replay total/partiel, incertain bloquant, crash simulé + replay, force, bq_only, clés dupliquées). E2E validé contre BQ réel le 2026-07-02 (run 1 poste 3 batches synthétiques, run 2 skippe 3/3).

## BQ trace — `pennylane.raw_postings` (chantier 1 rapprochement, 2026-05-06)
Table append-only qui capture chaque ligne d'écriture générée par le pipeline (= 1 ledger_entry_line PennyLane par row). Pivot pour le rapprochement futur avec `mews_raw.raw_bills`.

**Schéma** : `posted_at, run_id, ota, journal_code, processing_date, payout_date, source_file, batch_index, entry_index, ledger_entry_id, ledger_entry_line_id, account_code, ledger_account_id, label, debit, credit, ota_reservation_ref, ref_appart, code_comptable, ref_piece, bill_id_mews, test_mode, bq_only, service_version`. Partitionné `DATE(posted_at)`, clusterisé `ota, run_id`.

**SA** : `booking-pipeline-sa@merveil-data-warehouse.iam.gserviceaccount.com` a le rôle `WRITER` (= `bigquery.dataEditor`) sur le dataset `pennylane`. Streaming inserts via `insert_rows_json` (pas besoin de `jobUser`).

**Modes** :
- Run normal → `ledger_entry_id`/`ledger_entry_line_id` remplis avec les vrais IDs PennyLane (récupérés depuis la réponse API par alignement positionnel `entries[i] ↔ result.ledger_entry_lines[i]`)
- `bq_only=true` → POST PennyLane skippé, BQ écrit avec `ledger_entry_id=NULL` et `ledger_entry_line_id=NULL`. Mode validation pour itérer sur le schéma sans risque comptable
- ~~BQ insert non-fatal~~ → **FATAL depuis 2026-07-02** : la trace est écrite **par batch, juste après chaque POST** (plus de bulk final), et son échec stoppe le run. Le non-fatal d'avant est précisément ce qui rendait le rejeu aveugle (cf. section Journal write-ahead).

**Champs `ref_piece` + `bill_id_mews` (chantier 2 — 2026-05-07)** : remplis automatiquement via `lookups/mews.py`. Signature `(ota_ref, gross)` : le gross sert au matching par proximité de montant (= plus robuste que "Closed prioritaire" qui choisissait parfois un bill annexe REBATE au lieu du bill principal chambre, cas Rachel Ward 24618). Tie-breakers : Closed > consumed récent. Filtre `having abs(sum) >= 1` pour ignorer les bills techniques.

**Self-healing complémentaire côté dbt** : `dash_finance_postings` fait un retro-lookup à chaque dbt run avec COALESCE (retro_dbt prioritaire sur pipeline). Si pipeline a fait un mauvais lookup au moment du POST → retro_dbt corrige. Validé sur 619 lignes 411 (10 runs Booking + Airbnb) : 64% match parfait, 4 vrais orphelins (résas Mews annulées).

**Permissions IAM** : SA `booking-pipeline-sa@` a `roles/bigquery.jobUser` au project-level + `dataViewer` sur le dataset `staging` (en plus du `WRITER` sur `pennylane`). Lookup en best-effort (try/except non-fatal) : si BQ tombe, le run continue avec `ref_piece` vide.

**Champs NULL pour les lignes header** (`account_code='51105000'` bank, `account_code='401BOOKING'/'401AIRBNB'` supplier) : `ota_reservation_ref`, `ref_appart`, `code_comptable`, `payout_date` (les headers agrègent toutes les résas du payout — pas de réf unique).

## BQ coverage — Booking.com & Airbnb (vérifié 2026-04-16)
Pour les réservations non annulées (3 derniers mois), couverture dans `marts.fct_reservations` :
| Champ | Booking.com | Airbnb |
|---|---|---|
| `channel_number` (ref OTA) | 100% | 100% |
| `customer_name` | 99.9% | 99.1% |
| `apartment_name` | 100%* | 100%* |

*`P09-CAR7-0G` manquait dans `sheets_raw.appartements_snapshot` (`Nom_Appartement` null) → corrigé manuellement le 2026-04-16 via UPDATE BQ. Sera pris en compte au prochain run dbt.

**Conséquence** : pour Booking.com et Airbnb, le rapport de comptabilité suffit comme source financière. BQ enrichit avec le nom client via JOIN sur `channel_number`. Aucun export Mews supplémentaire nécessaire pour ces deux OTAs.

### Refresh `appartements_snapshot`
Table native (pas d'auto-sync). Quand le DWH Feed Google Sheets est modifié :
- Option rapide : `UPDATE sheets_raw.appartements_snapshot SET Nom_Appartement = '...' WHERE Code_Appartement = '...'`
- Refresh complet (depuis BQ Console uniquement — MCP n'a pas les credentials Drive) :
  ```sql
  TRUNCATE TABLE `merveil-data-warehouse.sheets_raw.appartements_snapshot`;
  INSERT INTO `merveil-data-warehouse.sheets_raw.appartements_snapshot`
  SELECT * FROM `merveil-data-warehouse.sheets_raw.appartements`;
  ```

---

## Pipeline PennyLane Grand Livre (mai 2026)

Module séparé du `booking-pipeline` (qui POSTe) — ici on **PULL** le grand livre PennyLane pour rapatrier les comptes loyer/charges/taxe foncière dans BQ. Source de données pour `/loyers` Tab 11.1.

**Architecture** :
- Script CLI : `pennylane/grand_livre.py`
- Cloud Run Job : `grand-livre-pull` (image partagée avec `booking-pipeline`)
- Scheduler : `grand-livre-pull-monthly` à **5 du mois 7h Paris** (cron `0 7 5 * *`)
- Table BQ cible : `pennylane.raw_grand_livre` (append-only avec MERGE sur `ledger_entry_line_id`)

**Filtres comptes** :
- `LOYER_PREFIX = "6041"` → loyer (1 sous-compte par bail, label = "P02-ABO52-0&1 - Loyer")
- `CHARGES_PREFIX = "6042"` → charges (idem pattern)
- `TAXE_PREFIX = "60472"` (5 chars côté API, ≠ "604720000000" 12-chars du grand livre xlsx) → taxe foncière TOM. Label générique "Taxe foncière - TOM" → le code appart est dans `libelle_piece` (extracté par cascade côté dbt `stg_pennylane__loyer_charges_taxes`).

**Fix 2026-06-02 — `libelle_piece` = `entry.label` (Libellé de pièce)** : le puller utilisait `line.get("label")` qui est une note de ligne souvent vide ou résumée ("TF 2025", "taxe foncière"). Le vrai Libellé de pièce visible dans le grand livre Pennylane (ex `"JJR27-1 - OPTIMMO GESTION - 10/2025"`) est porté par l'écriture **parente** (`ledger_entry.label`). Ajout de `get_entry(id)` avec cache (ratio observé ~2.1 lignes/entry, donc ~1350 GET additionnels pour 2025+2026 → coût marginal). Bascule de la couverture TF de 11% à 99.8% matchable une fois `dbt seed mapping_pennylane_compta` + cascade staging en place. Détails → memory `project_pennylane_tf_coverage` côté dashboards-v2.

**Optimisations clés** :
- `sort=-date` côté API (Pennylane v2 trie en date desc) → **early-stop** dès qu'on dépasse `from_date` (200 items consécutifs sous la borne). Pour 1 mois : 22k items scannés en 3 min au lieu de 613k en 31 min.
- `limit=100` (param `per_page` ignoré par v2, vrai param = `limit`).
- Retries 8× sur 429 / 5xx / Timeout / ConnectionError avec backoff exponentiel.
- MERGE BQ sur `ledger_entry_line_id` (gère les corrections comptables tardives sans doublon).

**Limites connues** :
- ~~⚠️ **Taxe foncière : ~80% des écritures ont `libelle_piece` vide**~~ — **résolu 2026-06-02** par le fix `entry.label` ci-dessus + seed `mapping_pennylane_compta` + cascade staging. Couverture matchable passée à 99.8% (301 lignes / 230 k€ sur 2025). Limitation résiduelle : 100+ k€ de libellés contiennent un **code immeuble** ("HOC16 - ORALIA SULLY") qui couvre N sous-apparts → étape 4 du staging répartit en 1/N (simpliste, ~50-60% précision). **Mail envoyé à Philippe le 2026-06-05** pour trancher entre saisie par sous-code (idéal) ou prorata superficie côté DWH avec flag estimated. À ajuster selon sa réponse. Voir memory `project_pennylane_tf_coverage`.
- ~26 apparts en compta SCI propriétaire (Archides n'est pas preneur) ne remontent pas dans cet API (autre dossier Pennylane). Couverture actuelle : 99/125 apparts pour loyer/charges.

**Bootstrap historique** : `python -m pennylane.grand_livre --from-date 2025-01-01 --to-date 2026-05-31` (~25 min, 267k lignes scannées, 2 951 lignes mergées).

**Run mensuel auto** : `python -m pennylane.grand_livre --last-month` (Cloud Run Job).

---

## Trésorerie — `bank_accounts` (2026-06-25)

Soldes bancaires **réels** par compte via l'endpoint Pennylane `/bank_accounts` (Pennylane connecté aux banques → solde live, ≠ solde comptable reconstitué). Cf. ADR `Archides/docs/decisions.md` 2026-06-25.

- Script CLI : `pennylane/bank_accounts.py` (réutilise `PennylaneGLClient`). Snapshot **append-only** : 1 ligne / compte / run → `pennylane.raw_bank_accounts` (solde, nom, `ledger_account_id` = lien transactions, `updated_at` synchro banque).
- Cloud Run Job : `bank-accounts-pull` (image partagée `booking-pipeline`, args `-m,pennylane.bank_accounts`, secret `pennylane-token`).
- Scheduler : `bank-accounts-pull-daily` 6h30 Paris.
- **Aval dbt** : `stg_pennylane__bank_accounts` (dernier snapshot) → `dashboard_finance.dash_finance_treasury` (solde + seuil seed `treasury_thresholds` + statut + `montant_a_virer`) → trigger `treasury_low` (digest daily, action-engine).
- 11 comptes remontés ; 6 actifs whitelistés dans le seed seuils (placeholders à calibrer). Dormants (BNP/HSBC/Spendesk à 0) hors seed.

## Pipeline Grand Livre COMPLET — `ledger_full` (Lot A, 2026-06-23)

Frère du pipeline ci-dessus, mais **tous les comptes** (pas seulement loyer/charges/taxe) en **daily incrémental**. Objectif : tout le grand livre dans BQ pour analyses financières + futur rapprochement (Lot B = factures). Cible archi = remplacer le mensuel à terme (Phase 2), pour l'instant **séparé** (zéro risque). Cf. ADR `Archides/docs/decisions.md` 2026-06-23.

**Architecture** :
- Script CLI : `pennylane/ledger_full.py` (réutilise `PennylaneGLClient` de `grand_livre.py`)
- Cloud Run Job : `ledger-full-pull` (image partagée `booking-pipeline`, args `-m,pennylane.ledger_full`)
- Scheduler : `ledger-full-pull-daily` à **6h Paris** (cron `0 6 * * *`, hors cascade 2h)
- Table BQ cible : `pennylane.raw_ledger_lines` (partition MONTH sur `date`, cluster `account_number`, MERGE sur `ledger_entry_line_id`)

**Incrémental** : l'API v2 ignore les filtres de date mais respecte `sort=-date`. On tire en DESC, on garde `date >= from_date`, early-stop sous la borne (`EARLY_STOP_BUFFER=200`). ⚠️ **Curseur ancré sur `today − 45j`** (`OVERLAP_DAYS`, re-scan glissant pour corrections antidatées), **PAS `MAX(date)`** : des écritures sont datées dans le futur (versements/prévisions) → elles gonfleraient le curseur mais restent captées car en tête du scan DESC.

**Schéma** : `ledger_entry_line_id, ledger_entry_id, date, journal_id, account_id, account_number, label` (note de ligne), `debit, credit, lettered_line_ids` (ids lettrage, REPEATED), `created_at, updated_at, ingested_at`. **Pas** de nom de compte ni de libellé d'écriture parente (enrichissement `get_account`/`get_entry` **déféré** — N+1 trop coûteux sur le ledger complet ; à brancher Phase 2 via une référence `ledger_accounts`).

**Validation (2026-06-23)** : avril, 3 comptes, vs `raw_grand_livre` → **219/219 communes identiques au centime**. Écart = timing (écritures antidatées avril ajoutées en juin + corrections re-keyées Pennylane).

**Limites** : (1) ids supprimés côté Pennylane = orphelins dans la table (MERGE ne supprime jamais — dbt filtrera l'état courant) ; (2) labels comptes/écritures déférés (Phase 2) ; (3) **backfill 2026 lancé le 2026-06-24** (`--from-date 2026-01-01`, 76 892 lignes) → la table couvre **2026-01-01 → futur** ; pour 2025 aussi : `python -m pennylane.ledger_full --from-date 2025-01-01` (one-shot, pas encore fait).

**Run** : daily auto `python -m pennylane.ledger_full` (overlap 45j). Bootstrap/backfill : `--from-date YYYY-MM-DD`. Dry-run : `--dry-run`.

---

## Pipeline Factures — `invoices_full` (Lot B, 2026-06-24)

Frère de `ledger_full` (Lot A) mais sur les **factures** : endpoints Pennylane v2 `/customer_invoices` + `/supplier_invoices`. Objectif : faire passer la page Finances du prisme PMS (Mews, DSO faux) au prisme **comptable réel** (DSO trésorerie, vrais impayés tous canaux, lettrage). Pivot = `ledger_entry_id` (jointure directe sur `raw_ledger_lines` du Lot A). Cf. ADR `Archides/docs/decisions.md` 2026-06-24.

**Architecture** :
- Script CLI : `pennylane/invoices_full.py` (réutilise `PennylaneGLClient` de `grand_livre.py`, param `--kind customer|supplier|both`)
- Cloud Run Job : `invoices-full-pull` (image partagée `booking-pipeline`, command `python`, args `-m,pennylane.invoices_full`, SA `booking-pipeline-sa`, secret `PENNYLANE_TOKEN`)
- Scheduler : `invoices-full-pull-daily` à **6h15 Paris** (cron `15 6 * * *`, juste après le ledger Lot A 6h)
- Tables BQ cibles : `pennylane.raw_customer_invoices` + `raw_supplier_invoices` (partition MONTH sur `date`, cluster `paid`, MERGE sur `id`)

**Incrémental** : l'API v2 n'autorise `sort` que sur `id`/`date` (PAS `updated_at` → HTTP 400). On tire en `sort=-date` DESC, garde `date >= from_date`, early-stop sous la borne (`EARLY_STOP_BUFFER=200`). ⚠️ `OVERLAP_DAYS=90` (vs 45 pour le ledger) : un paiement décale plus dans le temps que l'écriture → fenêtre plus large pour capter le passage "payé".

**Schéma** (en-têtes scalaires) : `id, ledger_entry_id, party_id` (= customer/supplier id), `invoice_number, date, deadline, amount, tax, currency, currency_amount, status, paid, remaining_amount_with_tax, external_reference, label, created_at, updated_at, archived_at, ingested_at`. Nested `invoice_lines`/`payments`/`matched_transactions` **déférés** (non requis pour DSO/impayés/lettrage).

**Validation (2026-06-24)** : backfill `--from-date 2024-07-01` → 2 383 factures clients (jusqu'à 2024-07-09) + 15 650 fournisseurs. **100% portent un `ledger_entry_id`**. Re-run prod idempotent.

**Limites** : (1) jointure customer→`raw_ledger_lines` complète sur **2026** (Lot A backfillé 2026-06-24) — antérieur à 2026 partiel tant que `ledger_full --from-date 2025-01-01` pas lancé ; (2) `remaining` fournisseur négatif (convention signe Pennylane = dette) + loyers futurs non échus (max 2026-09-01) → gestion signe/échéance = sujet dbt Phase 2 ; (3) paiements > 90j ratés (rare).

**Run** : daily auto `python -m pennylane.invoices_full` (les 2 types, overlap 90j). Backfill : `--from-date 2024-07-01`. Un seul type : `--kind customer`. Dry-run : `--dry-run`.

---

## Changelog

### 2026-08-01 — Incident mapping KLE40 : flux 2 bloqué 3 jours + 15 écritures Airbnb mal libellées
- **Cause** : typo `KLE40-2D` (au lieu de `2F`) introduite fin avril 2026 dans les 3 CSV de mapping — le code n'existe dans aucune source amont (Mews, natif Mews→Pennylane, factures fournisseurs et nos propres écritures disent tous `2F`). Vraisemblablement recopié depuis la mauvaise colonne d'`Apparts.csv` (dump de référence, non chargé).
- **Impact 1** : 15 écritures Airbnb postées 28/04→28/07 avec libellé `KLE40-2D` (comptes et montants justes — le code comptable ne sert qu'au libellé, le 411 vient du canal). Correction côté Pennylane = décision Philippe, liste à fournir.
- **Impact 2** : flux 2 bloqué 29-31/07 (`MAPPING_NOT_FOUND` BLOCKING sur 1 tx de 1 412,70 € → tout le run annulé), 4 versements/66,6 k€ non tracés, **sans alerte** (HTTP 200 sur `blocked` + trigger continuité au mauvais grain, cf. ADR 2026-08-01).
- **Fix** : `2D`→`2F` dans les 3 CSV + ajout `BGO41-0F` à `Mapping_appart_code.csv` (2e appart actif absent, même bombe à retardement). Commit `8779167`, rev `00079-hqs`. Replay OK : 4 batches, 70 827,56 € équilibrés au centime, 0 bloquant. Couverture vérifiée : 124/124 apparts actifs 2026 mappés.
- **Restes ouverts** : (a) dériver le code comptable au lieu de le recopier (backlog, cf. Known issues) ; (b) `server.py` renvoie HTTP 200 sur `status="blocked"` → invisible côté Cloud Monitoring ; (c) liste des 15 écritures pour Philippe.

### 2026-06-24 — Lot B Factures (customer + supplier invoices → BQ)
- Module `pennylane/invoices_full.py` + job `invoices-full-pull` + scheduler `invoices-full-pull-daily` (6h15 Paris). Tables `pennylane.raw_customer_invoices` (2 383) + `raw_supplier_invoices` (15 650), 100% `ledger_entry_id`. Backfill 2024-07-01. Découverte : `sort` API limité à `id`/`date` → overlap 90j sur `date`. Section "Pipeline Factures" + ADR `decisions.md` 2026-06-24.

### 2026-06-16 — Airbnb "Remboursement des frais d'annulation" → CREDIT 604610
- Demande Philippe (mail) : Airbnb rembourse des frais d'annulation (RC : Emilia/Louise) → **crédit du compte 604610**. Fichiers `260609` / `260615 Import Airbnb.xlsx`.
- Type `Remboursement des frais d'annulation` était absent de `AIRBNB_RESERVATION_TYPES` → lignes ignorées par le parser (43 le 09/06, 77 le 15/06).
- **Implémentation** :
  - `config/settings.py` : ajout du type à `AIRBNB_RESERVATION_TYPES`.
  - `accounting/entries.py` : la branche `is_cancellation_fee` (604610) matche désormais les 2 types et route en **débit/crédit selon le signe** (positif → CREDIT, négatif → DEBIT) au lieu d'un débit forcé. Label dynamique sur `reservation_status`. Comportement `Frais d'annulation` inchangé (Montant toujours négatif → DEBIT).
  - `config/mapping/AirbnbLogement_Compta.csv` : ajout `Merveil - Signature Suite - Louvre - Bourse II` → `BRS1-5G` (manquant, MAPPING_NOT_FOUND bloquant sur 260615).
- **Validation locale** (dry, non posté) : `260609` 113 résas, balance 0,00 €, 604610 CREDIT 23 458,11 € / 43 lignes. `260615` 376 résas, 0 anomalie, balance 0,00 €, 604610 CREDIT 44 165,93 € (77 remb) + DEBIT 657,00 € (2 frais).

### 2026-06-15 — Classeur Booking multi-onglets + fix lookup Mews
- **Sélection auto de l'onglet** (`parsers/booking.py`) : Philippe dépose désormais un classeur avec onglets de contrôle (`Export Encaissements non-lettré`, `Matrice de vérification`, `Journal`) en plus de `Exports CSV Booking`. `wb.active` pointait sur le mauvais onglet → `No batches found`. Le parser retient maintenant le 1er onglet dont l'entête mappe les 5 `REQUIRED_FIELDS`, fallback `wb.active` pour les fichiers mono-onglet. Validé sur `260615` : onglet `Exports CSV Booking` sélectionné, 44 batches, 53 résas, balance OK, ajustement commission `160,28 €` (MTM13-2G) correctement routé 401BOOKING.
- **Fix lookup Mews `ref_piece`** (`lookups/mews.py`) : `ScalarQueryParameterType(name, type)` cassait sur `google-cloud-bigquery` 3.x (signature `(type_, *, name=...)`) → warning non-fatal `takes 2 positional arguments but 3 were given`, `ref_piece`/`bill_id_mews` vides dans le trace BQ (rattrapés par self-healing dbt). Corrigé en keyword-only. Après fix : `50/51 refs matched (48 avec bill_number)`.
- **Revisions** : `00056-z7t` (parser) → `00057-vl2` (lookup) → `00058-db8` (dédup) → `00059-t7b` (mapping Airbnb).
- **Alias mapping Airbnb** (`AirbnbLogement_Compta.csv`) : annonce renommée `Luxury Suite - Louvre - Dalayrac` → `DAL40-1D` (MAPPING_NOT_FOUND bloquant sur le run Airbnb 260615 ; Dalayrac = 1 seul appart P02-DAL40-1D).
- **Runs réels postés 2026-06-15** : Booking `4860fe08` → 44 batches Pennylane, 51 résas, 129 lignes BQ, balance OK, 3 warnings. Airbnb `9c9e8371` → 42 batches, 299 résas, 383 lignes BQ, balance OK, 5 warnings (CAD/annulations non bloquantes, feuille d'anomalies archivée).
- **Dédup lignes strictement identiques** (`parsers/booking.py`) : résa `5983771449` (Wioleta Buczyńska, appart 3788679) présente en **3 lignes identiques** dans `260615` → triple comptage CREDIT 411BOOKING (~5 504 € en trop). Ajout d'une dédup sur clé `(ref, payout, ref_appart, amount, net, commission, payment_charge, city_tax, status)` : on garde la 1re occurrence, chaque doublon écarté → anomalie `DUPLICATE_RESERVATION` WARNING tracée. Les lignes de même ref aux montants différents (modif/correction) ne sont PAS dédupliquées. Après dédup : 51 résas (vs 53), balance `110 487,26 €`. ⚠️ Les totaux de Philippe (Net 98 684,48 €) incluaient les 3 copies — à signaler.

### 2026-06-11 — Routage "Commission adjustment" Booking vers 401BOOKING
- Demande Philippe (mail 2026-06-08) : les rows `Commission adjustment` du CSV Booking (col M déduction mais col J vide) doivent aller en **DEBIT 401BOOKING** (fournisseur) avec libellé `BOOKING - {code_comptable} - Comm ajustement`. Avant : routées comme un refund client en DEBIT 411BOOKING avec label guest+CO.
- **Implémentation** :
  - `parsers/booking.py` : `reservation_status = "Commission adjustment"` pour ces rows (au lieu de `"ok"`). Exclusion du check `CANCELLED_WITH_AMOUNT` sur ce nouveau status.
  - `accounting/entries.py` : nouveau param `account_commission_adjustment: Optional[str]`. Quand fourni + `r.reservation_status == "Commission adjustment"` → route vers ce compte avec label spec Philippe. Sens DEBIT si net négatif, CREDIT si positif.
  - `sources/booking.py` : `entries_kwargs` ajoute `"account_commission_adjustment": ACCOUNT_SUPPLIER`. Airbnb non affecté (n'a pas ce row type).
- **Validation** : test intégration sur le CSV du 8 juin Philippe (46 batches, 61 résas, 4 commission adjustments) — toutes les 4 (lignes 6/18/39/41) sont correctement routées vers 401BOOKING avec le bon libellé, balance préservée sur les 46 batches.
- **Process pour Philippe** : à partir de la prochaine bascule (image redeploy), il peut déposer le CSV/xlsx Booking habituel sans manip préalable, même quand il contient des Commission adjustment.

### 2026-06-05 — Incident Iavotsoa + sanity check anti-décalage
- **Incident 2026-06-01** : fichier source Booking déposé par Iavotsoa avec valeurs **décalées d'1 colonne** par rapport aux entêtes (cf. mail Philippe 2026-06-04). Pipeline header-based → a parsé correctement chaque colonne selon son entête, mais les valeurs étant mal positionnées en amont, 41 écritures Pennylane ont été postées avec 411BOOKING au débit au lieu du crédit et 401BOOKING inversé aussi. 50 résas concernées (`260601 - Import Paiements Booking.xlsx`).
- **Résolution 2026-06-05** : (1) Philippe a supprimé manuellement les 41 écritures dans l'UI Pennylane (l'API v2 ne supporte pas DELETE/cancel sur ledger_entries — testé, HTTP 404). (2) Hatim a uploadé le fichier corrigé (= onglet "Copie de Exports CSV Booking" du xlsx Philippe). (3) Pipeline re-déclenché via `gcloud scheduler jobs run booking-pipeline-weekly`. Vérification BQ : 411BOOKING 90763.62 € crédit / 51105 77621.08 € débit / 401BOOKING 13142.54 € débit — au centime près sur les attendus Philippe.
- **Hardenings parser `parsers/booking.py`** (commit `70ba25e`) :
  - **Aliases élargis** sur `city_tax` : ajout de `"Taxe séjour"` (avec accent, sans "de") et `"Taxe sejour"` (sans accent, sans "de"). En plus de `"Taxe de séjour"` existant.
  - **Sanity check anti-décalage** dans `parse_into_batches` : si > 50% des résas ont `|city_tax| > |amount|`, émission d'une anomalie `BLOCKING` → l'orchestrator stoppe le POST Pennylane et archive le run avec anomaly sheet sur Drive. Aurait détecté l'incident avant qu'il ne touche Pennylane.
- **Process à retenir** : en cas de mauvais déversement, suppression manuelle UI Pennylane uniquement (pas d'API). Replay = fichier source corrigé + `gcloud scheduler jobs run booking-pipeline-weekly`. Mémoire `project_compta_booking_iavotsoa_incident` côté dashboards-v2.

### 2026-06-02 — Fix puller grand_livre.py (libelle_piece = entry.label)
- Pull libellé pièce depuis `ledger_entry.label` (parente) au lieu de `line.label` (note ligne). Ajout `get_entry(id)` cached. Backfill complet 2025-01 → 2026-11 (~25 min). Couverture matchable TF 11% → 99.8%. Détails section "Pipeline PennyLane Grand Livre" ci-dessus.

### 2026-05-12 — Visio Mews + ordre d'exécution révisé
- **Visio Mews (Francesca)** : confirme qu'il n'y a **pas d'endpoint Mews pour les commissions Stripe / versements**. Pour Expedia/VRBO, réconciliation des commissions à faire via rapports plateformes (pas via Mews). Accès Stripe lecture seule sur compte Mews **non répondu** (esquivé, à reposer par email).
- **Gap upsells Duve/Mews confirmé** : Mews en investigation avec Duve, pas de roadmap. Pattern observé côté BQ : 2052 bills Mews `State='Open'` sur 30j sans `IssuedUtc` (probablement payés via Stripe Duve qui ne push pas dans Mews). Action : contacter Duve directement.
- **`MewsBillsSource` (flux 3) — PRIORITÉ V1, à coder cette semaine** : correction 2026-05-13 — il n'y a **AUCUNE intégration native Mews → PennyLane**. Le comptable saisit manuellement toutes les écritures de vente Mews depuis un bilan Excel Mews (~2-3 h tous les 5 jours = 12-15 h/mois rien que pour Airbnb+Booking). Notre erreur d'interprétation précédente venait de l'observation des écritures sur journal `3605247` qu'on attribuait à tort à du natif — c'était en fait la saisie comptable manuelle. Notre dev `MewsBillsSource` doit lire `int_compta__bills_net` et poster automatiquement (DEBIT 411<canal> + CREDIT 708101 HT + CREDIT 44571008 TVA). Effort 3-5 j. Gain ~20 h/mois côté comptable une fois Phase 2 en prod.
- **Stripe direct + Stripe Duve** : Hatim a accès admin sur les 2 comptes → 2 clés API restricted read-only à créer côté Stripe, puis `StripeDirectSource` + `StripeDuveSource` (~1-2j chacun). Indépendant de Mews.
- **Nouvel ordre d'exécution** :
  - **Phase 1 (~3-5j)** : coder `MewsBillsSource` pour automatiser le déversement des ventes Mews (aujourd'hui manuel via Excel = 12-15 h/mois comptable). Lecture `int_compta__bills_net` → POST PennyLane (706 + 44571008 + 411<canal>). Mode `bq_only=true` pendant 2-3 sem en parallèle de la saisie comptable pour comparer.
  - **Phase 2 (~1 sem)** : bascule live + activation auto-letter. Comptable arrête sa saisie, notre pipeline poste les ventes en temps réel après chaque polling Mews. Module auto-letter activé simultanément (précision V1 déjà à 98,9 % sur le tab 9.5).
  - **Phase 2** : selon réponse comptable, `StripeDirectSource` + `StripeDuveSource` (toujours pertinents), `MewsBillsSource` (à valider), `MewsPaymentsSource` (bloqué côté commissions Mews mais possible avec taux figés).

### 2026-05-11 — Refacto orchestrator + Sources (Open/Closed) + Sebastopol I-II
- **Refacto pipeline** : extraction de la logique commune des 2 pipelines Booking + Airbnb dans `orchestrator.run_pipeline()` + interface `Source` (ABC) avec implémentations `BookingDriveSource` + `AirbnbDriveSource`. `server.py` passe de 521 à 110 lignes. API HTTP inchangée. Préparation à l'ajout des futurs pipelines V1 (Mews Bills / ventes) et V2 (Mews Payments / Stripe). Détails dans la section "Architecture" ci-dessus.
- **Nouveau mapping appart** : ajout `Merveil Connecting Luxury Suites - Sebastopol I-II` (code interne `P01-SEB23-3F&3G`) → code comptable **`SEB23-3FG`** dans `AirbnbLogement_Compta.csv`. Décision : location combinée des 2 apparts Sebastopol I (SEB23-3F) + II (SEB23-3G) → nouveau code dédié, pas de split 50/50, pas de route vers un compte existant. Master file `Mapping_appart_code.csv` également mis à jour (+ complétion des suffixes 411AIRBNB/411BOOKING/etc. sur les ~140 logements pour matcher la convention PennyLane).
- **Décision archi compta** : 3 pipelines DWH → PennyLane custom (option B), pas Mews → PennyLane natif. ADR complet dans `Archides/docs/decisions.md`. Vue d'ensemble dans `Archides/docs/compta-vision.md`. Doc HTML refondue dans `Archides/docs/compta-architecture.html` (multi-audience CEO/Comptable/IT).
- **Run réussi Airbnb 12:55 UTC** : 104 résas, 2 warnings, balance OK, 12 batches PennyLane, 128 lignes BQ trace, revision `booking-pipeline-00046-jp6`.

### 2026-04-16 — Couverture BQ vérifiée + fix appartements_snapshot
- Vérifié couverture `fct_reservations` pour Booking.com et Airbnb : `channel_number` 100%, `apartment_name` ~100%, `customer_name` ~99%
- `P09-CAR7-0G` manquait dans `sheets_raw.appartements_snapshot` → UPDATE manuel `Nom_Appartement = 'Merveil - Family Suite - Opera - Cardinal Mercier'`
- Confirmé : aucun export Mews supplémentaire nécessaire pour Booking.com et Airbnb — rapport de comptabilité + BQ suffisent

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
