-- Write-ahead journal des POST Pennylane (idempotence rejeu).
-- Voir bigquery/journal.py pour le contrat intent/posted.
-- Exécution (one-shot) :
--   bq query --use_legacy_sql=false < bigquery/create_posting_journal.sql

CREATE TABLE IF NOT EXISTS `merveil-data-warehouse.pennylane.posting_journal` (
  logged_at        TIMESTAMP NOT NULL,
  run_id           STRING,
  ota              STRING    NOT NULL,   -- 'booking' | 'airbnb' | futures sources
  source_file      STRING,               -- nom du fichier Drive
  file_hash        STRING,               -- md5 du contenu (un fichier corrigé = nouveau journal)
  batch_key        STRING    NOT NULL,   -- payout_id Booking / payout_reference Airbnb
  batch_index      INT64,                -- position dans le run (info, aligné raw_postings)
  phase            STRING    NOT NULL,   -- 'intent' (avant POST) | 'posted' (POST + trace OK)
  mode             STRING    NOT NULL,   -- 'live' | 'test' | 'bq_only'
  ledger_entry_id  STRING,               -- id Pennylane (phase posted, live uniquement)
  label            STRING,               -- libellé de l'écriture (aide à la vérif manuelle Pennylane)
  service_version  STRING
)
OPTIONS (description = 'Write-ahead journal des POST Pennylane du booking-pipeline. Append-only, 2 lignes/batch (intent puis posted). intent sans posted = état incertain, le pipeline se bloque au replay.');
