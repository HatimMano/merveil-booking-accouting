"""Tests du journal write-ahead (idempotence des POST Pennylane).

Couvre la logique replay de orchestrator.run_pipeline :
  - run nominal : intent → POST → trace → posted, pour chaque batch
  - replay complet : tous les batches `posted` → 0 POST, archive quand même
  - replay partiel : seuls les batches non `posted` sont postés
  - état incertain (`intent` orphelin) : run bloqué, 0 POST, pas d'archive
  - crash mi-run : le journal capture l'état exact ; le replay skippe le fait
    et bloque sur l'incertain
  - force=True : re-poste tout malgré le journal

Tout est mocké (BQ journal, raw_postings, client Pennylane, lookup Mews,
Drive) — on teste l'ordonnancement et les décisions, pas les I/O.
"""

from datetime import date
from decimal import Decimal

import pytest

import orchestrator
from models.reservation import Reservation
from parsers.booking import BookingPayoutBatch
from sources.base import Source, SourceFetchResult

PROCESSING_DATE = date(2026, 6, 15)


# ---------------------------------------------------------------------------
# Fixtures / fakes
# ---------------------------------------------------------------------------

def _resa(ref: str, payout_id: str) -> Reservation:
    # Équilibre : net + |commission| + |payment_charge| = amount − |city_tax|
    # 89 + 10 + 1 = 100 = 102 − 2 ✓
    return Reservation(
        source_file="payout.xlsx",
        ref_appart="3015679",
        payout_id=payout_id,
        reference_number=ref,
        check_in=date(2026, 6, 1),
        checkout=date(2026, 6, 3),
        guest_name="Test Guest",
        reservation_status="ok",
        currency="EUR",
        payment_status="by_booking",
        city_tax=Decimal("-2.00"),
        amount=Decimal("102.00"),
        commission=Decimal("-10.00"),
        payment_charge=Decimal("-1.00"),
        net=Decimal("89.00"),
        payout_date=date(2026, 6, 10),
    )


class StubSource(Source):
    """Source en mémoire : 3 batches d'1 résa (payout P0/P1/P2)."""

    name = "booking"
    entries_kwargs = {"per_reservation_fees": True}

    def __init__(self, n_batches: int = 3):
        self._batches = [
            BookingPayoutBatch(
                payout_id=f"P{i}",
                payout_date=date(2026, 6, 10),
                reservations=[_resa(ref=f"500000{i}", payout_id=f"P{i}")],
            )
            for i in range(n_batches)
        ]

    def fetch(self, processing_date):
        return SourceFetchResult(
            batches=self._batches,
            anomalies=[],
            mapping={"3015679": "TEST-0G"},
            source_file="payout.xlsx",
            archive_file_ids=["drive-file-id"],
            drive_folder_id="drive-folder-id",
            file_hash="abc123hash",
        )


class FakeDrive:
    def __init__(self):
        self.archived = []
        self.anomaly_sheets = []

    def get_or_create_folder(self, parent_id, name):
        return "archive-folder-id"

    def move_file(self, fid, dest, src):
        self.archived.append(fid)

    def create_anomaly_sheet(self, folder_id, name, rows):
        self.anomaly_sheets.append((folder_id, name, len(rows) - 1))


class FakePennyLane:
    """Compte les POST ; peut lever une exception au i-ème appel (crash simulé)."""

    def __init__(self, crash_at: int | None = None):
        self.posted_labels = []
        self.crash_at = crash_at
        self._next_id = 1000

    def post_ledger_entry(self, entries, dry_run=False):
        if self.crash_at is not None and len(self.posted_labels) == self.crash_at:
            raise ConnectionError("simulated network crash mid-run")
        self.posted_labels.append(entries[0].label)
        self._next_id += 1
        return {
            "dry_run": False,
            "ledger_entry_id": self._next_id,
            "ledger_entry_lines": [
                {"ledger_entry_line_id": self._next_id * 10 + j, "ledger_account_id": 1}
                for j, _ in enumerate(entries)
            ],
        }


class JournalRecorder:
    """Journal en mémoire, même contrat que bigquery.journal."""

    def __init__(self, initial_state: dict | None = None):
        self.state = dict(initial_state or {})   # batch_key -> phase résolue
        self.phases_written = []                 # [(batch_key, phase)] dans l'ordre

    def read_journal(self, **kwargs):
        return dict(self.state)

    def write_phase(self, *, batch_key, phase, **kwargs):
        self.phases_written.append((batch_key, phase))
        # 'posted' écrase 'intent' (résolution comme LOGICAL_OR en BQ)
        if self.state.get(batch_key) != "posted":
            self.state[batch_key] = phase


@pytest.fixture
def env(monkeypatch):
    """Monte tous les fakes sur l'orchestrateur ; retourne les poignées."""
    journal = JournalRecorder()
    pennylane = FakePennyLane()
    drive = FakeDrive()
    postings_calls = []

    monkeypatch.setattr(orchestrator, "lookup_bills", lambda targets: {})
    monkeypatch.setattr(orchestrator, "read_journal", journal.read_journal)
    monkeypatch.setattr(orchestrator, "write_phase", journal.write_phase)
    monkeypatch.setattr(orchestrator, "_get_pennylane_client", lambda: pennylane)
    monkeypatch.setattr(orchestrator.time, "sleep", lambda s: None)

    def fake_write_postings(**kwargs):
        postings_calls.append(kwargs["first_batch_index"])
        return sum(len(b) for b in kwargs["per_batch_entries"])

    monkeypatch.setattr(orchestrator, "write_postings", fake_write_postings)

    class Env:
        pass

    e = Env()
    e.journal, e.pennylane, e.drive, e.postings_calls = journal, pennylane, drive, postings_calls
    return e


def _run(env, source=None, **kwargs):
    return orchestrator.run_pipeline(
        source=source or StubSource(),
        processing_date=PROCESSING_DATE,
        drive_client=env.drive,
        run_id="test-run",
        **kwargs,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_nominal_posts_all_batches_with_journal_order(env):
    result = _run(env)

    assert result["status"] == "ok"
    assert result["pennylane_batches_posted"] == 3
    assert result["batches_skipped_journal"] == 0
    assert len(env.pennylane.posted_labels) == 3
    # Ordre par batch : intent puis posted, jamais entrelacé
    assert env.journal.phases_written == [
        ("P0", "intent"), ("P0", "posted"),
        ("P1", "intent"), ("P1", "posted"),
        ("P2", "intent"), ("P2", "posted"),
    ]
    # Trace raw_postings par batch, aux bons index
    assert env.postings_calls == [0, 1, 2]
    assert env.drive.archived == ["drive-file-id"]


def test_full_replay_skips_everything(env):
    env.journal.state = {"P0": "posted", "P1": "posted", "P2": "posted"}

    result = _run(env)

    assert result["status"] == "ok"
    assert result["pennylane_batches_posted"] == 0
    assert result["batches_skipped_journal"] == 3
    assert env.pennylane.posted_labels == []          # zéro POST
    assert env.journal.phases_written == []           # zéro écriture journal
    assert env.drive.archived == ["drive-file-id"]    # mais on ré-archive


def test_partial_replay_posts_only_missing_batches(env):
    env.journal.state = {"P0": "posted", "P2": "posted"}

    result = _run(env)

    assert result["pennylane_batches_posted"] == 1
    assert result["batches_skipped_journal"] == 2
    assert env.journal.phases_written == [("P1", "intent"), ("P1", "posted")]
    assert env.postings_calls == [1]                  # batch_index réel conservé


def test_uncertain_intent_blocks_run_no_post_no_archive(env):
    env.journal.state = {"P0": "posted", "P1": "intent"}

    result = _run(env)

    assert result["status"] == "journal_blocked"
    assert result["uncertain_batches"] == ["P1"]
    assert env.pennylane.posted_labels == []          # RIEN posté
    assert env.journal.phases_written == []
    assert env.drive.archived == []                   # fichier laissé en place
    assert len(env.drive.anomaly_sheets) == 1         # anomalie visible ops


def test_crash_mid_run_then_replay(env):
    # Run 1 : crash réseau au moment du POST du batch P1 (après P0 réussi)
    env.pennylane.crash_at = 1
    with pytest.raises(ConnectionError):
        _run(env)

    assert env.journal.state == {"P0": "posted", "P1": "intent"}
    assert env.drive.archived == []                   # crash avant archive

    # Run 2 (replay) : P0 skippé, P1 incertain → bloqué, toujours zéro re-POST
    env.pennylane.crash_at = None
    result = _run(env)
    assert result["status"] == "journal_blocked"
    assert result["uncertain_batches"] == ["P1"]
    assert env.pennylane.posted_labels == ["Payout P0 3015679"] or len(env.pennylane.posted_labels) == 1


def test_force_reposts_everything_despite_journal(env):
    env.journal.state = {"P0": "posted", "P1": "intent", "P2": "posted"}

    result = _run(env, force=True)

    assert result["status"] == "ok"
    assert result["pennylane_batches_posted"] == 3
    assert result["batches_skipped_journal"] == 0
    assert len(env.pennylane.posted_labels) == 3


def test_bq_only_journals_in_bq_only_mode_without_client(env):
    result = _run(env, bq_only=True)

    assert result["status"] == "ok"
    assert env.pennylane.posted_labels == []          # jamais de POST en bq_only
    assert result["bq_rows_written"] > 0
    assert env.journal.phases_written[0] == ("P0", "intent")


def test_duplicate_batch_keys_abort(env):
    class DupSource(StubSource):
        def fetch(self, processing_date):
            r = super().fetch(processing_date)
            for b in r.batches:
                b.payout_id = "SAME"
            return r

    with pytest.raises(ValueError, match="non uniques"):
        _run(env, source=DupSource())
