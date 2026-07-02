"""PennyLane API client — posts accounting entries via the v2 API."""

import logging
from typing import List, Optional

import requests

from accounting.entries import AccountingEntry
from config.settings import PENNYLANE_ACCOUNT_IDS, PENNYLANE_JOURNAL_IDS

logger = logging.getLogger(__name__)

_BASE_URL = "https://app.pennylane.com/api/external/v2"


class PennyLaneClient:
    """
    Posts balanced sets of AccountingEntry objects to PennyLane as ledger entries.

    One call to post_ledger_entry() = one écriture comptable in PennyLane
    (one balanced group: DEBIT bank + DEBIT supplier + N×CREDIT client).

    Usage
    -----
    client = PennyLaneClient(token=os.environ["PENNYLANE_TOKEN"])
    client.post_ledger_entry(entries)          # live
    client.post_ledger_entry(entries, dry_run=True)  # validate only, no write
    """

    def __init__(self, token: str):
        self._session = requests.Session()
        self._session.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def post_ledger_entry(
        self,
        entries: List[AccountingEntry],
        dry_run: bool = False,
    ) -> dict:
        """
        Post a balanced set of AccountingEntry objects as one PennyLane ledger entry.

        All entries must share the same journal code and processing date.
        The first entry's label is used as the overall ledger entry label.

        Returns a dict with:
            ledger_entry_id     : PennyLane id (None on dry_run)
            ledger_entry_lines  : list aligned with `entries`, each
                                  {ledger_entry_line_id, ledger_account_id}
            raw                 : raw API response (or dry_run summary)
            dry_run             : bool

        Raises ValueError if a journal or account ID cannot be resolved.
        Raises requests.HTTPError on API errors.
        """
        if not entries:
            raise ValueError("Empty entries list.")

        journal_code = entries[0].journal
        journal_id = PENNYLANE_JOURNAL_IDS.get(journal_code)
        if journal_id is None:
            raise ValueError(
                f"No PennyLane journal ID for code '{journal_code}'. "
                f"Known codes: {list(PENNYLANE_JOURNAL_IDS)}"
            )

        entry_date = entries[0].date.strftime("%Y-%m-%d")
        label = entries[0].label

        lines = []
        account_ids: List[int] = []
        for e in entries:
            account_id = self._resolve_account_id(e.account, journal_code)
            account_ids.append(account_id)
            lines.append({
                "debit":             f"{e.debit:.2f}"  if e.debit  is not None else "0.00",
                "credit":            f"{e.credit:.2f}" if e.credit is not None else "0.00",
                "ledger_account_id": account_id,
                "label":             e.label,
            })

        payload = {
            "date":               entry_date,
            "label":              label,
            "journal_id":         journal_id,
            "ledger_entry_lines": lines,
        }

        if dry_run:
            total_debit  = sum(float(l["debit"])  for l in lines)
            total_credit = sum(float(l["credit"]) for l in lines)
            logger.info(
                "[DRY RUN] journal=%s date=%s label='%s' lines=%d "
                "debit=%.2f credit=%.2f balanced=%s",
                journal_code, entry_date, label, len(lines),
                total_debit, total_credit,
                abs(total_debit - total_credit) < 0.01,
            )
            return {
                "dry_run":             True,
                "ledger_entry_id":     None,
                "ledger_entry_lines":  [
                    {"ledger_entry_line_id": None, "ledger_account_id": aid}
                    for aid in account_ids
                ],
                "raw": {
                    "journal":  journal_code,
                    "date":     entry_date,
                    "lines":    len(lines),
                    "balanced": abs(total_debit - total_credit) < 0.01,
                },
            }

        # timeout : un hang réseau sans limite = requête tuée par Cloud Run en
        # plein run (= le scénario crash mi-run que le journal protège).
        response = self._session.post(f"{_BASE_URL}/ledger_entries", json=payload, timeout=30)
        response.raise_for_status()
        raw = response.json()

        api_lines = raw.get("ledger_entry_lines", [])
        if len(api_lines) != len(entries):
            logger.warning(
                "PennyLane returned %d lines but we sent %d — line id mapping may be off.",
                len(api_lines), len(entries),
            )
        line_results = []
        for i, _e in enumerate(entries):
            api_line = api_lines[i] if i < len(api_lines) else {}
            line_results.append({
                "ledger_entry_line_id": api_line.get("id"),
                "ledger_account_id":    api_line.get("ledger_account_id", account_ids[i]),
            })

        logger.info(
            "Posted ledger entry id=%s journal=%s date=%s lines=%d",
            raw.get("id"), journal_code, entry_date, len(lines),
        )
        return {
            "dry_run":             False,
            "ledger_entry_id":     raw.get("id"),
            "ledger_entry_lines":  line_results,
            "raw":                 raw,
        }

    # NOTE: post_batches() a été supprimé (2026-07-02) — la boucle de POST vit
    # dans orchestrator.run_pipeline(), journalisée batch par batch dans
    # pennylane.posting_journal (write-ahead). Poster en masse sans journal
    # est exactement le chemin qui produisait des doublons au replay.

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _resolve_account_id(self, account_code: str, journal_code: str) -> int:
        """
        Resolve an internal account code to a PennyLane ledger_account_id.

        The bank account "51105000" maps to different PennyLane accounts
        depending on the OTA (51105 for Booking, 51104 for Airbnb), so it
        uses a journal-qualified key "51105000_BOOK" / "51105000_AIRB".
        """
        key = f"{account_code}_{journal_code}" if account_code == "51105000" else account_code
        account_id = PENNYLANE_ACCOUNT_IDS.get(key)
        if account_id is None:
            raise ValueError(
                f"No PennyLane account ID for '{account_code}' (journal={journal_code}). "
                f"Add it to PENNYLANE_ACCOUNT_IDS in config/settings.py."
            )
        return account_id
