"""PennyLane API client — posts accounting entries via the v2 API."""

import logging
from typing import List

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

        Returns the API response dict, or a dry_run summary dict.
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
        label = entries[0].label  # bank header line label

        lines = []
        for e in entries:
            account_id = self._resolve_account_id(e.account, journal_code)
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
                "dry_run":  True,
                "journal":  journal_code,
                "date":     entry_date,
                "lines":    len(lines),
                "balanced": abs(total_debit - total_credit) < 0.01,
            }

        response = self._session.post(f"{_BASE_URL}/ledger_entries", json=payload)
        response.raise_for_status()
        result = response.json()
        logger.info(
            "Posted ledger entry id=%s journal=%s date=%s lines=%d",
            result.get("id"), journal_code, entry_date, len(lines),
        )
        return result

    def post_batches(
        self,
        batches: List[List[AccountingEntry]],
        dry_run: bool = False,
    ) -> List[dict]:
        """Post multiple entry batches sequentially. Returns one result per batch."""
        results = []
        for i, batch in enumerate(batches, 1):
            logger.info("Posting batch %d/%d (%d lines)...", i, len(batches), len(batch))
            results.append(self.post_ledger_entry(batch, dry_run=dry_run))
        return results

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
