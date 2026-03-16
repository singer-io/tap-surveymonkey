"""Test that sync resumes correctly from a saved bookmark."""
from base import SurveyMonkeyBaseTest

INTERRUPTED_BOOKMARK = "2023-01-15T00:00:00.000000Z"


class SurveyMonkeyInterruptedSyncTest(SurveyMonkeyBaseTest):
    """Verify a resuming sync replays records from the saved bookmark onwards."""

    def test_resumes_from_bookmark(self):
        """Records at-or-after the interrupted bookmark are re-written; state persisted."""
        records = [
            {"id": "2", "date_modified": "2023-01-15T00:00:00.000000Z"},  # at bookmark → written
            {"id": "3", "date_modified": "2023-01-20T00:00:00.000000Z"},  # after        → written
        ]
        state = {"bookmarks": {"surveys": {"full_sync": INTERRUPTED_BOOKMARK}}}
        write_record, write_state = self._run_sync(
            "surveys", records, bookmark=INTERRUPTED_BOOKMARK, state=state
        )
        self.assertEqual(write_record.call_count, 2)
        write_state.assert_called()
