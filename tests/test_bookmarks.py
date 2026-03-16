"""Test bookmark-based incremental replication."""
from base import SurveyMonkeyBaseTest

BOOKMARK = "2023-01-16T00:00:00.000000Z"


class SurveyMonkeyBookmarksTest(SurveyMonkeyBaseTest):
    """Verify records before the bookmark are skipped; state is persisted."""

    def test_bookmark_filters_old_records(self):
        """Only records at-or-after the bookmark are written; state is updated."""
        records = [
            {"id": "1", "date_modified": "2023-01-15T00:00:00.000000Z"},  # before → skipped
            {"id": "2", "date_modified": "2023-01-16T00:00:00.000000Z"},  # at    → written
            {"id": "3", "date_modified": "2023-01-17T00:00:00.000000Z"},  # after → written
        ]
        state = {"bookmarks": {"surveys": {"full_sync": BOOKMARK}}}
        write_record, write_state = self._run_sync(
            "surveys", records, bookmark=BOOKMARK, state=state
        )
        self.assertEqual(write_record.call_count, 2)
        write_state.assert_called()
