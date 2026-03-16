"""Test that all selected fields are replicated."""
from base import SurveyMonkeyBaseTest


class SurveyMonkeyAllFieldsTest(SurveyMonkeyBaseTest):
    """Verify every schema field reaches the target when all fields are selected."""

    FULL_RECORD = {
        "id": "1",
        "title": "My Survey",
        "nickname": "test",
        "date_created": "2023-01-01T00:00:00.000000Z",
        "date_modified": "2023-01-15T00:00:00.000000Z",
        "language": "en",
        "question_count": 5,
        "response_count": 100,
    }

    def test_all_fields_replicated(self):
        """Sync surveys with a full record; assert every field reaches the target."""
        write_record, _ = self._run_sync("surveys", [self.FULL_RECORD])
        self.assertEqual(write_record.call_count, 1)
        written = write_record.call_args[0][1]
        for field in self.FULL_RECORD:
            self.assertIn(field, written)
