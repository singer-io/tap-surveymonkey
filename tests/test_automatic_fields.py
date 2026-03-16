"""Test that automatic fields (pk + replication key) are always replicated."""
from base import SurveyMonkeyBaseTest


class SurveyMonkeyAutomaticFieldsTest(SurveyMonkeyBaseTest):
    """Verify pk and replication key are written even with minimal field selection."""

    def test_automatic_fields_always_replicated(self):
        """Sync with only pk+rep-key in the record; assert both appear in output."""
        record = {"id": "1", "date_modified": "2023-01-15T00:00:00.000000Z"}
        write_record, _ = self._run_sync("surveys", [record])
        self.assertEqual(write_record.call_count, 1)
        written = write_record.call_args[0][1]
        self.assertIn("id", written)
        self.assertIn("date_modified", written)
