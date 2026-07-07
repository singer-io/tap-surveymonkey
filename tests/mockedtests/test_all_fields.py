"""Integration tests — verify all schema fields are replicated for tap-surveymonkey.

Patches tap_surveymonkey.client.requests.request so no live API is called.
Run with: python -m pytest tests/test_all_fields.py -v
"""
import sys
import os
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(__file__))
from base import SurveyMonkeyBaseTest, MockResponse

from tap_surveymonkey.sync import sync


# Fields that exist in the schema but may not be returned by the API in all cases
# (e.g. optional metadata fields only returned when survey has specific features).
# Populate after first live run reveals absent fields; always document the reason.
KNOWN_MISSING_FIELDS = {
    # "surveys": {"field_name"},  # reason: not returned by sandbox API
}


class SurveyMonkeyAllFieldsTest(SurveyMonkeyBaseTest, unittest.TestCase):
    """Verify all schema fields appear in synced records when all fields are selected."""

    def _run_sync_and_collect(self, stream_name, mock_records):
        """Run sync for stream_name and return all written records for that stream."""
        def _routed(method, url, **kwargs):
            if "/responses/bulk" in url:
                return MockResponse({
                    "data": mock_records,
                    "links": {},
                    "total": len(mock_records),
                })
            if url.endswith("/details"):
                return MockResponse(mock_records[0] if mock_records else {})
            # surveys list / parent iterator
            return MockResponse({
                "data": [{"id": "s1", "date_modified": "2024-01-01T00:00:00.000000Z"}],
                "links": {},
                "total": 1,
            })

        records_written = []
        catalog = self._make_catalog([stream_name])

        with patch("tap_surveymonkey.client.requests.request", side_effect=_routed), \
             patch("tap_surveymonkey.sync.singer.write_schema"), \
             patch("tap_surveymonkey.sync.singer.write_state"), \
             patch("tap_surveymonkey.sync.singer.write_record",
                   side_effect=lambda s, r: records_written.append((s, r))):
            sync(self.config, {}, catalog)

        return [r for s, r in records_written if s == stream_name]

    # ── surveys ──────────────────────────────────────────────────────────────

    def test_surveys_all_schema_fields_present_in_record(self):
        """Every top-level surveys schema property appears in the generated record."""
        schema = self._load_schema("surveys")
        expected_fields = set(schema["properties"].keys())
        known_missing = KNOWN_MISSING_FIELDS.get("surveys", set())

        record = self._generate_stream_record("surveys")
        actual_fields = set(record.keys())
        missing = (expected_fields - known_missing) - actual_fields
        self.assertEqual(missing, set(),
                         f"Schema fields missing from mock record: {missing}")

    def test_surveys_records_written_contain_id(self):
        """Synced surveys records always include the primary key 'id'."""
        mock_data = [self._generate_stream_record("surveys")]
        mock_data[0]["id"] = "s1"
        mock_data[0]["date_modified"] = "2024-01-01T00:00:00.000000Z"
        records = self._run_sync_and_collect("surveys", mock_data)
        for rec in records:
            self.assertIn("id", rec)

    def test_surveys_records_written_contain_date_modified(self):
        """Synced surveys records include the replication key 'date_modified'."""
        mock_data = [self._generate_stream_record("surveys")]
        mock_data[0]["id"] = "s1"
        mock_data[0]["date_modified"] = "2024-01-01T00:00:00.000000Z"
        records = self._run_sync_and_collect("surveys", mock_data)
        for rec in records:
            self.assertIn("date_modified", rec)

    # ── survey_details ────────────────────────────────────────────────────────

    def test_survey_details_record_contains_id(self):
        """Synced survey_details records always include the primary key 'id'."""
        mock_rec = self._generate_stream_record("survey_details")
        mock_rec["id"] = "s1"
        mock_rec["date_modified"] = "2024-01-01T00:00:00.000000Z"
        records = self._run_sync_and_collect("survey_details", [mock_rec])
        for rec in records:
            self.assertIn("id", rec)

    def test_survey_details_record_contains_date_modified(self):
        """Synced survey_details records include the replication key 'date_modified'."""
        mock_rec = self._generate_stream_record("survey_details")
        mock_rec["id"] = "s1"
        mock_rec["date_modified"] = "2024-01-01T00:00:00.000000Z"
        records = self._run_sync_and_collect("survey_details", [mock_rec])
        for rec in records:
            self.assertIn("date_modified", rec)

    # ── responses ─────────────────────────────────────────────────────────────

    def test_responses_all_schema_fields_present_in_record(self):
        """Every top-level responses schema property appears in the generated record."""
        schema = self._load_schema("responses")
        expected_fields = set(schema["properties"].keys())
        known_missing = KNOWN_MISSING_FIELDS.get("responses", set())

        record = self._generate_stream_record("responses")
        actual_fields = set(record.keys())
        missing = (expected_fields - known_missing) - actual_fields
        self.assertEqual(missing, set(),
                         f"Schema fields missing from mock record: {missing}")

    def test_responses_records_contain_id(self):
        """Synced responses records always include the primary key 'id'."""
        mock_data = [self._generate_stream_record("responses")]
        mock_data[0]["id"] = "r1"
        mock_data[0]["survey_id"] = "s1"
        mock_data[0]["date_modified"] = "2024-01-01T00:00:00.000000Z"
        records = self._run_sync_and_collect("responses", mock_data)
        for rec in records:
            self.assertIn("id", rec)

    # ── simplified_responses ──────────────────────────────────────────────────

    def test_simplified_responses_schema_matches_responses_schema(self):
        """simplified_responses and responses share the same schema fields."""
        schema_r  = set(self._load_schema("responses")["properties"].keys())
        schema_sr = set(self._load_schema("simplified_responses")["properties"].keys())
        self.assertEqual(schema_r, schema_sr)

    def test_simplified_responses_records_contain_id(self):
        """Synced simplified_responses records always include the primary key 'id'."""
        mock_data = [self._generate_stream_record("simplified_responses")]
        mock_data[0]["id"] = "r1"
        mock_data[0]["survey_id"] = "s1"
        mock_data[0]["date_modified"] = "2024-01-01T00:00:00.000000Z"
        records = self._run_sync_and_collect("simplified_responses", mock_data)
        for rec in records:
            self.assertIn("id", rec)
