"""Integration tests — verify automatic fields are replicated even with no selection.

Patches tap_surveymonkey.client.requests.request so no live API is called.
Run with: python -m pytest tests/test_automatic_fields.py -v
"""
import sys
import os
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(__file__))
from base import SurveyMonkeyBaseTest, MockResponse

from tap_surveymonkey.sync import sync


class SurveyMonkeyAutomaticFieldsTest(SurveyMonkeyBaseTest, unittest.TestCase):
    """Verify that primary keys and replication keys always appear in synced records.

    These are 'automatic' fields that must be present regardless of field selection.
    """

    def _run_sync_for_stream(self, stream_name, mock_record):
        """Run sync for one stream and return the list of written records."""
        def _routed(method, url, **kwargs):
            if url.endswith("/details"):
                return MockResponse(mock_record)
            if "/responses/bulk" in url:
                return MockResponse({
                    "data": [mock_record],
                    "links": {},
                    "total": 1,
                })
            # surveys list / parent iterator
            return MockResponse({
                "data": [{"id": "s1", "date_modified": "2024-01-01T00:00:00.000000Z"}],
                "links": {},
                "total": 1,
            })

        catalog = self._make_catalog([stream_name])
        records_written = []

        with patch("tap_surveymonkey.client.requests.request", side_effect=_routed), \
             patch("tap_surveymonkey.sync.singer.write_schema"), \
             patch("tap_surveymonkey.sync.singer.write_state"), \
             patch("tap_surveymonkey.sync.singer.write_record",
                   side_effect=lambda s, r: records_written.append((s, r))):
            sync(self.config, {}, catalog)

        return [r for s, r in records_written if s == stream_name]

    # ── surveys ──────────────────────────────────────────────────────────────

    def test_surveys_primary_key_always_present(self):
        """surveys: primary key 'id' is present in every synced record."""
        rec = self._generate_stream_record("surveys")
        rec["id"] = "s1"
        rec["date_modified"] = "2024-01-01T00:00:00.000000Z"
        records = self._run_sync_for_stream("surveys", rec)
        for r in records:
            self.assertIn("id", r)

    def test_surveys_replication_key_always_present(self):
        """surveys: replication key 'date_modified' is present in every synced record."""
        rec = self._generate_stream_record("surveys")
        rec["id"] = "s1"
        rec["date_modified"] = "2024-01-01T00:00:00.000000Z"
        records = self._run_sync_for_stream("surveys", rec)
        for r in records:
            self.assertIn("date_modified", r)

    # ── survey_details ────────────────────────────────────────────────────────

    def test_survey_details_primary_key_always_present(self):
        """survey_details: primary key 'id' is in every synced record."""
        rec = self._generate_stream_record("survey_details")
        rec["id"] = "s1"
        rec["date_modified"] = "2024-01-01T00:00:00.000000Z"
        records = self._run_sync_for_stream("survey_details", rec)
        for r in records:
            self.assertIn("id", r)

    def test_survey_details_replication_key_always_present(self):
        """survey_details: replication key 'date_modified' is in every synced record."""
        rec = self._generate_stream_record("survey_details")
        rec["id"] = "s1"
        rec["date_modified"] = "2024-01-01T00:00:00.000000Z"
        records = self._run_sync_for_stream("survey_details", rec)
        for r in records:
            self.assertIn("date_modified", r)

    # ── responses ─────────────────────────────────────────────────────────────

    def test_responses_primary_key_always_present(self):
        """responses: primary key 'id' is in every synced record."""
        rec = self._generate_stream_record("responses")
        rec["id"] = "r1"
        rec["survey_id"] = "s1"
        rec["date_modified"] = "2024-01-01T00:00:00.000000Z"
        records = self._run_sync_for_stream("responses", rec)
        for r in records:
            self.assertIn("id", r)

    def test_responses_replication_key_always_present(self):
        """responses: replication key 'date_modified' is in every synced record."""
        rec = self._generate_stream_record("responses")
        rec["id"] = "r1"
        rec["survey_id"] = "s1"
        rec["date_modified"] = "2024-01-01T00:00:00.000000Z"
        records = self._run_sync_for_stream("responses", rec)
        for r in records:
            self.assertIn("date_modified", r)

    # ── simplified_responses ──────────────────────────────────────────────────

    def test_simplified_responses_primary_key_always_present(self):
        """simplified_responses: primary key 'id' is in every synced record."""
        rec = self._generate_stream_record("simplified_responses")
        rec["id"] = "r1"
        rec["survey_id"] = "s1"
        rec["date_modified"] = "2024-01-01T00:00:00.000000Z"
        records = self._run_sync_for_stream("simplified_responses", rec)
        for r in records:
            self.assertIn("id", r)

    def test_simplified_responses_replication_key_always_present(self):
        """simplified_responses: replication key 'date_modified' is in every synced record."""
        rec = self._generate_stream_record("simplified_responses")
        rec["id"] = "r1"
        rec["survey_id"] = "s1"
        rec["date_modified"] = "2024-01-01T00:00:00.000000Z"
        records = self._run_sync_for_stream("simplified_responses", rec)
        for r in records:
            self.assertIn("date_modified", r)

    # ── automatic fields metadata ─────────────────────────────────────────────

    def test_discovery_date_modified_has_automatic_inclusion_metadata(self):
        """date_modified carries inclusion=automatic in the Singer catalog metadata."""
        from unittest.mock import MagicMock
        from singer import metadata as md
        from tap_surveymonkey.discover import discover

        catalog = discover(MagicMock())
        for entry in catalog.streams:
            with self.subTest(stream=entry.tap_stream_id):
                mdata = md.to_map(entry.metadata)
                inclusion = md.get(mdata, ("properties", "date_modified"), "inclusion")
                self.assertEqual(inclusion, "automatic")

    def test_discovery_id_has_inclusion_metadata(self):
        """'id' field carries an inclusion type in the Singer catalog metadata."""
        from unittest.mock import MagicMock
        from singer import metadata as md
        from tap_surveymonkey.discover import discover

        catalog = discover(MagicMock())
        for entry in catalog.streams:
            with self.subTest(stream=entry.tap_stream_id):
                mdata = md.to_map(entry.metadata)
                inclusion = md.get(mdata, ("properties", "id"), "inclusion")
                self.assertIsNotNone(inclusion)
