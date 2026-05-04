"""Integration tests for tap-surveymonkey stream discovery with mocked data.

discover() reads schema JSON files from disk — no HTTP calls are made.
Run with: python -m pytest tests/test_discovery.py -v
"""
import sys
import os
import unittest

from singer import metadata

sys.path.insert(0, os.path.dirname(__file__))
from base import SurveyMonkeyBaseTest

from tap_surveymonkey.discover import discover
from tap_surveymonkey.streams import STREAMS


class SurveyMonkeyDiscoveryTest(SurveyMonkeyBaseTest, unittest.TestCase):
    """Verify tap discovery returns the correct catalog structure."""

    def _get_catalog(self):
        """Run discover() — reads schema files, no HTTP required."""
        return discover()

    # ── Stream presence ──────────────────────────────────────────────────────

    def test_discovery_returns_all_expected_streams(self):
        """discover() returns a catalog entry for every stream in expected_metadata."""
        catalog = self._get_catalog()
        expected = set(self.expected_metadata().keys())
        discovered = {s.tap_stream_id for s in catalog.streams}
        self.assertEqual(discovered, expected)

    def test_discovery_stream_count_matches_streams_dict(self):
        """Number of catalog entries equals the number of entries in STREAMS."""
        catalog = self._get_catalog()
        self.assertEqual(len(catalog.streams), len(STREAMS))

    def test_discovery_tap_stream_id_equals_stream_name(self):
        """tap_stream_id and stream name are identical on every catalog entry."""
        catalog = self._get_catalog()
        for entry in catalog.streams:
            with self.subTest(stream=entry.tap_stream_id):
                self.assertEqual(entry.tap_stream_id, entry.stream)

    # ── Schema structure ─────────────────────────────────────────────────────

    def test_discovery_every_schema_has_properties(self):
        """Every discovered stream schema contains a 'properties' key with fields."""
        catalog = self._get_catalog()
        for entry in catalog.streams:
            with self.subTest(stream=entry.tap_stream_id):
                schema_dict = entry.schema.to_dict()
                self.assertIn("properties", schema_dict)
                self.assertGreater(len(schema_dict["properties"]), 0)

    def test_discovery_id_field_present_in_all_schemas(self):
        """Every stream schema has an 'id' field (the primary key)."""
        catalog = self._get_catalog()
        for entry in catalog.streams:
            with self.subTest(stream=entry.tap_stream_id):
                schema_props = entry.schema.to_dict()["properties"]
                self.assertIn("id", schema_props)

    def test_discovery_surveys_schema_has_date_modified(self):
        """surveys schema contains the 'date_modified' replication key field."""
        catalog = self._get_catalog()
        surveys_entry = next(e for e in catalog.streams if e.tap_stream_id == "surveys")
        props = surveys_entry.schema.to_dict()["properties"]
        self.assertIn("date_modified", props)

    def test_discovery_survey_details_schema_has_date_modified(self):
        """survey_details schema contains the 'date_modified' replication key field."""
        catalog = self._get_catalog()
        entry = next(e for e in catalog.streams if e.tap_stream_id == "survey_details")
        props = entry.schema.to_dict()["properties"]
        self.assertIn("date_modified", props)

    def test_discovery_responses_schema_has_date_modified(self):
        """responses schema contains the 'date_modified' replication key field."""
        catalog = self._get_catalog()
        entry = next(e for e in catalog.streams if e.tap_stream_id == "responses")
        props = entry.schema.to_dict()["properties"]
        self.assertIn("date_modified", props)

    # ── Replication metadata ─────────────────────────────────────────────────

    def test_discovery_all_streams_are_incremental(self):
        """All streams have forced-replication-method=INCREMENTAL in their metadata."""
        catalog = self._get_catalog()
        expected = self.expected_metadata()
        for entry in catalog.streams:
            with self.subTest(stream=entry.tap_stream_id):
                mdata = metadata.to_map(entry.metadata)
                actual = (
                    metadata.get(mdata, (), "forced-replication-method")
                    or metadata.get(mdata, (), "replication-method")
                )
                self.assertEqual(
                    actual,
                    expected[entry.tap_stream_id][self.REPLICATION_METHOD],
                )

    def test_discovery_date_modified_has_automatic_inclusion(self):
        """date_modified has inclusion=automatic in INCREMENTAL stream metadata."""
        catalog = self._get_catalog()
        for entry in catalog.streams:
            with self.subTest(stream=entry.tap_stream_id):
                mdata = metadata.to_map(entry.metadata)
                inclusion = metadata.get(mdata, ("properties", "date_modified"), "inclusion")
                self.assertEqual(inclusion, "automatic")

    def test_discovery_metadata_entries_are_lists(self):
        """Entry metadata is returned as a list (singer metadata list format)."""
        catalog = self._get_catalog()
        for entry in catalog.streams:
            with self.subTest(stream=entry.tap_stream_id):
                self.assertIsInstance(entry.metadata, list)

    # ── Key-property fields ──────────────────────────────────────────────────

    def test_discovery_id_field_in_schema_for_all_streams(self):
        """The 'id' primary key field appears in the schema properties of every stream."""
        catalog = self._get_catalog()
        expected = self.expected_metadata()
        for entry in catalog.streams:
            with self.subTest(stream=entry.tap_stream_id):
                pks = expected[entry.tap_stream_id][self.PRIMARY_KEYS]
                schema_props = entry.schema.to_dict()["properties"]
                for pk in pks:
                    self.assertIn(pk, schema_props)
