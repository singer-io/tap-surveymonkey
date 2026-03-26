import os
import unittest
from unittest.mock import patch, MagicMock

from singer.catalog import Catalog
from singer import metadata

from tap_surveymonkey.discover import discover, get_schemas, get_abs_path
from tap_surveymonkey.streams import STREAMS


class TestGetAbsPath(unittest.TestCase):

    def test_returns_absolute_path(self):
        """get_abs_path returns an absolute filesystem path."""
        result = get_abs_path("schemas/surveys.json")
        self.assertTrue(os.path.isabs(result))

    def test_path_ends_with_filename(self):
        """get_abs_path includes the passed filename at the end of the result."""
        result = get_abs_path("schemas/surveys.json")
        self.assertTrue(result.endswith("surveys.json"))

    def test_different_filenames_produce_different_paths(self):
        """get_abs_path produces distinct paths for distinct filenames."""
        path_a = get_abs_path("schemas/surveys.json")
        path_b = get_abs_path("schemas/responses.json")
        self.assertNotEqual(path_a, path_b)


class TestGetSchemas(unittest.TestCase):

    def test_all_streams_present_in_schemas(self):
        """get_schemas returns a schema entry for every stream defined in STREAMS."""
        schemas, _ = get_schemas()
        for stream_name in STREAMS:
            self.assertIn(stream_name, schemas)

    def test_all_streams_present_in_metadata(self):
        """get_schemas returns a metadata entry for every stream defined in STREAMS."""
        _, schemas_metadata = get_schemas()
        for stream_name in STREAMS:
            self.assertIn(stream_name, schemas_metadata)

    def test_schema_metadata_keys_match(self):
        """get_schemas returns the same keys in schemas and metadata dicts."""
        schemas, schemas_metadata = get_schemas()
        self.assertEqual(set(schemas.keys()), set(schemas_metadata.keys()))

    def test_each_schema_is_a_dict(self):
        """Each schema value returned is a dictionary (JSON Schema object)."""
        schemas, _ = get_schemas()
        for stream_name, schema in schemas.items():
            with self.subTest(stream=stream_name):
                self.assertIsInstance(schema, dict)

    def test_incremental_stream_replication_key_is_automatic(self):
        """Replication key field has inclusion=automatic for incremental streams."""
        _, schemas_metadata = get_schemas()
        # 'surveys' is INCREMENTAL with replication_key='date_modified'
        surveys_meta = metadata.to_map(schemas_metadata["surveys"])
        inclusion = metadata.get(surveys_meta, ("properties", "date_modified"), "inclusion")
        self.assertEqual(inclusion, "automatic")

    def test_survey_details_replication_key_is_automatic(self):
        """survey_details replication key has inclusion=automatic."""
        _, schemas_metadata = get_schemas()
        details_meta = metadata.to_map(schemas_metadata["survey_details"])
        inclusion = metadata.get(details_meta, ("properties", "date_modified"), "inclusion")
        self.assertEqual(inclusion, "automatic")

    def test_metadata_entries_are_lists(self):
        """Metadata for each stream is returned as a list (singer metadata list format)."""
        _, schemas_metadata = get_schemas()
        for stream_name, meta in schemas_metadata.items():
            with self.subTest(stream=stream_name):
                self.assertIsInstance(meta, list)


class TestDiscover(unittest.TestCase):

    def test_returns_catalog_instance(self):
        """discover() returns a singer Catalog object."""
        cat = discover()
        self.assertIsInstance(cat, Catalog)

    def test_catalog_contains_all_streams(self):
        """discover() catalog has an entry for every stream in STREAMS."""
        cat = discover()
        tap_stream_ids = {s.tap_stream_id for s in cat.streams}
        for stream_name in STREAMS:
            with self.subTest(stream=stream_name):
                self.assertIn(stream_name, tap_stream_ids)

    def test_catalog_entry_tap_stream_id_equals_stream(self):
        """tap_stream_id and stream name are identical on every catalog entry."""
        cat = discover()
        for entry in cat.streams:
            self.assertEqual(entry.tap_stream_id, entry.stream)

    def test_catalog_entry_schema_contains_key_property_fields(self):
        """Every key_property field defined on a stream class appears in the catalog schema."""
        cat = discover()
        for entry in cat.streams:
            stream_obj = STREAMS[entry.tap_stream_id]
            schema_props = entry.schema.to_dict().get("properties", {})
            for key_prop in (stream_obj.key_properties or []):
                with self.subTest(stream=entry.tap_stream_id, key_prop=key_prop):
                    self.assertIn(key_prop, schema_props)

    def test_catalog_entry_schema_has_properties(self):
        """Every catalog entry schema contains a 'properties' key."""
        cat = discover()
        for entry in cat.streams:
            with self.subTest(stream=entry.tap_stream_id):
                self.assertIn("properties", entry.schema.to_dict())

    def test_catalog_stream_count_matches_streams_dict(self):
        """Number of catalog entries equals the number of entries in STREAMS."""
        cat = discover()
        self.assertEqual(len(cat.streams), len(STREAMS))
