"""
Unit tests for discovery, schemas, and the main entrypoint.

Covers: get_schemas(), discover(), get_abs_path(), schema file validation,
schema metadata, and the main() entrypoint (discover/sync modes).
"""
import json
import os
import unittest
from unittest.mock import Mock, patch
from singer.catalog import Catalog
from tap_surveymonkey import main, REQUIRED_CONFIG_KEYS
from tap_surveymonkey.discover import discover, get_schemas, get_abs_path


EXPECTED_STREAMS = [
    "surveys", "survey_details", "responses", "simplified_responses"
]


# ---------------------------------------------------------------------------
# get_abs_path
# ---------------------------------------------------------------------------

class TestGetAbsPath(unittest.TestCase):

    def test_returns_string(self):
        self.assertIsInstance(get_abs_path("schemas/surveys.json"), str)

    def test_contains_relative_path(self):
        result = get_abs_path("schemas/surveys.json")
        self.assertIn("schemas", result)
        self.assertIn("surveys.json", result)

    def test_all_schema_paths(self):
        for name in EXPECTED_STREAMS:
            result = get_abs_path(f"schemas/{name}.json")
            self.assertIsInstance(result, str)


# ---------------------------------------------------------------------------
# Schema files (real files on disk)
# ---------------------------------------------------------------------------

class TestSchemaFiles(unittest.TestCase):

    def test_all_schema_files_exist(self):
        for name in EXPECTED_STREAMS:
            path = get_abs_path(f"schemas/{name}.json")
            self.assertTrue(os.path.exists(path), f"Missing: {name}.json")

    def test_all_schemas_are_valid_json(self):
        for name in EXPECTED_STREAMS:
            path = get_abs_path(f"schemas/{name}.json")
            with open(path) as f:
                try:
                    json.load(f)
                except json.JSONDecodeError as e:
                    self.fail(f"{name}.json is not valid JSON: {e}")

    def test_all_schemas_have_type_object(self):
        schemas, _ = get_schemas()
        for name, schema in schemas.items():
            self.assertEqual(schema.get("type"), "object", f"{name} type mismatch")

    def test_all_schemas_have_properties(self):
        schemas, _ = get_schemas()
        for name, schema in schemas.items():
            self.assertIn("properties", schema, f"{name} missing properties")
            self.assertIsInstance(schema["properties"], dict)

    def test_all_schemas_have_id_field(self):
        schemas, _ = get_schemas()
        for name, schema in schemas.items():
            props = schema.get("properties", {})
            self.assertIn("id", props, f"{name} missing id field")

    def test_surveys_has_required_fields(self):
        schemas, _ = get_schemas()
        props = schemas["surveys"]["properties"]
        for field in ("id", "date_modified"):
            self.assertIn(field, props, f"surveys missing {field}")

    def test_responses_has_required_fields(self):
        schemas, _ = get_schemas()
        props = schemas["responses"]["properties"]
        for field in ("id", "date_modified"):
            self.assertIn(field, props, f"responses missing {field}")

    def test_simplified_responses_has_required_fields(self):
        schemas, _ = get_schemas()
        props = schemas["simplified_responses"]["properties"]
        for field in ("id", "date_modified"):
            self.assertIn(field, props, f"simplified_responses missing {field}")

    def test_date_modified_types_consistent(self):
        schemas, _ = get_schemas()
        types = set()
        for schema in schemas.values():
            dm = schema["properties"].get("date_modified", {})
            if "type" in dm:
                types.add(str(dm["type"]))
        self.assertLessEqual(len(types), 1, "Inconsistent date_modified types")


# ---------------------------------------------------------------------------
# get_schemas()
# ---------------------------------------------------------------------------

class TestGetSchemas(unittest.TestCase):

    def test_returns_two_dicts(self):
        schemas, meta = get_schemas()
        self.assertIsInstance(schemas, dict)
        self.assertIsInstance(meta, dict)

    def test_loads_all_four_streams(self):
        schemas, meta = get_schemas()
        for name in EXPECTED_STREAMS:
            self.assertIn(name, schemas, f"schemas missing {name}")
            self.assertIn(name, meta, f"metadata missing {name}")

    def test_schemas_and_metadata_same_keys(self):
        schemas, meta = get_schemas()
        self.assertEqual(set(schemas.keys()), set(meta.keys()))

    def test_metadata_is_list_per_stream(self):
        _, meta = get_schemas()
        for name, entries in meta.items():
            self.assertIsInstance(entries, list, f"metadata[{name}] not a list")

    def test_metadata_has_root_breadcrumb(self):
        _, meta = get_schemas()
        for name, entries in meta.items():
            roots = [e for e in entries if e.get("breadcrumb") == ()]
            self.assertGreater(len(roots), 0,
                               f"{name} metadata missing root breadcrumb")

    def test_all_streams_incremental(self):
        _, meta = get_schemas()
        for name in EXPECTED_STREAMS:
            root = next(e for e in meta[name] if e.get("breadcrumb") == ())
            self.assertEqual(
                root["metadata"]["forced-replication-method"], "INCREMENTAL",
                f"{name} should be INCREMENTAL",
            )

    def test_replication_key_marked_automatic(self):
        _, meta = get_schemas()
        for name in EXPECTED_STREAMS:
            entries = meta[name]
            key_meta = next(
                (e for e in entries
                 if e.get("breadcrumb") == ("properties", "date_modified")),
                None,
            )
            self.assertIsNotNone(key_meta, f"No date_modified metadata in {name}")
            self.assertEqual(key_meta["metadata"]["inclusion"], "automatic")

    def test_id_types_consistent_across_schemas(self):
        schemas, _ = get_schemas()
        types = {str(s["properties"]["id"].get("type")) for s in schemas.values()}
        self.assertEqual(len(types), 1, "Inconsistent id field types")

    @patch("tap_surveymonkey.discover.utils.load_json")
    @patch("tap_surveymonkey.discover.get_abs_path")
    def test_mocked_load_returns_tuple(self, mock_path, mock_load):
        mock_path.side_effect = lambda x: f"/fake/{x}"
        mock_load.return_value = {
            "type": "object",
            "properties": {"id": {"type": "string"}, "date_modified": {"type": "string"}}
        }
        schemas, meta = get_schemas()
        self.assertIsInstance(schemas, dict)
        self.assertIsInstance(meta, dict)
        self.assertEqual(len(schemas), len(EXPECTED_STREAMS))

    @patch("tap_surveymonkey.discover.utils.load_json")
    @patch("tap_surveymonkey.discover.get_abs_path")
    def test_mocked_incremental_metadata(self, mock_path, mock_load):
        mock_path.side_effect = lambda x: f"/fake/{x}"
        mock_load.return_value = {
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "date_modified": {"type": "string", "format": "date-time"},
            }
        }
        _, meta = get_schemas()
        for name in EXPECTED_STREAMS:
            dm_entry = next(
                (e for e in meta[name]
                 if e.get("breadcrumb") == ("properties", "date_modified")),
                None,
            )
            self.assertIsNotNone(dm_entry)
            self.assertEqual(dm_entry["metadata"]["inclusion"], "automatic")


# ---------------------------------------------------------------------------
# discover()
# ---------------------------------------------------------------------------

class TestDiscover(unittest.TestCase):

    @patch("tap_surveymonkey.discover.get_schemas")
    def test_returns_catalog(self, mock_gs):
        mock_gs.return_value = (
            {"surveys": {"type": "object"}, "responses": {"type": "object"}},
            {"surveys": [], "responses": []},
        )
        result = discover()
        self.assertIsInstance(result, Catalog)

    @patch("tap_surveymonkey.discover.get_schemas")
    def test_catalog_has_correct_stream_count(self, mock_gs):
        schemas = {n: {"type": "object"} for n in EXPECTED_STREAMS}
        meta = {n: [] for n in EXPECTED_STREAMS}
        mock_gs.return_value = (schemas, meta)
        result = discover()
        self.assertEqual(len(result.streams), 4)

    @patch("tap_surveymonkey.discover.get_schemas")
    def test_catalog_stream_ids_correct(self, mock_gs):
        schemas = {n: {"type": "object"} for n in EXPECTED_STREAMS}
        meta = {n: [] for n in EXPECTED_STREAMS}
        mock_gs.return_value = (schemas, meta)
        ids = {s.tap_stream_id for s in discover().streams}
        self.assertEqual(ids, set(EXPECTED_STREAMS))

    @patch("tap_surveymonkey.discover.get_schemas")
    def test_catalog_entry_has_stream_and_schema(self, mock_gs):
        mock_gs.return_value = (
            {"surveys": {"type": "object", "properties": {"id": {"type": "string"}}}},
            {"surveys": [{"breadcrumb": [], "metadata": {"selected": False}}]},
        )
        stream_entry = discover().streams[0]
        self.assertEqual(stream_entry.tap_stream_id, "surveys")
        self.assertEqual(stream_entry.stream, "surveys")
        self.assertIsNotNone(stream_entry.schema)
        self.assertIsNotNone(stream_entry.metadata)

    def test_real_discover_returns_four_streams(self):
        catalog = discover()
        ids = {s.tap_stream_id for s in catalog.streams}
        self.assertEqual(ids, set(EXPECTED_STREAMS))


# ---------------------------------------------------------------------------
# main() entrypoint
# ---------------------------------------------------------------------------

class TestMainEntrypoint(unittest.TestCase):
    """Tests for main() using correct patch paths after __init__.py fix."""

    def _args(self, discover_mode=False, catalog=None, state=None):
        args = Mock()
        args.discover = discover_mode
        args.config = {"access_token": "tok", "start_date": "2021-01-01T00:00:00Z"}
        args.state = state or {}
        args.catalog = catalog
        return args

    @patch("tap_surveymonkey.discover.discover")
    @patch("singer.utils.parse_args")
    def test_discover_mode_calls_discover(self, mock_pa, mock_discover):
        mock_pa.return_value = self._args(discover_mode=True)
        mock_catalog = Mock()
        mock_catalog.dump.return_value = {}
        mock_discover.return_value = mock_catalog

        main()

        mock_discover.assert_called_once()
        mock_catalog.dump.assert_called_once()

    @patch("tap_surveymonkey.sync.sync")
    @patch("tap_surveymonkey.discover.discover")
    @patch("singer.utils.parse_args")
    def test_sync_mode_with_provided_catalog(self, mock_pa, mock_discover, mock_sync):
        provided = Mock(spec=Catalog)
        mock_pa.return_value = self._args(catalog=provided)

        main()

        mock_sync.assert_called_once_with(
            mock_pa.return_value.config,
            mock_pa.return_value.state,
            provided,
        )
        mock_discover.assert_not_called()

    @patch("tap_surveymonkey.sync.sync")
    @patch("tap_surveymonkey.discover.discover")
    @patch("singer.utils.parse_args")
    def test_sync_mode_without_catalog_auto_discovers(self, mock_pa, mock_discover, mock_sync):
        mock_pa.return_value = self._args()
        auto_cat = Mock(spec=Catalog)
        mock_discover.return_value = auto_cat

        main()

        mock_discover.assert_called_once()
        mock_sync.assert_called_once_with(
            mock_pa.return_value.config,
            mock_pa.return_value.state,
            auto_cat,
        )

    @patch("singer.utils.parse_args")
    def test_missing_config_raises_system_exit(self, mock_pa):
        mock_pa.side_effect = SystemExit(1)
        with self.assertRaises(SystemExit):
            main()

    @patch("tap_surveymonkey.sync.sync")
    @patch("singer.utils.parse_args")
    def test_sync_receives_empty_state(self, mock_pa, mock_sync):
        mock_pa.return_value = self._args(catalog=Mock(spec=Catalog), state={})
        main()
        call_state = mock_sync.call_args[0][1]
        self.assertEqual(call_state, {})

    @patch("tap_surveymonkey.sync.sync")
    @patch("singer.utils.parse_args")
    def test_sync_receives_bookmarked_state(self, mock_pa, mock_sync):
        state = {"bookmarks": {"surveys": {"full_sync": "2023-01-01T00:00:00Z"}}}
        mock_pa.return_value = self._args(catalog=Mock(spec=Catalog), state=state)
        main()
        call_state = mock_sync.call_args[0][1]
        self.assertIn("bookmarks", call_state)


# ---------------------------------------------------------------------------
# REQUIRED_CONFIG_KEYS
# ---------------------------------------------------------------------------

class TestRequiredConfigKeys(unittest.TestCase):

    def test_is_list(self):
        self.assertIsInstance(REQUIRED_CONFIG_KEYS, list)

    def test_contains_access_token(self):
        self.assertIn("access_token", REQUIRED_CONFIG_KEYS)

    def test_contains_start_date(self):
        self.assertIn("start_date", REQUIRED_CONFIG_KEYS)


if __name__ == "__main__":
    unittest.main()
