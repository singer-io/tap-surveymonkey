import os
import unittest
from unittest.mock import MagicMock, patch

from singer.catalog import Catalog
from singer import metadata

from tap_surveymonkey.discover import discover, get_schemas, get_abs_path, _apply_access_checks, _prune_inaccessible_children
from tap_surveymonkey.exceptions import SurveyMonkeyForbiddenError
from tap_surveymonkey.streams import STREAMS


def _make_accessible_client():
    """Return a mock client where every stream is accessible (no 403)."""
    client = MagicMock()
    client.make_request.return_value = {"data": [], "links": {}}
    return client


def _make_forbidden_client():
    """Return a mock client that always raises 403."""
    client = MagicMock()
    client.make_request.side_effect = SurveyMonkeyForbiddenError("HTTP 403: Forbidden")
    return client


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
        cat = discover(_make_accessible_client())
        self.assertIsInstance(cat, Catalog)

    def test_catalog_contains_all_streams(self):
        """discover() catalog has an entry for every stream in STREAMS."""
        cat = discover(_make_accessible_client())
        tap_stream_ids = {s.tap_stream_id for s in cat.streams}
        for stream_name in STREAMS:
            with self.subTest(stream=stream_name):
                self.assertIn(stream_name, tap_stream_ids)

    def test_catalog_entry_tap_stream_id_equals_stream(self):
        """tap_stream_id and stream name are identical on every catalog entry."""
        cat = discover(_make_accessible_client())
        for entry in cat.streams:
            self.assertEqual(entry.tap_stream_id, entry.stream)

    def test_catalog_entry_schema_contains_key_property_fields(self):
        """Every key_property field defined on a stream class appears in the catalog schema."""
        cat = discover(_make_accessible_client())
        for entry in cat.streams:
            stream_obj = STREAMS[entry.tap_stream_id]
            schema_props = entry.schema.to_dict().get("properties", {})
            for key_prop in (stream_obj.key_properties or []):
                with self.subTest(stream=entry.tap_stream_id, key_prop=key_prop):
                    self.assertIn(key_prop, schema_props)

    def test_catalog_entry_schema_has_properties(self):
        """Every catalog entry schema contains a 'properties' key."""
        cat = discover(_make_accessible_client())
        for entry in cat.streams:
            with self.subTest(stream=entry.tap_stream_id):
                self.assertIn("properties", entry.schema.to_dict())

    def test_catalog_stream_count_matches_streams_dict(self):
        """Number of catalog entries equals the number of entries in STREAMS."""
        cat = discover(_make_accessible_client())
        self.assertEqual(len(cat.streams), len(STREAMS))

    def test_catalog_entry_has_key_properties(self):
        """Every catalog entry exposes key_properties (added in PR #44)."""
        cat = discover(_make_accessible_client())
        for entry in cat.streams:
            with self.subTest(stream=entry.tap_stream_id):
                # CatalogEntry stores key_properties; access via attribute or dict
                entry_dict = entry.to_dict()
                self.assertIn("key_properties", entry_dict)

    def test_catalog_entry_key_properties_match_stream_class(self):
        """key_properties in each catalog entry matches the stream class definition (PR #44)."""
        cat = discover(_make_accessible_client())
        for entry in cat.streams:
            with self.subTest(stream=entry.tap_stream_id):
                expected = STREAMS[entry.tap_stream_id].key_properties
                self.assertEqual(entry.key_properties, expected)

    def test_catalog_entry_key_properties_is_list(self):
        """key_properties on every catalog entry is a list (or None for full-table streams)."""
        cat = discover(_make_accessible_client())
        for entry in cat.streams:
            with self.subTest(stream=entry.tap_stream_id):
                self.assertIsInstance(entry.key_properties, (list, type(None)))


class TestCheckAccess(unittest.TestCase):
    """Tests for Stream.check_access() method."""

    def test_parent_stream_accessible(self):
        """check_access returns True when the API responds successfully."""
        client = _make_accessible_client()
        stream_obj = STREAMS["surveys"]
        self.assertTrue(stream_obj.check_access(client))

    def test_parent_stream_forbidden(self):
        """check_access returns False when the API raises 403."""
        client = _make_forbidden_client()
        stream_obj = STREAMS["surveys"]
        self.assertFalse(stream_obj.check_access(client))

    def test_child_stream_always_returns_true(self):
        """check_access returns True for child streams regardless of client."""
        client = _make_forbidden_client()
        for name, stream_obj in STREAMS.items():
            if stream_obj.parent is not None:
                with self.subTest(stream=name):
                    self.assertTrue(stream_obj.check_access(client))

    def test_child_stream_does_not_call_api(self):
        """check_access for child streams does not make any API call."""
        client = _make_forbidden_client()
        for name, stream_obj in STREAMS.items():
            if stream_obj.parent is not None:
                with self.subTest(stream=name):
                    client.reset_mock()
                    stream_obj.check_access(client)
                    client.make_request.assert_not_called()

    def test_forbidden_logs_warning_with_stream_id_and_error(self):
        """check_access logs a warning with stream_id and error message on 403."""
        client = _make_forbidden_client()
        stream_obj = STREAMS["surveys"]
        with patch("tap_surveymonkey.streams.LOGGER") as mock_logger:
            stream_obj.check_access(client)
            mock_logger.warning.assert_called_once_with(
                "Unauthorized Stream: %s, excluding from catalog. HTTP-Error-Message: '%s'",
                "surveys",
                unittest.mock.ANY,
            )


class TestApplyAccessChecks(unittest.TestCase):
    """Tests for _apply_access_checks() in discover.py."""

    def test_all_accessible_keeps_all_streams(self):
        """When all streams are accessible, schemas/metadata are unchanged."""
        schemas, field_metadata = get_schemas()
        original_keys = set(schemas.keys())
        _apply_access_checks(_make_accessible_client(), schemas, field_metadata)
        self.assertEqual(set(schemas.keys()), original_keys)

    def test_forbidden_parent_excludes_children(self):
        """When 'surveys' is forbidden, all child streams are also excluded."""
        schemas, field_metadata = get_schemas()
        with self.assertRaises(SurveyMonkeyForbiddenError):
            _apply_access_checks(_make_forbidden_client(), schemas, field_metadata)
        # surveys is the only parent; all children depend on it
        self.assertEqual(len(schemas), 0)

    def test_all_forbidden_raises_with_message(self):
        """When no streams are accessible, the error message mentions credentials."""
        schemas, field_metadata = get_schemas()
        with self.assertRaises(SurveyMonkeyForbiddenError) as ctx:
            _apply_access_checks(_make_forbidden_client(), schemas, field_metadata)
        self.assertIn(
            "No streams are accessible. Ensure the credentials have read permission for at least one stream.",
            str(ctx.exception),
        )

    def test_schemas_and_metadata_stay_in_sync(self):
        """schemas and field_metadata always have the same keys after access checks."""
        schemas, field_metadata = get_schemas()
        _apply_access_checks(_make_accessible_client(), schemas, field_metadata)
        self.assertEqual(set(schemas.keys()), set(field_metadata.keys()))

    def test_partial_forbidden_logs_excluded_streams(self):
        """When some parent streams are forbidden, a warning lists the excluded streams."""
        schemas, field_metadata = get_schemas()

        # Add a fake accessible parent so that not ALL schemas are empty after pruning
        schemas["_fake_accessible"] = {"properties": {}}
        field_metadata["_fake_accessible"] = []

        # Client that forbids 'surveys' but allows the fake stream
        from tap_surveymonkey.streams import Stream
        fake_stream = Stream(stream_id="_fake_accessible", path="fake")

        def selective_make_request(endpoint, **kwargs):
            if endpoint == "surveys":
                raise SurveyMonkeyForbiddenError("HTTP 403: Forbidden")
            return {"data": [], "links": {}}

        client = MagicMock()
        client.make_request.side_effect = selective_make_request

        with patch("tap_surveymonkey.discover.STREAMS", {**STREAMS, "_fake_accessible": fake_stream}):
            with patch("tap_surveymonkey.discover.LOGGER") as mock_logger:
                _apply_access_checks(client, schemas, field_metadata)
                mock_logger.warning.assert_any_call(
                    "These streams have been excluded due to HTTP-Error-Code:403 Forbidden: %s",
                    unittest.mock.ANY,
                )


class TestPruneInaccessibleChildren(unittest.TestCase):
    """Tests for _prune_inaccessible_children() in discover.py."""

    def test_children_removed_when_parent_missing(self):
        """Child streams are removed if their parent is not in schemas."""
        schemas, field_metadata = get_schemas()
        # Remove the parent
        schemas.pop("surveys", None)
        field_metadata.pop("surveys", None)
        _prune_inaccessible_children(schemas, field_metadata)
        # All remaining streams have parent="surveys", so all should be pruned
        self.assertEqual(len(schemas), 0)
        self.assertEqual(len(field_metadata), 0)

    def test_children_kept_when_parent_present(self):
        """Child streams are kept when their parent is still in schemas."""
        schemas, field_metadata = get_schemas()
        original_keys = set(schemas.keys())
        _prune_inaccessible_children(schemas, field_metadata)
        self.assertEqual(set(schemas.keys()), original_keys)

    def test_logs_warning_for_each_pruned_child(self):
        """A warning is logged for each child stream removed due to missing parent."""
        schemas, field_metadata = get_schemas()
        schemas.pop("surveys", None)
        field_metadata.pop("surveys", None)
        child_streams = [name for name, obj in STREAMS.items() if obj.parent == "surveys"]
        with patch("tap_surveymonkey.discover.LOGGER") as mock_logger:
            _prune_inaccessible_children(schemas, field_metadata)
            self.assertEqual(mock_logger.warning.call_count, len(child_streams))
            for call_args in mock_logger.warning.call_args_list:
                self.assertEqual(
                    call_args[0][0],
                    "Stream '%s' excluded from catalog because its parent stream '%s' is not accessible.",
                )
