"""
Unit tests for sync behavior and bookmark logic.

Covers: client init, schema/record/state writes, child-stream iteration,
bookmark filtering, per-record state updates, full-sync bookmark writing,
max-bookmark tracking, and Transformer usage.
"""
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch, MagicMock, call

from singer.catalog import Catalog
from tap_surveymonkey.sync import sync


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BASE_CONFIG = {
    "access_token": "test_token",
    "start_date": "2021-01-01T00:00:00Z",
}

_SIMPLE_SCHEMA = {
    "type": "object",
    "properties": {
        "id": {"type": "string"},
        "date_modified": {"type": "string"},
    },
}


def _mock_catalog_stream(tap_stream_id, schema=None):
    """Return a minimal mock CatalogEntry."""
    s = Mock()
    s.tap_stream_id = tap_stream_id
    s.stream = tap_stream_id
    s.key_properties = ["id"]
    s.schema = SimpleNamespace(to_dict=lambda: schema or _SIMPLE_SCHEMA)
    s.metadata = []
    return s


def _mock_stream_obj(stream_id="surveys", parent=None, records=None,
                     rep_key_from_parent=False, is_sorted=True):
    """Return a minimal mock stream object."""
    obj = Mock()
    obj.stream_id = stream_id
    obj.replication_key = "date_modified"
    obj.is_sorted = is_sorted
    obj.parent_stream = parent
    obj.replication_key_from_parent = rep_key_from_parent
    obj.fetch_data.return_value = records if records is not None else []
    return obj


# ---------------------------------------------------------------------------
# Core sync tests
# ---------------------------------------------------------------------------

class TestSyncCore(unittest.TestCase):
    """Test basic sync() behavior."""

    def setUp(self):
        self.config = dict(_BASE_CONFIG)

    @patch("tap_surveymonkey.sync.SurveyMonkeyClient")
    def test_initializes_client_with_token(self, mock_cls):
        sync(self.config, {}, Catalog([]))
        mock_cls.assert_called_once_with("test_token")

    @patch("tap_surveymonkey.sync.SurveyMonkeyClient")
    @patch("tap_surveymonkey.sync.singer.write_schema")
    def test_writes_schema_for_selected_stream(self, mock_write_schema, mock_cls):
        catalog = Mock()
        catalog.get_selected_streams.return_value = [_mock_catalog_stream("surveys")]
        stream_obj = _mock_stream_obj()

        with patch("tap_surveymonkey.sync.STREAMS", {"surveys": stream_obj}):
            sync(self.config, {}, catalog)

        mock_write_schema.assert_called_once()
        self.assertEqual(mock_write_schema.call_args[1]["stream_name"], "surveys")

    @patch("tap_surveymonkey.sync.SurveyMonkeyClient")
    @patch("tap_surveymonkey.sync.singer.write_schema")
    @patch("tap_surveymonkey.sync.singer.write_record")
    @patch("tap_surveymonkey.sync.singer.write_state")
    @patch("tap_surveymonkey.sync.bookmarks")
    def test_writes_records(self, mock_bkm, mock_ws, mock_wr, mock_wsch, mock_cls):
        mock_bkm.get_bookmark.return_value = None
        mock_bkm.write_bookmark.return_value = {}
        catalog = Mock()
        catalog.get_selected_streams.return_value = [_mock_catalog_stream("surveys")]
        stream_obj = _mock_stream_obj(records=[
            {"id": "1", "date_modified": "2023-01-15T00:00:00.000000Z"},
            {"id": "2", "date_modified": "2023-01-16T00:00:00.000000Z"},
        ])

        with patch("tap_surveymonkey.sync.STREAMS", {"surveys": stream_obj}):
            sync(self.config, {}, catalog)

        self.assertEqual(mock_wr.call_count, 2)

    @patch("tap_surveymonkey.sync.SurveyMonkeyClient")
    @patch("tap_surveymonkey.sync.singer.write_schema")
    @patch("tap_surveymonkey.sync.singer.write_record")
    @patch("tap_surveymonkey.sync.singer.write_state")
    @patch("tap_surveymonkey.sync.bookmarks")
    def test_writes_state_after_sync(self, mock_bkm, mock_ws, mock_wr, mock_wsch, mock_cls):
        mock_bkm.get_bookmark.return_value = None
        mock_bkm.write_bookmark.return_value = {"bookmarks": {"surveys": {}}}
        catalog = Mock()
        catalog.get_selected_streams.return_value = [_mock_catalog_stream("surveys")]
        stream_obj = _mock_stream_obj(records=[
            {"id": "1", "date_modified": "2023-01-15T00:00:00.000000Z"}
        ])

        with patch("tap_surveymonkey.sync.STREAMS", {"surveys": stream_obj}):
            sync(self.config, {}, catalog)

        self.assertGreater(mock_ws.call_count, 0)


# ---------------------------------------------------------------------------
# Child-stream iteration
# ---------------------------------------------------------------------------

class TestChildStreamSync(unittest.TestCase):

    @patch("tap_surveymonkey.sync.SurveyMonkeyClient")
    @patch("tap_surveymonkey.sync.singer.write_schema")
    @patch("tap_surveymonkey.sync.singer.write_record")
    @patch("tap_surveymonkey.sync.singer.write_state")
    @patch("tap_surveymonkey.sync.bookmarks")
    def test_parent_fetched_once_child_fetched_per_parent(
            self, mock_bkm, mock_ws, mock_wr, mock_wsch, mock_cls):
        mock_bkm.get_bookmark.return_value = None
        mock_bkm.write_bookmark.return_value = {}

        parent = Mock()
        parent.fetch_data.return_value = [{"id": "s1"}, {"id": "s2"}]

        stream_obj = _mock_stream_obj(
            stream_id="survey_details",
            parent=parent,
            rep_key_from_parent=True,
            records=[{"id": "d1", "date_modified": "2023-01-15T00:00:00.000000Z"}],
        )

        catalog = Mock()
        catalog.get_selected_streams.return_value = [
            _mock_catalog_stream("survey_details")
        ]

        with patch("tap_surveymonkey.sync.STREAMS", {"survey_details": stream_obj}):
            sync(_BASE_CONFIG, {}, catalog)

        parent.fetch_data.assert_called_once()
        self.assertEqual(stream_obj.fetch_data.call_count, 2)


# ---------------------------------------------------------------------------
# Bookmark filtering
# ---------------------------------------------------------------------------

class TestBookmarkFiltering(unittest.TestCase):

    @patch("tap_surveymonkey.sync.SurveyMonkeyClient")
    @patch("tap_surveymonkey.sync.singer.write_schema")
    @patch("tap_surveymonkey.sync.singer.write_record")
    @patch("tap_surveymonkey.sync.singer.write_state")
    @patch("tap_surveymonkey.sync.bookmarks")
    def test_filters_records_before_bookmark(
            self, mock_bkm, mock_ws, mock_wr, mock_wsch, mock_cls):
        bookmark = "2023-01-16T00:00:00.000000Z"
        mock_bkm.get_bookmark.return_value = bookmark
        mock_bkm.write_bookmark.return_value = {}

        catalog = Mock()
        catalog.get_selected_streams.return_value = [_mock_catalog_stream("surveys")]
        stream_obj = _mock_stream_obj(records=[
            {"id": "1", "date_modified": "2023-01-15T00:00:00.000000Z"},  # before
            {"id": "2", "date_modified": "2023-01-16T00:00:00.000000Z"},  # at
            {"id": "3", "date_modified": "2023-01-17T00:00:00.000000Z"},  # after
        ])

        with patch("tap_surveymonkey.sync.STREAMS", {"surveys": stream_obj}):
            sync(dict(_BASE_CONFIG), {}, catalog)

        self.assertEqual(mock_wr.call_count, 2)


# ---------------------------------------------------------------------------
# Bookmark writing
# ---------------------------------------------------------------------------

class TestBookmarkWriting(unittest.TestCase):

    @patch("tap_surveymonkey.sync.SurveyMonkeyClient")
    @patch("tap_surveymonkey.sync.singer.write_schema")
    @patch("tap_surveymonkey.sync.singer.write_record")
    @patch("tap_surveymonkey.sync.singer.write_state")
    @patch("tap_surveymonkey.sync.bookmarks")
    def test_full_sync_bookmark_written_for_surveys(
            self, mock_bkm, mock_ws, mock_wr, mock_wsch, mock_cls):
        mock_bkm.get_bookmark.return_value = None
        mock_bkm.write_bookmark.return_value = {}

        catalog = Mock()
        catalog.get_selected_streams.return_value = [_mock_catalog_stream("surveys")]
        stream_obj = _mock_stream_obj(records=[
            {"id": "1", "date_modified": "2023-01-15T00:00:00.000000Z"}
        ])

        with patch("tap_surveymonkey.sync.STREAMS", {"surveys": stream_obj}):
            sync(dict(_BASE_CONFIG), {}, catalog)

        write_calls = mock_bkm.write_bookmark.call_args_list
        self.assertGreater(len(write_calls), 0)
        last = write_calls[-1]
        # full_sync bookmark written for surveys stream (no survey_id in config)
        self.assertEqual(last[0][1], "surveys")
        self.assertEqual(last[0][2], "full_sync")

    @patch("tap_surveymonkey.sync.SurveyMonkeyClient")
    @patch("tap_surveymonkey.sync.singer.write_schema")
    @patch("tap_surveymonkey.sync.singer.write_record")
    @patch("tap_surveymonkey.sync.singer.write_state")
    @patch("tap_surveymonkey.sync.bookmarks")
    def test_sorted_response_stream_writes_per_record_state(
            self, mock_bkm, mock_ws, mock_wr, mock_wsch, mock_cls):
        mock_bkm.get_bookmark.return_value = None
        mock_bkm.write_bookmark.return_value = {}

        parent = Mock()
        parent.fetch_data.return_value = [{"id": "survey1"}]

        stream_obj = _mock_stream_obj(
            stream_id="responses",
            parent=parent,
            rep_key_from_parent=False,
            records=[
                {"id": "1", "survey_id": "survey1", "date_modified": "2023-01-15T00:00:00.000000Z"},
                {"id": "2", "survey_id": "survey1", "date_modified": "2023-01-16T00:00:00.000000Z"},
            ],
        )

        catalog = Mock()
        catalog.get_selected_streams.return_value = [_mock_catalog_stream("responses")]

        with patch("tap_surveymonkey.sync.STREAMS", {"responses": stream_obj}):
            sync(dict(_BASE_CONFIG), {}, catalog)

        # Per-record bookmarks for sorted child streams
        self.assertGreater(mock_ws.call_count, 0)

    @patch("tap_surveymonkey.sync.SurveyMonkeyClient")
    @patch("tap_surveymonkey.sync.singer.write_schema")
    @patch("tap_surveymonkey.sync.singer.write_record")
    @patch("tap_surveymonkey.sync.singer.write_state")
    @patch("tap_surveymonkey.sync.bookmarks")
    def test_bookmark_retrieved_from_state(
            self, mock_bkm, mock_ws, mock_wr, mock_wsch, mock_cls):
        mock_bkm.get_bookmark.return_value = "2023-01-15T00:00:00.000000Z"
        mock_bkm.write_bookmark.return_value = {}

        catalog = Mock()
        catalog.get_selected_streams.return_value = [_mock_catalog_stream("surveys")]
        stream_obj = _mock_stream_obj(records=[])

        state = {"bookmarks": {"surveys": {"full_sync": "2023-01-15T00:00:00.000000Z"}}}
        with patch("tap_surveymonkey.sync.STREAMS", {"surveys": stream_obj}):
            sync(dict(_BASE_CONFIG), state, catalog)

        mock_bkm.get_bookmark.assert_called()

    @patch("tap_surveymonkey.sync.SurveyMonkeyClient")
    @patch("tap_surveymonkey.sync.singer.write_schema")
    @patch("tap_surveymonkey.sync.singer.write_record")
    @patch("tap_surveymonkey.sync.singer.write_state")
    @patch("tap_surveymonkey.sync.bookmarks")
    def test_replication_key_from_parent_uses_parent_value(
            self, mock_bkm, mock_ws, mock_wr, mock_wsch, mock_cls):
        mock_bkm.get_bookmark.return_value = None
        mock_bkm.write_bookmark.return_value = {}

        parent = Mock()
        parent.fetch_data.return_value = [
            {"id": "s1", "date_modified": "2023-01-15T00:00:00.000000Z"}
        ]

        stream_obj = _mock_stream_obj(
            stream_id="survey_details",
            parent=parent,
            rep_key_from_parent=True,
            records=[{"id": "d1", "date_modified": "2023-01-15T00:00:00.000000Z"}],
        )

        catalog = Mock()
        catalog.get_selected_streams.return_value = [
            _mock_catalog_stream("survey_details")
        ]

        with patch("tap_surveymonkey.sync.STREAMS", {"survey_details": stream_obj}):
            sync(dict(_BASE_CONFIG), {}, catalog)

        mock_bkm.write_bookmark.assert_called()

    @patch("tap_surveymonkey.sync.SurveyMonkeyClient")
    @patch("tap_surveymonkey.sync.singer.write_schema")
    @patch("tap_surveymonkey.sync.singer.write_record")
    @patch("tap_surveymonkey.sync.singer.write_state")
    @patch("tap_surveymonkey.sync.bookmarks")
    def test_max_bookmark_tracked_across_records(
            self, mock_bkm, mock_ws, mock_wr, mock_wsch, mock_cls):
        mock_bkm.get_bookmark.return_value = None
        mock_bkm.write_bookmark.return_value = {}

        catalog = Mock()
        catalog.get_selected_streams.return_value = [_mock_catalog_stream("surveys")]
        stream_obj = _mock_stream_obj(records=[
            {"id": "1", "date_modified": "2023-01-15T00:00:00.000000Z"},
            {"id": "2", "date_modified": "2023-01-20T00:00:00.000000Z"},  # max
            {"id": "3", "date_modified": "2023-01-10T00:00:00.000000Z"},
        ])

        with patch("tap_surveymonkey.sync.STREAMS", {"surveys": stream_obj}):
            sync(dict(_BASE_CONFIG), {}, catalog)

        self.assertGreater(len(mock_bkm.write_bookmark.call_args_list), 0)

    @patch("tap_surveymonkey.sync.SurveyMonkeyClient")
    @patch("tap_surveymonkey.sync.singer.write_schema")
    @patch("tap_surveymonkey.sync.singer.write_record")
    @patch("tap_surveymonkey.sync.singer.write_state")
    @patch("tap_surveymonkey.sync.bookmarks")
    def test_empty_state_syncs_from_start_date(
            self, mock_bkm, mock_ws, mock_wr, mock_wsch, mock_cls):
        mock_bkm.get_bookmark.return_value = None
        mock_bkm.write_bookmark.return_value = {}

        catalog = Mock()
        catalog.get_selected_streams.return_value = [_mock_catalog_stream("surveys")]
        stream_obj = _mock_stream_obj(records=[
            {"id": "1", "date_modified": "2023-01-15T00:00:00.000000Z"}
        ])

        with patch("tap_surveymonkey.sync.STREAMS", {"surveys": stream_obj}):
            sync(dict(_BASE_CONFIG), {}, catalog)

        mock_bkm.get_bookmark.assert_called()
        self.assertGreater(mock_wr.call_count, 0)


# ---------------------------------------------------------------------------
# Logging and Transformer
# ---------------------------------------------------------------------------

class TestLoggingAndTransformer(unittest.TestCase):

    @patch("tap_surveymonkey.sync.SurveyMonkeyClient")
    @patch("tap_surveymonkey.sync.singer.write_schema")
    @patch("tap_surveymonkey.sync.singer.write_record")
    @patch("tap_surveymonkey.sync.singer.write_state")
    @patch("tap_surveymonkey.sync.bookmarks")
    @patch("tap_surveymonkey.sync.LOGGER")
    def test_logs_stream_name_at_start(
            self, mock_log, mock_bkm, mock_ws, mock_wr, mock_wsch, mock_cls):
        mock_bkm.get_bookmark.return_value = None
        mock_bkm.write_bookmark.return_value = {}
        catalog = Mock()
        catalog.get_selected_streams.return_value = [_mock_catalog_stream("surveys")]
        stream_obj = _mock_stream_obj()

        with patch("tap_surveymonkey.sync.STREAMS", {"surveys": stream_obj}):
            sync(dict(_BASE_CONFIG), {}, catalog)

        mock_log.info.assert_any_call("Syncing stream: surveys")

    @patch("tap_surveymonkey.sync.SurveyMonkeyClient")
    @patch("tap_surveymonkey.sync.singer.write_schema")
    @patch("tap_surveymonkey.sync.singer.write_record")
    @patch("tap_surveymonkey.sync.singer.write_state")
    @patch("tap_surveymonkey.sync.bookmarks")
    @patch("tap_surveymonkey.sync.Transformer")
    def test_transformer_used_for_records(
            self, mock_tf_cls, mock_bkm, mock_ws, mock_wr, mock_wsch, mock_cls):
        mock_bkm.get_bookmark.return_value = None
        mock_bkm.write_bookmark.return_value = {}

        mock_tf = MagicMock()
        mock_tf.__enter__ = Mock(return_value=mock_tf)
        mock_tf.__exit__ = Mock(return_value=False)
        mock_tf.transform.side_effect = lambda rec, schema, mdata: rec
        mock_tf_cls.return_value = mock_tf

        catalog = Mock()
        catalog.get_selected_streams.return_value = [_mock_catalog_stream("surveys")]
        stream_obj = _mock_stream_obj(records=[
            {"id": "1", "date_modified": "2023-01-15T00:00:00.000000Z"}
        ])

        with patch("tap_surveymonkey.sync.STREAMS", {"surveys": stream_obj}):
            sync(dict(_BASE_CONFIG), {}, catalog)

        mock_tf.transform.assert_called()


