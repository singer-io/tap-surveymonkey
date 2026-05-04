import unittest
from unittest.mock import patch, MagicMock

from tap_surveymonkey.sync import sync


SIMPLE_SCHEMA = {
    "properties": {
        "id": {"type": "string"},
        "date_modified": {"type": "string"},
        "survey_id": {"type": "string"},
    }
}


def _make_stream_obj(stream_id, records, parent_records=None, replication_key="date_modified",
                     is_sorted=False, replication_key_from_parent=False):
    """Minimal mock stream object for sync() testing."""
    mock_stream = MagicMock()
    mock_stream.stream_id = stream_id
    mock_stream.replication_key = replication_key
    mock_stream.is_sorted = is_sorted
    mock_stream.replication_key_from_parent = replication_key_from_parent
    # Return a list so the iterator is not exhausted on repeated calls
    mock_stream.fetch_data.return_value = list(records)
    if parent_records is not None:
        mock_parent = MagicMock()
        mock_parent.fetch_data.return_value = list(parent_records)
        mock_stream.parent_stream = mock_parent
    else:
        mock_stream.parent_stream = None
    return mock_stream


def _make_catalog_entry(stream_id, schema=None):
    entry = MagicMock()
    entry.tap_stream_id = stream_id
    entry.key_properties = ["id"]
    entry.metadata = []
    entry.schema.to_dict.return_value = schema or SIMPLE_SCHEMA
    return entry


class TestSyncClientInit(unittest.TestCase):

    def test_client_instantiated_with_access_token(self):
        """sync() creates a SurveyMonkeyClient with the access_token from config."""
        stream_obj = _make_stream_obj("surveys", records=[])
        mock_streams = {"surveys": stream_obj}
        mock_catalog = MagicMock()
        mock_catalog.get_selected_streams.return_value = [_make_catalog_entry("surveys")]

        with patch("tap_surveymonkey.sync.SurveyMonkeyClient") as mock_client_cls, \
             patch("tap_surveymonkey.sync.STREAMS", mock_streams), \
             patch("tap_surveymonkey.sync.singer.write_schema"), \
             patch("tap_surveymonkey.sync.singer.write_record"), \
             patch("tap_surveymonkey.sync.singer.write_state"):
            sync({"access_token": "my-secret-token"}, {"bookmarks": {}}, mock_catalog)

        mock_client_cls.assert_called_once_with("my-secret-token")


class TestSyncWriteSchema(unittest.TestCase):

    def test_write_schema_called_once_with_correct_args(self):
        """sync() calls write_schema exactly once with the stream's schema and key_properties."""
        stream_obj = _make_stream_obj("surveys", records=[])
        catalog_entry = _make_catalog_entry("surveys")
        mock_streams = {"surveys": stream_obj}
        mock_catalog = MagicMock()
        mock_catalog.get_selected_streams.return_value = [catalog_entry]

        with patch("tap_surveymonkey.sync.SurveyMonkeyClient"), \
             patch("tap_surveymonkey.sync.STREAMS", mock_streams), \
             patch("tap_surveymonkey.sync.singer.write_schema") as mock_write_schema, \
             patch("tap_surveymonkey.sync.singer.write_record"), \
             patch("tap_surveymonkey.sync.singer.write_state"):
            sync({"access_token": "tok"}, {"bookmarks": {}}, mock_catalog)

        mock_write_schema.assert_called_once_with(
            stream_name="surveys",
            schema=catalog_entry.schema.to_dict.return_value,
            key_properties=catalog_entry.key_properties,
        )

    def test_write_schema_called_for_each_selected_stream(self):
        """sync() calls write_schema once per selected stream."""
        stream_a = _make_stream_obj("surveys", records=[])
        stream_b = _make_stream_obj("responses", records=[])
        mock_streams = {"surveys": stream_a, "responses": stream_b}
        mock_catalog = MagicMock()
        mock_catalog.get_selected_streams.return_value = [
            _make_catalog_entry("surveys"),
            _make_catalog_entry("responses"),
        ]

        with patch("tap_surveymonkey.sync.SurveyMonkeyClient"), \
             patch("tap_surveymonkey.sync.STREAMS", mock_streams), \
             patch("tap_surveymonkey.sync.singer.write_schema") as mock_write_schema, \
             patch("tap_surveymonkey.sync.singer.write_record"), \
             patch("tap_surveymonkey.sync.singer.write_state"):
            sync({"access_token": "tok"}, {"bookmarks": {}}, mock_catalog)

        self.assertEqual(mock_write_schema.call_count, 2)


class TestSyncWriteRecord(unittest.TestCase):

    def test_all_records_written_when_no_bookmark(self):
        """write_record is called once per record when no prior bookmark exists."""
        records = [
            {"id": "1", "date_modified": "2022-01-01T00:00:00Z", "survey_id": "0"},
            {"id": "2", "date_modified": "2022-02-01T00:00:00Z", "survey_id": "0"},
        ]
        stream_obj = _make_stream_obj("surveys", records=records)
        mock_streams = {"surveys": stream_obj}
        mock_catalog = MagicMock()
        mock_catalog.get_selected_streams.return_value = [_make_catalog_entry("surveys")]

        with patch("tap_surveymonkey.sync.SurveyMonkeyClient"), \
             patch("tap_surveymonkey.sync.STREAMS", mock_streams), \
             patch("tap_surveymonkey.sync.singer.write_schema"), \
             patch("tap_surveymonkey.sync.singer.write_record") as mock_write_record, \
             patch("tap_surveymonkey.sync.singer.write_state"):
            sync({"access_token": "tok"}, {"bookmarks": {}}, mock_catalog)

        self.assertEqual(mock_write_record.call_count, 2)

    def test_no_records_written_when_stream_returns_empty(self):
        """write_record is never called when the stream yields no records."""
        stream_obj = _make_stream_obj("surveys", records=[])
        mock_streams = {"surveys": stream_obj}
        mock_catalog = MagicMock()
        mock_catalog.get_selected_streams.return_value = [_make_catalog_entry("surveys")]

        with patch("tap_surveymonkey.sync.SurveyMonkeyClient"), \
             patch("tap_surveymonkey.sync.STREAMS", mock_streams), \
             patch("tap_surveymonkey.sync.singer.write_schema"), \
             patch("tap_surveymonkey.sync.singer.write_record") as mock_write_record, \
             patch("tap_surveymonkey.sync.singer.write_state"):
            sync({"access_token": "tok"}, {"bookmarks": {}}, mock_catalog)

        mock_write_record.assert_not_called()

    def test_record_before_bookmark_is_not_written(self):
        """A record whose bookmark value is before the saved bookmark is skipped."""
        state = {"bookmarks": {"surveys": {"full_sync": "2022-06-01T00:00:00Z"}}}
        old_record = {"id": "1", "date_modified": "2022-01-01T00:00:00Z", "survey_id": "0"}
        stream_obj = _make_stream_obj("surveys", records=[old_record])
        mock_streams = {"surveys": stream_obj}
        mock_catalog = MagicMock()
        mock_catalog.get_selected_streams.return_value = [_make_catalog_entry("surveys")]

        with patch("tap_surveymonkey.sync.SurveyMonkeyClient"), \
             patch("tap_surveymonkey.sync.STREAMS", mock_streams), \
             patch("tap_surveymonkey.sync.singer.write_schema"), \
             patch("tap_surveymonkey.sync.singer.write_record") as mock_write_record, \
             patch("tap_surveymonkey.sync.singer.write_state"):
            sync({"access_token": "tok"}, state, mock_catalog)

        mock_write_record.assert_not_called()

    def test_record_at_or_after_bookmark_is_written(self):
        """A record whose bookmark value meets or exceeds the saved bookmark is written."""
        state = {"bookmarks": {"surveys": {"full_sync": "2022-01-01T00:00:00Z"}}}
        new_record = {"id": "2", "date_modified": "2022-06-01T00:00:00Z", "survey_id": "0"}
        stream_obj = _make_stream_obj("surveys", records=[new_record])
        mock_streams = {"surveys": stream_obj}
        mock_catalog = MagicMock()
        mock_catalog.get_selected_streams.return_value = [_make_catalog_entry("surveys")]

        with patch("tap_surveymonkey.sync.SurveyMonkeyClient"), \
             patch("tap_surveymonkey.sync.STREAMS", mock_streams), \
             patch("tap_surveymonkey.sync.singer.write_schema"), \
             patch("tap_surveymonkey.sync.singer.write_record") as mock_write_record, \
             patch("tap_surveymonkey.sync.singer.write_state"):
            sync({"access_token": "tok"}, state, mock_catalog)

        self.assertEqual(mock_write_record.call_count, 1)

    def test_write_record_stream_name_argument(self):
        """write_record is called with the correct stream name as first argument."""
        record = {"id": "1", "date_modified": "2022-01-01T00:00:00Z", "survey_id": "0"}
        stream_obj = _make_stream_obj("surveys", records=[record])
        mock_streams = {"surveys": stream_obj}
        mock_catalog = MagicMock()
        mock_catalog.get_selected_streams.return_value = [_make_catalog_entry("surveys")]

        with patch("tap_surveymonkey.sync.SurveyMonkeyClient"), \
             patch("tap_surveymonkey.sync.STREAMS", mock_streams), \
             patch("tap_surveymonkey.sync.singer.write_schema"), \
             patch("tap_surveymonkey.sync.singer.write_record") as mock_write_record, \
             patch("tap_surveymonkey.sync.singer.write_state"):
            sync({"access_token": "tok"}, {"bookmarks": {}}, mock_catalog)

        call_args = mock_write_record.call_args
        self.assertEqual(call_args[0][0], "surveys")


class TestSyncWriteState(unittest.TestCase):

    def test_write_state_called_after_stream_sync(self):
        """write_state is invoked at least once after a stream has been processed."""
        stream_obj = _make_stream_obj("surveys", records=[])
        mock_streams = {"surveys": stream_obj}
        mock_catalog = MagicMock()
        mock_catalog.get_selected_streams.return_value = [_make_catalog_entry("surveys")]

        with patch("tap_surveymonkey.sync.SurveyMonkeyClient"), \
             patch("tap_surveymonkey.sync.STREAMS", mock_streams), \
             patch("tap_surveymonkey.sync.singer.write_schema"), \
             patch("tap_surveymonkey.sync.singer.write_record"), \
             patch("tap_surveymonkey.sync.singer.write_state") as mock_write_state:
            sync({"access_token": "tok"}, {"bookmarks": {}}, mock_catalog)

        mock_write_state.assert_called()

    def test_write_state_called_per_record_for_sorted_non_surveys_stream(self):
        """For sorted non-surveys streams, write_state is called after every record processed."""
        records = [
            {"id": "1", "date_modified": "2022-01-01T00:00:00Z", "survey_id": "abc"},
            {"id": "2", "date_modified": "2022-02-01T00:00:00Z", "survey_id": "abc"},
        ]
        stream_obj = _make_stream_obj("responses", records=records, is_sorted=True)
        mock_streams = {"responses": stream_obj}
        mock_catalog = MagicMock()
        mock_catalog.get_selected_streams.return_value = [_make_catalog_entry("responses")]

        with patch("tap_surveymonkey.sync.SurveyMonkeyClient"), \
             patch("tap_surveymonkey.sync.STREAMS", mock_streams), \
             patch("tap_surveymonkey.sync.singer.write_schema"), \
             patch("tap_surveymonkey.sync.singer.write_record"), \
             patch("tap_surveymonkey.sync.singer.write_state") as mock_write_state:
            sync({"access_token": "tok"}, {"bookmarks": {}}, mock_catalog)

        # 1 write_state per record (sorted bookmark) + 1 for full_sync at the end
        self.assertGreaterEqual(mock_write_state.call_count, 2)
