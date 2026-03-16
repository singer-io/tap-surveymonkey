"""Base test class for tap-surveymonkey integration tests."""
from datetime import datetime
from unittest import TestCase
from unittest.mock import patch, Mock

import pytz


class SurveyMonkeyBaseTest(TestCase):
    """Common metadata and sync helper for all integration tests."""

    STREAM_METADATA = {
        "surveys":              {"keys": ["id"], "replication_method": "INCREMENTAL", "replication_key": "date_modified", "api_limit": 50,   "obeys_start_date": True},
        "survey_details":       {"keys": ["id"], "replication_method": "INCREMENTAL", "replication_key": "date_modified", "api_limit": None,  "obeys_start_date": True, "parent": "surveys"},
        "responses":            {"keys": ["id"], "replication_method": "INCREMENTAL", "replication_key": "date_modified", "api_limit": 50,   "obeys_start_date": True, "parent": "surveys"},
        "simplified_responses": {"keys": ["id"], "replication_method": "INCREMENTAL", "replication_key": "date_modified", "api_limit": 50,   "obeys_start_date": True, "parent": "surveys"},
    }

    @staticmethod
    def expected_stream_names():
        return {"surveys", "survey_details", "responses", "simplified_responses"}

    @staticmethod
    def expected_primary_keys():
        return {s: ["id"] for s in ("surveys", "survey_details", "responses", "simplified_responses")}

    @staticmethod
    def expected_replication_keys():
        return {s: "date_modified" for s in ("surveys", "survey_details", "responses", "simplified_responses")}

    @staticmethod
    def expected_replication_methods():
        return {s: "INCREMENTAL" for s in ("surveys", "survey_details", "responses", "simplified_responses")}

    @staticmethod
    def get_properties():
        return {"start_date": "2021-01-01T00:00:00Z", "access_token": "test_token"}

    @staticmethod
    def parse_date(value):
        if isinstance(value, int):
            return datetime.fromtimestamp(value, tz=pytz.UTC)
        for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z"):
            try:
                dt = datetime.strptime(value, fmt)
                return dt if dt.tzinfo else dt.replace(tzinfo=pytz.UTC)
            except ValueError:
                continue
        raise ValueError(f"Unsupported date format: {value!r}")

    def _run_sync(self, stream_name, records, *, bookmark=None, state=None):
        """Run sync for one stream with mocked API data.

        Returns (mock_write_record, mock_write_state) for post-call assertions.
        Eliminates the repeated 5-patch + setup boilerplate from every sync test.
        """
        from tap_surveymonkey.sync import sync
        from tap_surveymonkey.discover import discover

        catalog = discover()
        selected = next(s for s in catalog.streams if s.tap_stream_id == stream_name)

        catalog_mock = Mock()
        catalog_mock.get_selected_streams.return_value = [selected]

        stream_mock = Mock(
            replication_key="date_modified",
            is_sorted=True,
            parent_stream=None,
            replication_key_from_parent=False,
            stream_id=stream_name,
        )
        stream_mock.fetch_data.return_value = records

        with patch("tap_surveymonkey.sync.SurveyMonkeyClient"), \
             patch("tap_surveymonkey.sync.singer.write_schema"), \
             patch("tap_surveymonkey.sync.singer.write_record") as mock_record, \
             patch("tap_surveymonkey.sync.singer.write_state") as mock_state, \
             patch("tap_surveymonkey.sync.bookmarks") as mock_bm, \
             patch("tap_surveymonkey.sync.STREAMS", {stream_name: stream_mock}):
            mock_bm.get_bookmark.return_value = bookmark
            mock_bm.write_bookmark.return_value = {}
            sync(self.get_properties(), state or {}, catalog_mock)

        return mock_record, mock_state
