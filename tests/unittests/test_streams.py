import datetime
import unittest
from unittest.mock import MagicMock

import pytz

from tap_surveymonkey.streams import (
    strptime,
    find_max_timestamp,
    patch_time_str,
    SurveyStream,
    Surveys,
    PaginatedStream,
    Responses,
    Stream,
    DATETIME_FMT,
    DATETIME_FMT_MAC,
    DATETIME_PARSE,
)


class TestStrptime(unittest.TestCase):

    def test_parses_datetime_fmt_with_microseconds(self):
        """strptime parses the DATETIME_FMT format (with microseconds)."""
        result = strptime("2022-03-15T10:30:00.000000Z")
        self.assertEqual(result.year, 2022)
        self.assertEqual(result.month, 3)
        self.assertEqual(result.day, 15)

    def test_parses_datetime_fmt_mac(self):
        """strptime parses the DATETIME_FMT_MAC format (platform-safe microseconds)."""
        result = strptime("2022-03-15T10:30:00.123456Z")
        self.assertIsInstance(result, datetime.datetime)

    def test_parses_datetime_parse_seconds_only(self):
        """strptime parses the DATETIME_PARSE format (no microseconds)."""
        result = strptime("2022-03-15T10:30:00Z")
        self.assertEqual(result.year, 2022)
        self.assertEqual(result.hour, 10)

    def test_returns_datetime_instance(self):
        """strptime always returns a datetime.datetime object."""
        result = strptime("2022-01-01T00:00:00Z")
        self.assertIsInstance(result, datetime.datetime)


class TestFindMaxTimestamp(unittest.TestCase):

    def test_returns_utc_localized_datetime(self):
        """find_max_timestamp returns a UTC-aware datetime."""
        state = {"bookmarks": {"surveys": {"s1": "2022-01-01T00:00:00Z"}}}
        result = find_max_timestamp(state, "surveys")
        self.assertIsNotNone(result.tzinfo)

    def test_picks_latest_timestamp_from_multiple_entries(self):
        """find_max_timestamp returns the maximum timestamp across all bookmark entries."""
        state = {
            "bookmarks": {
                "surveys": {
                    "s1": "2022-01-01T00:00:00Z",
                    "s2": "2023-06-01T00:00:00Z",
                    "s3": "2021-12-31T00:00:00Z",
                }
            }
        }
        result = find_max_timestamp(state, "surveys")
        expected = pytz.utc.localize(datetime.datetime(2023, 6, 1, 0, 0, 0))
        self.assertEqual(result, expected)

    def test_returns_datetime_min_for_missing_stream(self):
        """find_max_timestamp returns UTC min-time when the stream has no bookmarks."""
        state = {"bookmarks": {}}
        result = find_max_timestamp(state, "surveys")
        expected = pytz.utc.localize(datetime.datetime.min)
        self.assertEqual(result, expected)


class TestPatchTimeStr(unittest.TestCase):

    def test_date_modified_is_reformatted(self):
        """patch_time_str rewrites date_modified to RFC 3339 format."""
        record = {"date_modified": "2022-03-15T10:30:00-05:00"}
        patch_time_str(record)
        # Should have been parsed and reformatted — no longer the original string
        self.assertIn("T", record["date_modified"])

    def test_date_created_is_reformatted(self):
        """patch_time_str rewrites date_created to RFC 3339 format."""
        record = {"date_created": "2021-01-01T00:00:00+00:00"}
        patch_time_str(record)
        self.assertIn("T", record["date_created"])

    def test_record_without_date_fields_unchanged(self):
        """patch_time_str leaves records that have no date fields untouched."""
        record = {"id": "42", "title": "No dates here"}
        patch_time_str(record)
        self.assertEqual(record, {"id": "42", "title": "No dates here"})

    def test_unrelated_fields_not_modified(self):
        """patch_time_str does not modify fields other than date_modified/date_created."""
        record = {"id": "7", "date_modified": "2022-01-01T00:00:00Z", "title": "Keep me"}
        patch_time_str(record)
        self.assertEqual(record["id"], "7")
        self.assertEqual(record["title"], "Keep me")


class TestSurveyStreamFetchData(unittest.TestCase):

    def test_yields_survey_id_dict_when_survey_id_in_config(self):
        """SurveyStream.fetch_data yields {'id': survey_id} when survey_id is configured."""
        stream = SurveyStream(stream_id=None, path="surveys")
        config = {"survey_id": "survey-abc", "start_date": "2022-01-01T00:00:00Z"}
        client = MagicMock()
        results = list(stream.fetch_data(client, None, config, {}))
        self.assertEqual(results, [{"id": "survey-abc"}])

    def test_yields_exactly_one_record_when_survey_id_in_config(self):
        """SurveyStream.fetch_data yields exactly one record when survey_id is present."""
        stream = SurveyStream(stream_id=None, path="surveys")
        config = {"survey_id": "xyz", "start_date": "2022-01-01T00:00:00Z"}
        client = MagicMock()
        results = list(stream.fetch_data(client, None, config, {}))
        self.assertEqual(len(results), 1)

    def test_calls_paginated_fetch_when_no_survey_id(self):
        """SurveyStream.fetch_data delegates to PaginatedStream when no survey_id in config."""
        stream = SurveyStream(stream_id=None, path="surveys")
        config = {"start_date": "2022-01-01T00:00:00Z"}
        mock_resp = {
            "data": [{"id": "1"}, {"id": "2"}],
            "links": {}
        }
        client = MagicMock()
        client.make_request.return_value = mock_resp
        results = list(stream.fetch_data(client, MagicMock(), config, {}))
        self.assertEqual(len(results), 2)


class TestPaginatedStreamFetchData(unittest.TestCase):

    def _make_paginated_stream(self):
        return PaginatedStream(stream_id="test_stream", path="test_path")

    def test_yields_all_records_from_single_page(self):
        """PaginatedStream.fetch_data yields all records when only one page exists."""
        stream = self._make_paginated_stream()
        client = MagicMock()
        client.make_request.return_value = {
            "data": [{"id": "1"}, {"id": "2"}],
            "links": {}
        }
        results = list(stream.fetch_data(client, MagicMock(),
                                         {"start_date": "2021-01-01T00:00:00Z"}, {}))
        self.assertEqual(len(results), 2)

    def test_yields_records_from_multiple_pages(self):
        """PaginatedStream.fetch_data yields records across paginated responses."""
        stream = self._make_paginated_stream()
        client = MagicMock()
        client.make_request.side_effect = [
            {"data": [{"id": "1"}], "links": {"next": "?page=2"}},
            {"data": [{"id": "2"}, {"id": "3"}], "links": {}},
        ]
        results = list(stream.fetch_data(client, MagicMock(),
                                         {"start_date": "2021-01-01T00:00:00Z"}, {}))
        self.assertEqual(len(results), 3)

    def test_raises_exception_when_response_is_none(self):
        """PaginatedStream.fetch_data raises an exception when the client returns None."""
        stream = self._make_paginated_stream()
        client = MagicMock()
        client.make_request.return_value = None
        with self.assertRaises(Exception):
            list(stream.fetch_data(client, MagicMock(),
                                   {"start_date": "2021-01-01T00:00:00Z"}, {}))

    def test_raises_exception_on_error_response(self):
        """PaginatedStream.fetch_data raises an exception when the response contains an error."""
        stream = self._make_paginated_stream()
        client = MagicMock()
        client.make_request.return_value = {"error": {"message": "Unauthorized"}}
        with self.assertRaises(Exception):
            list(stream.fetch_data(client, MagicMock(),
                                   {"start_date": "2021-01-01T00:00:00Z"}, {}))

    def test_stops_after_last_page(self):
        """PaginatedStream.fetch_data stops after reaching a page with no next link."""
        stream = self._make_paginated_stream()
        client = MagicMock()
        client.make_request.side_effect = [
            {"data": [{"id": "1"}], "links": {"next": "?page=2"}},
            {"data": [{"id": "2"}], "links": {}},
            # A third call should never happen
            {"data": [{"id": "3"}], "links": {}},
        ]
        results = list(stream.fetch_data(client, MagicMock(),
                                         {"start_date": "2021-01-01T00:00:00Z"}, {}))
        self.assertEqual(len(results), 2)
        self.assertEqual(client.make_request.call_count, 2)


class TestStreamFetchData(unittest.TestCase):

    def test_format_response_returns_input(self):
        """Base Stream.format_response returns the original response unchanged."""
        stream = Stream(stream_id="survey_details", path="surveys/123/details")
        payload = {"id": "123", "title": "Survey"}
        self.assertIs(stream.format_response(payload), payload)

    def test_raises_exception_when_response_is_none(self):
        """Stream.fetch_data raises an exception when the client returns None (404)."""
        stream = Stream(stream_id="survey_details", path="surveys/123/details")
        client = MagicMock()
        client.make_request.return_value = None
        with self.assertRaises(Exception):
            list(stream.fetch_data(client, None, {}, {}))

    def test_raises_exception_on_error_in_response(self):
        """Stream.fetch_data raises an exception when the response contains an error key."""
        stream = Stream(stream_id="survey_details", path="surveys/123/details")
        client = MagicMock()
        client.make_request.return_value = {"error": {"message": "Not found"}}
        with self.assertRaises(Exception):
            list(stream.fetch_data(client, None, {}, {}))

    def test_yields_response_when_successful(self):
        """Stream.fetch_data yields the response dict on a successful request."""
        stream = Stream(stream_id="survey_details", path="surveys/123/details")
        client = MagicMock()
        payload = {"id": "123", "title": "Survey"}
        client.make_request.return_value = payload
        results = list(stream.fetch_data(client, None, {}, {}))
        self.assertEqual(results, [payload])

    def test_path_template_substituted_from_parent_row(self):
        """Stream.fetch_data replaces {parent_id} placeholders from the parent_row dict."""
        stream = Stream(stream_id="survey_details", path="surveys/{parent_id}/details")
        client = MagicMock()
        client.make_request.return_value = {"id": "55"}
        list(stream.fetch_data(client, None, {}, {}, parent_row={"id": "55"}))
        call_path = client.make_request.call_args[0][0]
        self.assertEqual(call_path, "surveys/55/details")


class TestResponsesGetParams(unittest.TestCase):

    def test_simple_true_adds_simple_param(self):
        """Responses stream with simple=True includes 'simple' in request params."""
        from tap_surveymonkey.streams import Responses, SurveyStream
        stream = Responses(
            stream_id="simplified_responses",
            path="surveys/{parent_id}/responses/bulk",
            parent_stream=SurveyStream(stream_id=None, path="surveys"),
            simple=True
        )
        mock_catalog_stream = MagicMock()
        mock_catalog_stream.metadata = []
        params = stream.get_params(mock_catalog_stream, {"start_date": "2022-01-01T00:00:00Z"}, {}, None)
        self.assertTrue(params.get("simple"))

    def test_simple_false_omits_simple_param(self):
        """Responses stream with simple=False does not include 'simple' in request params."""
        from tap_surveymonkey.streams import Responses, SurveyStream
        stream = Responses(
            stream_id="responses",
            path="surveys/{parent_id}/responses/bulk",
            parent_stream=SurveyStream(stream_id=None, path="surveys"),
            simple=False
        )
        mock_catalog_stream = MagicMock()
        mock_catalog_stream.metadata = []
        params = stream.get_params(mock_catalog_stream, {"start_date": "2022-01-01T00:00:00Z"}, {}, None)
        self.assertFalse(params.get("simple", False))

    def test_bookmark_value_sets_start_modified_at(self):
        """Responses.get_params includes start_modified_at when a bookmark_value is given."""
        from tap_surveymonkey.streams import Responses, SurveyStream
        stream = Responses(
            stream_id="responses",
            path="surveys/{parent_id}/responses/bulk",
            parent_stream=SurveyStream(stream_id=None, path="surveys"),
        )
        mock_catalog_stream = MagicMock()
        mock_catalog_stream.metadata = []
        params = stream.get_params(
            mock_catalog_stream,
            {"start_date": "2021-01-01T00:00:00Z"},
            {},
            "2022-06-01T00:00:00Z"
        )
        self.assertEqual(params["start_modified_at"], "2022-06-01T00:00:00Z")

    def test_start_date_used_when_no_bookmark(self):
        """Responses.get_params uses config start_date when no bookmark_value is provided."""
        from tap_surveymonkey.streams import Responses, SurveyStream
        stream = Responses(
            stream_id="responses",
            path="surveys/{parent_id}/responses/bulk",
            parent_stream=SurveyStream(stream_id=None, path="surveys"),
        )
        mock_catalog_stream = MagicMock()
        mock_catalog_stream.metadata = []
        params = stream.get_params(
            mock_catalog_stream,
            {"start_date": "2020-01-01T00:00:00Z"},
            {},
            None
        )
        self.assertEqual(params["start_modified_at"], "2020-01-01T00:00:00Z")


class TestSurveysGetParams(unittest.TestCase):

    def test_selected_optional_fields_are_added_to_include(self):
        """Surveys.get_params appends selected optional metadata fields to include."""
        stream = Surveys(stream_id="surveys", path="surveys")
        mock_catalog_stream = MagicMock()
        mock_catalog_stream.metadata = [
            {
                "breadcrumb": (),
                "metadata": {},
            },
            {
                "breadcrumb": ("properties", "language"),
                "metadata": {"selected": True},
            },
        ]

        params = stream.get_params(
            mock_catalog_stream,
            {"start_date": "2020-01-01T00:00:00Z"},
            {},
            None,
        )

        self.assertIn("language", params["include"].split(","))
