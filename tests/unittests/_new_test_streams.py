"""
Unit tests for stream classes and date/time utilities.

Covers: Stream, PaginatedStream, SurveyStream, Surveys, SurveyDetails,
Responses, the STREAMS registry, strptime, patch_time_str, and
find_max_timestamp.
"""
import unittest
import unittest.mock
import datetime
import pytz
from types import SimpleNamespace
from unittest.mock import Mock, patch, MagicMock

from tap_surveymonkey.streams import (
    Stream, PaginatedStream, SurveyStream, Surveys,
    SurveyDetails, Responses, STREAMS,
    patch_time_str, strptime, find_max_timestamp,
    DATETIME_PARSE, DATETIME_FMT, DATETIME_FMT_MAC,
)
from tap_surveymonkey.client import SurveyMonkeyClient


# ---------------------------------------------------------------------------
# Base Stream
# ---------------------------------------------------------------------------

class TestStreamBase(unittest.TestCase):

    def test_init_defaults(self):
        s = Stream(stream_id="test", path="test/path")
        self.assertEqual(s.stream_id, "test")
        self.assertEqual(s.path, "test/path")
        self.assertIsNone(s.parent_stream)
        self.assertEqual(s._params, {})

    def test_init_with_parent(self):
        parent = Stream(stream_id="p", path="p/path")
        child = Stream(stream_id="c", path="c/path", parent_stream=parent)
        self.assertIs(child.parent_stream, parent)

    def test_class_level_defaults(self):
        self.assertIsNone(Stream.key_properties)
        self.assertIsNone(Stream.replication_method)
        self.assertIsNone(Stream.replication_key)
        self.assertFalse(Stream.replication_key_from_parent)
        self.assertFalse(Stream.is_sorted)

    def test_format_response_passthrough(self):
        s = Stream(stream_id="t", path="t")
        data = {"data": [{"id": "1"}]}
        self.assertEqual(s.format_response(data), data)

    @patch.object(SurveyMonkeyClient, "make_request")
    def test_fetch_data_yields_response(self, mock_req):
        mock_req.return_value = {"id": "123", "title": "T"}
        s = Stream(stream_id="t", path="t/ep")
        results = list(s.fetch_data(SurveyMonkeyClient("tok"), None, {}, {}))
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["id"], "123")

    @patch.object(SurveyMonkeyClient, "make_request")
    def test_fetch_data_replaces_parent_id_in_path(self, mock_req):
        mock_req.return_value = {"id": "456"}
        s = Stream(stream_id="c", path="parent/{parent_id}/child")
        list(s.fetch_data(SurveyMonkeyClient("tok"), None, {}, {},
                          parent_row={"id": "p123"}))
        self.assertEqual(mock_req.call_args[0][0], "parent/p123/child")

    @patch.object(SurveyMonkeyClient, "make_request")
    def test_fetch_data_raises_on_none_response(self, mock_req):
        mock_req.return_value = None
        s = Stream(stream_id="t", path="t")
        with self.assertRaises(Exception) as ctx:
            list(s.fetch_data(SurveyMonkeyClient("tok"), None, {}, {}))
        self.assertIn("Resource not found", str(ctx.exception))

    @patch.object(SurveyMonkeyClient, "make_request")
    def test_fetch_data_raises_on_error_response(self, mock_req):
        mock_req.return_value = {"error": {"message": "Bad"}}
        s = Stream(stream_id="t", path="t")
        with self.assertRaises(Exception):
            list(s.fetch_data(SurveyMonkeyClient("tok"), None, {}, {}))


# ---------------------------------------------------------------------------
# PaginatedStream
# ---------------------------------------------------------------------------

class TestPaginatedStream(unittest.TestCase):

    def test_default_params(self):
        s = PaginatedStream(stream_id="t", path="t")
        p = s.get_params(None, {}, {}, None)
        self.assertEqual(p["per_page"], 50)
        self.assertEqual(p["page"], 1)

    def test_custom_page_size_from_config(self):
        s = PaginatedStream(stream_id="t", path="t")
        p = s.get_params(None, {"page_size": "100"}, {}, None)
        self.assertEqual(p["per_page"], 100)

    def test_format_response_extracts_data(self):
        s = PaginatedStream(stream_id="t", path="t")
        resp = {"data": [{"id": "1"}, {"id": "2"}], "links": {}}
        self.assertEqual(s.format_response(resp), [{"id": "1"}, {"id": "2"}])

    @patch.object(SurveyMonkeyClient, "make_request")
    def test_single_page(self, mock_req):
        mock_req.return_value = {"data": [{"id": "1"}, {"id": "2"}], "links": {}}
        s = PaginatedStream(stream_id="t", path="t")
        results = list(s.fetch_data(SurveyMonkeyClient("tok"), None, {}, {}))
        self.assertEqual(len(results), 2)
        self.assertEqual(mock_req.call_count, 1)

    @patch.object(SurveyMonkeyClient, "make_request")
    def test_multi_page(self, mock_req):
        mock_req.side_effect = [
            {"data": [{"id": "1"}, {"id": "2"}], "links": {"next": "p2"}},
            {"data": [{"id": "3"}], "links": {}},
        ]
        s = PaginatedStream(stream_id="t", path="t")
        results = list(s.fetch_data(SurveyMonkeyClient("tok"), None, {}, {}))
        self.assertEqual(len(results), 3)
        self.assertEqual(mock_req.call_count, 2)

    @patch.object(SurveyMonkeyClient, "make_request")
    def test_page_number_increments(self, mock_req):
        mock_req.side_effect = [
            {"data": [{"id": "1"}], "links": {"next": "p2"}},
            {"data": [{"id": "2"}], "links": {"next": "p3"}},
            {"data": [{"id": "3"}], "links": {}},
        ]
        s = PaginatedStream(stream_id="t", path="t")
        list(s.fetch_data(SurveyMonkeyClient("tok"), None, {}, {}))
        for i, call in enumerate(mock_req.call_args_list, start=1):
            self.assertEqual(call[1]["params"]["page"], i)


# ---------------------------------------------------------------------------
# SurveyStream
# ---------------------------------------------------------------------------

class TestSurveyStream(unittest.TestCase):

    def test_has_key_properties(self):
        s = SurveyStream(stream_id="surveys", path="surveys")
        self.assertEqual(s.key_properties, ["id"])

    def test_params_include_sorting(self):
        s = SurveyStream(stream_id="surveys", path="surveys")
        p = s.get_params(None, {}, {}, None)
        self.assertEqual(p["sort_by"], "date_modified")
        self.assertEqual(p["sort_order"], "ASC")
        self.assertIn("date_modified", p.get("include", ""))

    def test_params_uses_start_date(self):
        s = SurveyStream(stream_id="surveys", path="surveys")
        p = s.get_params(None, {"start_date": "2021-01-01T00:00:00Z"}, {}, None)
        self.assertEqual(p["start_modified_at"], "2021-01-01T00:00:00Z")

    def test_params_bookmark_overrides_start_date(self):
        s = SurveyStream(stream_id="surveys", path="surveys")
        p = s.get_params(None, {"start_date": "2021-01-01T00:00:00Z"}, {},
                         "2022-06-01T00:00:00Z")
        self.assertEqual(p["start_modified_at"], "2022-06-01T00:00:00Z")

    @patch.object(SurveyMonkeyClient, "make_request")
    def test_survey_id_in_config_skips_api(self, mock_req):
        s = SurveyStream(stream_id=None, path="surveys")
        results = list(s.fetch_data(SurveyMonkeyClient("tok"), None,
                                    {"survey_id": "123456"}, {}))
        self.assertEqual(results, [{"id": "123456"}])
        mock_req.assert_not_called()

    @patch.object(SurveyMonkeyClient, "make_request")
    def test_no_survey_id_fetches_all(self, mock_req):
        mock_req.return_value = {"data": [{"id": "1"}, {"id": "2"}], "links": {}}
        s = SurveyStream(stream_id=None, path="surveys")
        results = list(s.fetch_data(SurveyMonkeyClient("tok"), None, {}, {}))
        self.assertEqual(len(results), 2)
        mock_req.assert_called_once()


# ---------------------------------------------------------------------------
# Surveys
# ---------------------------------------------------------------------------

class TestSurveysStream(unittest.TestCase):

    def test_class_properties(self):
        s = Surveys(stream_id="surveys", path="surveys")
        self.assertEqual(s.key_properties, ["id"])
        self.assertEqual(s.replication_method, "INCREMENTAL")
        self.assertEqual(s.replication_key, "date_modified")
        self.assertTrue(s.is_sorted)

    def test_optional_include_fields_defined(self):
        self.assertIsInstance(Surveys.OPTIONAL_INCLUDE_FIELDS, list)
        self.assertIn("response_count", Surveys.OPTIONAL_INCLUDE_FIELDS)
        self.assertIn("date_created", Surveys.OPTIONAL_INCLUDE_FIELDS)

    def test_params_include_date_modified(self):
        s = Surveys(stream_id="surveys", path="surveys")
        mock_stream = SimpleNamespace(metadata=[])
        p = s.get_params(mock_stream, {}, {}, None)
        self.assertIn("date_modified", p["include"])

    def test_params_bookmark_adjusted_back_one_minute(self):
        s = Surveys(stream_id="surveys", path="surveys")
        mock_stream = SimpleNamespace(metadata=[])
        bkm = "2023-01-01T12:00:00.000000Z"
        p = s.get_params(mock_stream, {}, {}, bkm)
        self.assertIn("start_modified_at", p)
        self.assertIn("2023-01-01T11:59", p["start_modified_at"])


# ---------------------------------------------------------------------------
# SurveyDetails
# ---------------------------------------------------------------------------

class TestSurveyDetailsStream(unittest.TestCase):

    def test_class_properties(self):
        parent = SurveyStream(stream_id=None, path="surveys")
        s = SurveyDetails(stream_id="survey_details",
                          path="surveys/{parent_id}/details",
                          parent_stream=parent)
        self.assertEqual(s.key_properties, ["id"])
        self.assertEqual(s.replication_method, "INCREMENTAL")
        self.assertEqual(s.replication_key, "date_modified")
        self.assertTrue(s.replication_key_from_parent)
        self.assertTrue(s.is_sorted)


# ---------------------------------------------------------------------------
# Responses
# ---------------------------------------------------------------------------

class TestResponsesStream(unittest.TestCase):

    def _make(self, simple=False):
        parent = SurveyStream(stream_id=None, path="surveys")
        return Responses(stream_id="responses" if not simple else "simplified_responses",
                         path="surveys/{parent_id}/responses/bulk",
                         parent_stream=parent,
                         simple=simple)

    def test_default_initialization(self):
        s = self._make()
        self.assertEqual(s.key_properties, ["id"])
        self.assertEqual(s.replication_method, "INCREMENTAL")
        self.assertEqual(s.replication_key, "date_modified")
        self.assertFalse(s.simple)

    def test_simple_flag_set(self):
        self.assertTrue(self._make(simple=True).simple)

    def test_params_simple_flag(self):
        p = self._make(simple=True).get_params(None, {}, {}, None)
        self.assertTrue(p["simple"])

    def test_params_no_simple_flag(self):
        p = self._make(simple=False).get_params(None, {}, {}, None)
        self.assertNotIn("simple", p)


# ---------------------------------------------------------------------------
# STREAMS registry
# ---------------------------------------------------------------------------

class TestStreamsRegistry(unittest.TestCase):

    def test_has_expected_keys(self):
        for key in ("surveys", "survey_details", "responses", "simplified_responses"):
            self.assertIn(key, STREAMS)

    def test_all_are_stream_instances(self):
        for stream in STREAMS.values():
            self.assertIsInstance(stream, Stream)

    def test_stream_ids_match_keys(self):
        for key, stream in STREAMS.items():
            self.assertEqual(stream.stream_id, key)

    def test_child_streams_have_parents(self):
        for key in ("survey_details", "responses", "simplified_responses"):
            self.assertIsNotNone(STREAMS[key].parent_stream)

    def test_surveys_has_no_parent(self):
        self.assertIsNone(STREAMS["surveys"].parent_stream)


# ---------------------------------------------------------------------------
# Date utilities – strptime
# ---------------------------------------------------------------------------

class TestStrptime(unittest.TestCase):

    def test_fmt_with_microseconds(self):
        r = strptime("2023-01-15T10:30:45.123456Z")
        self.assertIsInstance(r, datetime.datetime)
        self.assertEqual((r.year, r.month, r.day), (2023, 1, 15))

    def test_fmt_without_microseconds(self):
        r = strptime("2023-03-10T08:15:00Z")
        self.assertIsInstance(r, datetime.datetime)
        self.assertEqual(r.year, 2023)

    def test_various_valid_formats(self):
        for ds in ("2023-01-01T00:00:00Z",
                   "2023-12-31T23:59:59.999999Z",
                   "2022-06-15T12:30:45.123Z"):
            self.assertIsInstance(strptime(ds), datetime.datetime)

    def test_invalid_format_raises(self):
        with self.assertRaises(ValueError):
            strptime("2023/01/01 10:30:45")


# ---------------------------------------------------------------------------
# Date utilities – patch_time_str
# ---------------------------------------------------------------------------

class TestPatchTimeStr(unittest.TestCase):

    @unittest.mock.patch("tap_surveymonkey.streams.singer.utils.strptime_to_utc")
    @unittest.mock.patch("tap_surveymonkey.streams.singer.utils.strftime")
    def test_patches_date_modified(self, mock_strftime, mock_strptime):
        mock_strptime.return_value = datetime.datetime(2023, 1, 15, 10, 30, 45)
        mock_strftime.return_value = "2023-01-15T10:30:45.000000Z"
        rec = {"id": "1", "date_modified": "2023-01-15T10:30:45Z"}
        patch_time_str(rec)
        mock_strptime.assert_called_once_with("2023-01-15T10:30:45Z")
        self.assertEqual(rec["date_modified"], "2023-01-15T10:30:45.000000Z")

    @unittest.mock.patch("tap_surveymonkey.streams.singer.utils.strptime_to_utc")
    @unittest.mock.patch("tap_surveymonkey.streams.singer.utils.strftime")
    def test_patches_date_created(self, mock_strftime, mock_strptime):
        mock_strptime.return_value = datetime.datetime(2023, 1, 1)
        mock_strftime.return_value = "2023-01-01T00:00:00.000000Z"
        rec = {"date_created": "2023-01-01T00:00:00Z"}
        patch_time_str(rec)
        self.assertEqual(mock_strptime.call_count, 1)

    @unittest.mock.patch("tap_surveymonkey.streams.singer.utils.strptime_to_utc")
    @unittest.mock.patch("tap_surveymonkey.streams.singer.utils.strftime")
    def test_patches_both_fields(self, mock_strftime, mock_strptime):
        mock_strptime.return_value = datetime.datetime(2023, 1, 1)
        mock_strftime.return_value = "2023-01-01T00:00:00.000000Z"
        rec = {"date_modified": "2023-01-15T10:30:00Z", "date_created": "2023-01-01T00:00:00Z"}
        patch_time_str(rec)
        self.assertEqual(mock_strptime.call_count, 2)

    def test_no_date_fields_unchanged(self):
        rec = {"id": "1", "title": "T"}
        patch_time_str(rec)
        self.assertEqual(rec, {"id": "1", "title": "T"})

    def test_empty_dict_ok(self):
        rec = {}
        patch_time_str(rec)  # must not raise
        self.assertEqual(rec, {})


# ---------------------------------------------------------------------------
# Date utilities – find_max_timestamp
# ---------------------------------------------------------------------------

class TestFindMaxTimestamp(unittest.TestCase):

    def _state(self, bookmarks):
        return {"bookmarks": bookmarks}

    def test_single_bookmark(self):
        ts = "2023-01-15T10:00:00.000000Z"
        result = find_max_timestamp(self._state({"surveys": {"s1": ts}}), "surveys")
        self.assertEqual(result, pytz.utc.localize(strptime(ts)))

    def test_returns_latest_of_multiple(self):
        state = self._state({"surveys": {
            "s1": "2023-01-15T10:00:00.000000Z",
            "s2": "2023-06-20T15:30:00.000000Z",
            "s3": "2023-03-10T08:00:00.000000Z",
        }})
        result = find_max_timestamp(state, "surveys")
        expected = pytz.utc.localize(strptime("2023-06-20T15:30:00.000000Z"))
        self.assertEqual(result, expected)

    def test_empty_bookmarks_returns_min(self):
        result = find_max_timestamp(self._state({}), "surveys")
        self.assertEqual(result, pytz.utc.localize(datetime.datetime.min))

    def test_stream_not_in_bookmarks_returns_min(self):
        result = find_max_timestamp(
            self._state({"other": {"id1": "2023-01-01T00:00:00.000000Z"}}),
            "surveys",
        )
        self.assertEqual(result, pytz.utc.localize(datetime.datetime.min))


# ---------------------------------------------------------------------------
# Datetime format constants
# ---------------------------------------------------------------------------

class TestDateTimeConstants(unittest.TestCase):

    def test_datetime_parse_has_year_month_day(self):
        for part in ("%Y", "%m", "%d"):
            self.assertIn(part, DATETIME_PARSE)

    def test_datetime_fmt_has_microseconds(self):
        self.assertIn("%f", DATETIME_FMT)

    def test_datetime_fmt_mac_defined(self):
        self.assertIsInstance(DATETIME_FMT_MAC, str)
        self.assertIn("%Y", DATETIME_FMT_MAC)


if __name__ == "__main__":
    unittest.main()
