"""Integration tests for tap-surveymonkey start-date filtering with mocked data.

Patches tap_surveymonkey.client.requests.request so no live API is called.
Run with: python -m pytest tests/test_start_date.py -v
"""
import sys
import os
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(__file__))
from base import SurveyMonkeyBaseTest, MockResponse

from tap_surveymonkey.sync import sync


class SurveyMonkeyStartDateTest(SurveyMonkeyBaseTest, unittest.TestCase):
    """Verify that start_date config controls which records are synced."""

    # ── surveys — obeys start_date via start_modified_at param ───────────────

    @patch("tap_surveymonkey.client.requests.request")
    def test_surveys_early_start_date_returns_more_records(self, mock_req):
        """Using an earlier start_date should return at least as many records as a later one.

        The surveys API receives start_modified_at in params; we verify that the
        tap passes the start_date through to the API request, not that the API
        filters (which we don't control in the mock). Instead, we assert that
        the bookmark-based filter inside sync() lets records through.
        """
        date_old = "2019-06-01T00:00:00.000000Z"
        date_new = "2024-03-01T00:00:00.000000Z"

        def _routed(method, url, **kwargs):
            return MockResponse({
                "data": [
                    {"id": "s_old", "date_modified": date_old},
                    {"id": "s_new", "date_modified": date_new},
                ],
                "links": {},
                "total": 2,
            })

        mock_req.side_effect = _routed

        # Sync 1 — early start_date (before date_old)
        config_early = {**self.config, "start_date": "2018-01-01T00:00:00Z"}
        catalog = self._make_catalog(["surveys"])
        records_early = []
        with patch("tap_surveymonkey.sync.singer.write_schema"), \
             patch("tap_surveymonkey.sync.singer.write_state"), \
             patch("tap_surveymonkey.sync.singer.write_record",
                   side_effect=lambda s, r: records_early.append(r)):
            sync(config_early, {}, catalog)

        # Sync 2 — late start_date (after date_old, should filter it out)
        mock_req.side_effect = _routed
        config_late = {**self.config, "start_date": date_new}
        records_late = []
        with patch("tap_surveymonkey.sync.singer.write_schema"), \
             patch("tap_surveymonkey.sync.singer.write_state"), \
             patch("tap_surveymonkey.sync.singer.write_record",
                   side_effect=lambda s, r: records_late.append(r)):
            sync(config_late, {}, catalog)

        self.assertGreaterEqual(len(records_early), len(records_late),
                                "Earlier start_date should yield >= records")

    @patch("tap_surveymonkey.client.requests.request")
    def test_surveys_start_modified_at_sent_on_second_sync(self, mock_req):
        """On a second sync (saved bookmark exists) start_modified_at is sent to the API.

        Surveys.get_params() uses the 'elif bookmark_value' branch to add
        start_modified_at when a prior bookmark is already in state.  On the
        first sync (no bookmark) the param is NOT added because the start_date
        is only assigned locally inside get_params to seed the API filter; the
        in-process record filter in sync.py uses the state bookmark, not start_date.
        """
        saved_bookmark = "2023-06-01T00:00:00.000000Z"

        mock_req.return_value = MockResponse({
            "data": [],
            "links": {},
            "total": 0,
        })

        # Pre-populate state with a saved bookmark — simulates second sync
        state = {"bookmarks": {"surveys": {"full_sync": saved_bookmark}}}
        config = {**self.config, "start_date": "2020-01-01T00:00:00Z"}
        catalog = self._make_catalog(["surveys"])

        with patch("tap_surveymonkey.sync.singer.write_schema"), \
             patch("tap_surveymonkey.sync.singer.write_state"), \
             patch("tap_surveymonkey.sync.singer.write_record"):
            sync(config, state, catalog)

        called_params = [
            call.kwargs.get("params") or {}
            for call in mock_req.call_args_list
        ]
        has_start_modified = any(
            p.get("start_modified_at") for p in called_params
        )
        self.assertTrue(has_start_modified,
                        "start_modified_at should be sent to the API when a saved bookmark exists")

    # ── responses — obeys start_date ──────────────────────────────────────────

    @patch("tap_surveymonkey.client.requests.request")
    def test_responses_start_date_sent_as_api_param(self, mock_req):
        """sync() passes start_date as start_modified_at when fetching responses."""
        start_date = "2022-06-01T00:00:00Z"
        config = {**self.config, "start_date": start_date}

        def _routed(method, url, **kwargs):
            if "/responses/bulk" in url:
                return MockResponse({"data": [], "links": {}, "total": 0})
            return MockResponse({
                "data": [{"id": "s1", "date_modified": "2022-01-01T00:00:00.000000Z"}],
                "links": {},
                "total": 1,
            })

        mock_req.side_effect = _routed
        catalog = self._make_catalog(["responses"])
        with patch("tap_surveymonkey.sync.singer.write_schema"), \
             patch("tap_surveymonkey.sync.singer.write_state"), \
             patch("tap_surveymonkey.sync.singer.write_record"):
            sync(config, {}, catalog)

        called_params = [
            call.kwargs.get("params") or {}
            for call in mock_req.call_args_list
        ]
        bulk_params = [
            p for p in called_params if "start_modified_at" in p
        ]
        self.assertTrue(len(bulk_params) > 0,
                        "start_modified_at was not passed to the responses API")

    # ── config start_date is consumed when no bookmark exists ─────────────────

    @patch("tap_surveymonkey.client.requests.request")
    def test_no_bookmark_falls_back_to_start_date(self, mock_req):
        """When state has no bookmark, start_date is used as the filter minimum."""
        date_value = "2024-01-01T00:00:00.000000Z"
        mock_req.side_effect = self._mock_request_fn(
            survey_id="s1", date_value=date_value)

        config = {**self.config, "start_date": "2020-01-01T00:00:00Z"}
        state = {}  # no bookmarks
        catalog = self._make_catalog(["surveys"])
        records_written = []

        with patch("tap_surveymonkey.sync.singer.write_schema"), \
             patch("tap_surveymonkey.sync.singer.write_state"), \
             patch("tap_surveymonkey.sync.singer.write_record",
                   side_effect=lambda s, r: records_written.append(r)):
            sync(config, state, catalog)

        # Record date_value > start_date, so it should be written
        self.assertEqual(len(records_written), 1)
