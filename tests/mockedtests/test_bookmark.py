"""Integration tests for tap-surveymonkey bookmarking with mocked data.

Patches tap_surveymonkey.client.requests.request so no live API is called.
Run with: python -m pytest tests/test_bookmark.py -v
"""
import sys
import os
import unittest
from unittest.mock import patch

import singer

sys.path.insert(0, os.path.dirname(__file__))
from base import SurveyMonkeyBaseTest, MockResponse

from tap_surveymonkey.sync import sync


class SurveyMonkeyBookmarkTest(SurveyMonkeyBaseTest, unittest.TestCase):
    """Verify bookmark behaviour for INCREMENTAL streams."""

    # ── surveys stream ────────────────────────────────────────────────────────

    @patch("tap_surveymonkey.client.requests.request")
    def test_bookmark_surveys_is_written_after_sync(self, mock_req):
        """After syncing the surveys stream a bookmark must appear in state."""
        mock_req.side_effect = self._mock_request_fn(
            survey_id="s1", date_value="2024-03-01T00:00:00.000000Z")
        state = {}
        catalog = self._make_catalog(["surveys"])
        written_states = []

        with patch("tap_surveymonkey.sync.singer.write_record"), \
             patch("tap_surveymonkey.sync.singer.write_schema"), \
             patch("tap_surveymonkey.sync.singer.write_state",
                   side_effect=lambda s: written_states.append(s)):
            sync(self.config, state, catalog)

        self.assertTrue(len(written_states) > 0, "write_state was never called")
        last_state = written_states[-1]
        self.assertIn("bookmarks", last_state)

    @patch("tap_surveymonkey.client.requests.request")
    def test_bookmark_surveys_written_with_correct_stream_key(self, mock_req):
        """The surveys bookmark is stored under the 'surveys' key in state."""
        mock_req.side_effect = self._mock_request_fn(
            survey_id="s1", date_value="2024-03-01T00:00:00.000000Z")
        state = {}
        catalog = self._make_catalog(["surveys"])
        written_states = []

        with patch("tap_surveymonkey.sync.singer.write_record"), \
             patch("tap_surveymonkey.sync.singer.write_schema"), \
             patch("tap_surveymonkey.sync.singer.write_state",
                   side_effect=lambda s: written_states.append(s)):
            sync(self.config, state, catalog)

        last_state = written_states[-1]
        self.assertIn("surveys", last_state["bookmarks"])

    @patch("tap_surveymonkey.client.requests.request")
    def test_bookmark_surveys_filters_old_records(self, mock_req):
        """Second sync of surveys using a bookmark only returns records >= bookmark."""
        old_date  = "2023-01-01T00:00:00.000000Z"
        new_date  = "2024-03-01T00:00:00.000000Z"
        mid_date  = "2023-06-01T00:00:00.000000Z"

        # Mix of old and new records returned by the API
        def _routed(method, url, **kwargs):
            return MockResponse({
                "data": [
                    {"id": "s_old", "date_modified": old_date},
                    {"id": "s_new", "date_modified": new_date},
                ],
                "links": {},
                "total": 2,
            })

        mock_req.side_effect = _routed

        state = {"bookmarks": {"surveys": {"full_sync": mid_date}}}
        catalog = self._make_catalog(["surveys"])
        written_records = []

        with patch("tap_surveymonkey.sync.singer.write_schema"), \
             patch("tap_surveymonkey.sync.singer.write_state"), \
             patch("tap_surveymonkey.sync.singer.write_record",
                   side_effect=lambda s, r: written_records.append((s, r))):
            sync(self.config, state, catalog)

        surveys_written = [r for stream, r in written_records if stream == "surveys"]
        for record in surveys_written:
            self.assertGreaterEqual(record["date_modified"], mid_date,
                                    "Record before bookmark should have been skipped")

    # ── responses stream ──────────────────────────────────────────────────────

    @patch("tap_surveymonkey.client.requests.request")
    def test_bookmark_responses_written_after_sync(self, mock_req):
        """After syncing responses a bookmark entry must be written to state."""
        mock_req.side_effect = self._mock_request_fn(
            survey_id="s1", date_value="2024-03-01T00:00:00.000000Z")
        state = {}
        catalog = self._make_catalog(["responses"])
        written_states = []

        with patch("tap_surveymonkey.sync.singer.write_record"), \
             patch("tap_surveymonkey.sync.singer.write_schema"), \
             patch("tap_surveymonkey.sync.singer.write_state",
                   side_effect=lambda s: written_states.append(s)):
            sync(self.config, state, catalog)

        self.assertTrue(len(written_states) > 0)
        last_state = written_states[-1]
        self.assertIn("bookmarks", last_state)

    @patch("tap_surveymonkey.client.requests.request")
    def test_bookmark_responses_filters_old_records(self, mock_req):
        """Responses sync with a full_sync bookmark skips records before the bookmark.

        When no survey_id is in config, sync() looks up the responses bookmark
        under the 'full_sync' key (not per-survey), falling back to that path
        because config.get('survey_id') is None.
        """
        old_date = "2022-01-01T00:00:00.000000Z"
        new_date = "2024-03-01T00:00:00.000000Z"
        mid_date = "2023-01-01T00:00:00.000000Z"

        def _routed(method, url, **kwargs):
            if "/responses/bulk" in url:
                return MockResponse({
                    "data": [
                        {"id": "r_old", "survey_id": "s1", "date_modified": old_date},
                        {"id": "r_new", "survey_id": "s1", "date_modified": new_date},
                    ],
                    "links": {},
                    "total": 2,
                })
            # surveys list endpoint (parent iterator)
            return MockResponse({
                "data": [{"id": "s1", "date_modified": old_date}],
                "links": {},
                "total": 1,
            })

        mock_req.side_effect = _routed

        # With no survey_id in config, sync() resolves the bookmark via the
        # 'full_sync' sub-key, NOT the survey-id sub-key.
        state = {"bookmarks": {"responses": {"full_sync": mid_date}}}
        catalog = self._make_catalog(["responses"])
        written_records = []

        with patch("tap_surveymonkey.sync.singer.write_schema"), \
             patch("tap_surveymonkey.sync.singer.write_state"), \
             patch("tap_surveymonkey.sync.singer.write_record",
                   side_effect=lambda s, r: written_records.append((s, r))):
            sync(self.config, state, catalog)

        resp_records = [r for s, r in written_records if s == "responses"]
        for record in resp_records:
            self.assertGreaterEqual(record["date_modified"], mid_date)

    # ── full_sync bookmark ─────────────────────────────────────────────────

    @patch("tap_surveymonkey.client.requests.request")
    def test_full_sync_bookmark_written_under_full_sync_key(self, mock_req):
        """When no survey_id is in config, bookmark is written under 'full_sync'."""
        mock_req.side_effect = self._mock_request_fn(
            survey_id="s1", date_value="2024-03-01T00:00:00.000000Z")
        state = {}
        catalog = self._make_catalog(["surveys"])
        written_states = []

        with patch("tap_surveymonkey.sync.singer.write_record"), \
             patch("tap_surveymonkey.sync.singer.write_schema"), \
             patch("tap_surveymonkey.sync.singer.write_state",
                   side_effect=lambda s: written_states.append(s)):
            sync(self.config, state, catalog)

        last_state = written_states[-1]
        bk = last_state.get("bookmarks", {}).get("surveys", {})
        self.assertIn("full_sync", bk)
