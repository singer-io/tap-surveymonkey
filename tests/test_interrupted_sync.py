"""Integration tests for tap-surveymonkey interrupted sync resumption.

Verifies that when a sync is interrupted mid-stream and restarted with the
saved state, it resumes from the correct bookmark without duplicating records.

Patches tap_surveymonkey.client.requests.request so no live API is called.
Run with: python -m pytest tests/test_interrupted_sync.py -v
"""
import sys
import os
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(__file__))
from base import SurveyMonkeyBaseTest, MockResponse

from tap_surveymonkey.sync import sync


class SurveyMonkeyInterruptedSyncTest(SurveyMonkeyBaseTest, unittest.TestCase):
    """Verify that sync correctly resumes after an interruption."""

    # ── surveys ───────────────────────────────────────────────────────────────

    @patch("tap_surveymonkey.client.requests.request")
    def test_interrupted_surveys_sync_resumes_from_bookmark(self, mock_req):
        """Resumed surveys sync skips records already covered by the saved bookmark."""
        old_date = "2022-01-01T00:00:00.000000Z"
        new_date = "2024-03-01T00:00:00.000000Z"
        interrupted_bookmark = "2023-01-01T00:00:00.000000Z"

        # Both old and new records will be returned by the API;
        # sync() should filter out old_date because it precedes the bookmark.
        mock_req.return_value = MockResponse({
            "data": [
                {"id": "s_before", "date_modified": old_date},
                {"id": "s_after",  "date_modified": new_date},
            ],
            "links": {},
            "total": 2,
        })

        # State left by interrupted first sync
        interrupted_state = {
            "bookmarks": {
                "surveys": {"full_sync": interrupted_bookmark}
            }
        }

        catalog = self._make_catalog(["surveys"])
        records_after_resume = []

        with patch("tap_surveymonkey.sync.singer.write_schema"), \
             patch("tap_surveymonkey.sync.singer.write_state"), \
             patch("tap_surveymonkey.sync.singer.write_record",
                   side_effect=lambda s, r: records_after_resume.append((s, r))):
            sync(self.config, interrupted_state, catalog)

        surveys = [r for s, r in records_after_resume if s == "surveys"]
        for record in surveys:
            self.assertGreaterEqual(
                record["date_modified"], interrupted_bookmark,
                "Resumed sync should not replay records before the bookmark",
            )

    @patch("tap_surveymonkey.client.requests.request")
    def test_interrupted_surveys_only_new_record_written(self, mock_req):
        """Only the record whose date_modified >= bookmark is written on resume."""
        old_date = "2022-01-01T00:00:00.000000Z"
        new_date = "2024-03-01T00:00:00.000000Z"
        interrupted_bookmark = "2023-01-01T00:00:00.000000Z"

        mock_req.return_value = MockResponse({
            "data": [
                {"id": "s_before", "date_modified": old_date},
                {"id": "s_after",  "date_modified": new_date},
            ],
            "links": {},
            "total": 2,
        })

        interrupted_state = {
            "bookmarks": {"surveys": {"full_sync": interrupted_bookmark}}
        }

        catalog = self._make_catalog(["surveys"])
        records_written = []

        with patch("tap_surveymonkey.sync.singer.write_schema"), \
             patch("tap_surveymonkey.sync.singer.write_state"), \
             patch("tap_surveymonkey.sync.singer.write_record",
                   side_effect=lambda s, r: records_written.append((s, r))):
            sync(self.config, interrupted_state, catalog)

        surveys = [r for s, r in records_written if s == "surveys"]
        self.assertEqual(len(surveys), 1)
        self.assertEqual(surveys[0]["id"], "s_after")

    # ── responses ─────────────────────────────────────────────────────────────

    @patch("tap_surveymonkey.client.requests.request")
    def test_interrupted_responses_sync_resumes_from_bookmark(self, mock_req):
        """Resumed responses sync skips records already covered by the saved bookmark.

        With no survey_id in config sync() resolves the responses bookmark via
        the 'full_sync' sub-key (fallback path when config survey_id is None).
        """
        old_date = "2022-06-01T00:00:00.000000Z"
        new_date = "2024-03-01T00:00:00.000000Z"
        interrupted_bookmark = "2023-06-01T00:00:00.000000Z"

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
            # Parent survey iterator
            return MockResponse({
                "data": [{"id": "s1", "date_modified": old_date}],
                "links": {},
                "total": 1,
            })

        mock_req.side_effect = _routed

        # With no survey_id in config the bookmark is stored/read under 'full_sync'
        interrupted_state = {
            "bookmarks": {
                "responses": {"full_sync": interrupted_bookmark}
            }
        }

        catalog = self._make_catalog(["responses"])
        records_after_resume = []

        with patch("tap_surveymonkey.sync.singer.write_schema"), \
             patch("tap_surveymonkey.sync.singer.write_state"), \
             patch("tap_surveymonkey.sync.singer.write_record",
                   side_effect=lambda s, r: records_after_resume.append((s, r))):
            sync(self.config, interrupted_state, catalog)

        responses = [r for s, r in records_after_resume if s == "responses"]
        for record in responses:
            self.assertGreaterEqual(
                record["date_modified"], interrupted_bookmark,
                "Resumed responses sync should not replay records before bookmark",
            )

    # ── fresh sync without prior state ────────────────────────────────────────

    @patch("tap_surveymonkey.client.requests.request")
    def test_fresh_sync_with_no_state_writes_all_records(self, mock_req):
        """When state is empty (first sync), all returned records are written."""
        date_val = "2024-01-01T00:00:00.000000Z"
        mock_req.side_effect = self._mock_request_fn(
            survey_id="s1", date_value=date_val)

        catalog = self._make_catalog(["surveys"])
        records_written = []

        with patch("tap_surveymonkey.sync.singer.write_schema"), \
             patch("tap_surveymonkey.sync.singer.write_state"), \
             patch("tap_surveymonkey.sync.singer.write_record",
                   side_effect=lambda s, r: records_written.append((s, r))):
            sync(self.config, {}, catalog)

        surveys = [r for s, r in records_written if s == "surveys"]
        self.assertEqual(len(surveys), 1)

    # ── state written after interruption point ───────────────────────────────

    @patch("tap_surveymonkey.client.requests.request")
    def test_state_is_written_incrementally_for_responses(self, mock_req):
        """For sorted INCREMENTAL non-surveys streams, state is written after each record.

        surveys has a special code path (stream.tap_stream_id != 'surveys' guard)
        that skips per-record state writes. responses/simplified_responses DO
        write state per record because they are sorted and not the surveys stream.
        """
        survey_date = "2024-01-01T00:00:00.000000Z"
        resp_date1  = "2024-01-01T00:00:00.000000Z"
        resp_date2  = "2024-02-01T00:00:00.000000Z"

        def _routed(method, url, **kwargs):
            if "/responses/bulk" in url:
                return MockResponse({
                    "data": [
                        {"id": "r1", "survey_id": "s1", "date_modified": resp_date1},
                        {"id": "r2", "survey_id": "s1", "date_modified": resp_date2},
                    ],
                    "links": {},
                    "total": 2,
                })
            return MockResponse({
                "data": [{"id": "s1", "date_modified": survey_date}],
                "links": {},
                "total": 1,
            })

        mock_req.side_effect = _routed

        catalog = self._make_catalog(["responses"])
        state_writes = []

        with patch("tap_surveymonkey.sync.singer.write_schema"), \
             patch("tap_surveymonkey.sync.singer.write_record"), \
             patch("tap_surveymonkey.sync.singer.write_state",
                   side_effect=lambda s: state_writes.append(s)):
            sync(self.config, {}, catalog)

        # 2 per-record state writes (sorted non-surveys stream) + 1 final full_sync write
        self.assertGreaterEqual(len(state_writes), 2,
                                "write_state should be called per-record for responses (sorted stream)")
