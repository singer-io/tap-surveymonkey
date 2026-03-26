"""Integration tests for tap-surveymonkey pagination with mocked data.

Patches tap_surveymonkey.client.requests.request so no live API is called.
Run with: python -m pytest tests/test_pagination.py -v
"""
import sys
import os
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(__file__))
from base import SurveyMonkeyBaseTest, MockResponse

from tap_surveymonkey.sync import sync


def _page(data, has_next=False):
    return MockResponse({
        "data": data,
        "links": {"next": "?page=2"} if has_next else {},
        "total": len(data),
    })


class SurveyMonkeyPaginationTest(SurveyMonkeyBaseTest, unittest.TestCase):
    """Verify page-based pagination for streams that use PaginatedStream."""

    # ── surveys — paginated ───────────────────────────────────────────────────

    @patch("tap_surveymonkey.client.requests.request")
    def test_surveys_fetches_multiple_pages(self, mock_req):
        """surveys stream should keep fetching pages until no 'next' link."""
        page1_data = [{"id": f"s{i}", "date_modified": "2024-01-01T00:00:00.000000Z"}
                      for i in range(50)]
        page2_data = [{"id": "s_extra", "date_modified": "2024-01-02T00:00:00.000000Z"}]

        mock_req.side_effect = [
            _page(page1_data, has_next=True),   # page 1 → has next
            _page(page2_data, has_next=False),  # page 2 → stop
        ]

        catalog = self._make_catalog(["surveys"])
        records_written = []

        with patch("tap_surveymonkey.sync.singer.write_schema"), \
             patch("tap_surveymonkey.sync.singer.write_state"), \
             patch("tap_surveymonkey.sync.singer.write_record",
                   side_effect=lambda s, r: records_written.append((s, r))):
            sync({**self.config, "start_date": None}, {}, catalog)

        surveys_written = [r for s, r in records_written if s == "surveys"]
        self.assertEqual(len(surveys_written), 51)
        self.assertEqual(mock_req.call_count, 2)

    @patch("tap_surveymonkey.client.requests.request")
    def test_surveys_single_page_stops_correctly(self, mock_req):
        """surveys stream stops after one page when no 'next' link is present."""
        data = [{"id": f"s{i}", "date_modified": "2024-01-01T00:00:00.000000Z"}
                for i in range(5)]

        mock_req.return_value = _page(data, has_next=False)

        catalog = self._make_catalog(["surveys"])
        records_written = []

        with patch("tap_surveymonkey.sync.singer.write_schema"), \
             patch("tap_surveymonkey.sync.singer.write_state"), \
             patch("tap_surveymonkey.sync.singer.write_record",
                   side_effect=lambda s, r: records_written.append((s, r))):
            sync({**self.config, "start_date": None}, {}, catalog)

        surveys_written = [r for s, r in records_written if s == "surveys"]
        self.assertEqual(len(surveys_written), 5)
        self.assertEqual(mock_req.call_count, 1)

    # ── responses — paginated ─────────────────────────────────────────────────

    @patch("tap_surveymonkey.client.requests.request")
    def test_responses_fetches_multiple_pages(self, mock_req):
        """responses stream should iterate through multiple pages per survey."""
        survey_list_resp = MockResponse({
            "data": [{"id": "s1", "date_modified": "2024-01-01T00:00:00.000000Z"}],
            "links": {},
            "total": 1,
        })
        resp_page1_data = [
            {"id": f"r{i}", "survey_id": "s1",
             "date_modified": "2024-01-01T00:00:00.000000Z"}
            for i in range(50)
        ]
        resp_page2_data = [
            {"id": "r_extra", "survey_id": "s1",
             "date_modified": "2024-01-02T00:00:00.000000Z"}
        ]

        def _routed(method, url, **kwargs):
            if "/responses/bulk" in url:
                # Track how many times /responses/bulk has been called
                call_n = sum(
                    1 for c in mock_req.call_args_list
                    if "/responses/bulk" in c.args[1]
                )
                if call_n <= 1:
                    return MockResponse({
                        "data": resp_page1_data,
                        "links": {"next": "?page=2"},
                        "total": len(resp_page1_data),
                    })
                return MockResponse({
                    "data": resp_page2_data,
                    "links": {},
                    "total": len(resp_page2_data),
                })
            return survey_list_resp

        mock_req.side_effect = _routed

        catalog = self._make_catalog(["responses"])
        records_written = []

        with patch("tap_surveymonkey.sync.singer.write_schema"), \
             patch("tap_surveymonkey.sync.singer.write_state"), \
             patch("tap_surveymonkey.sync.singer.write_record",
                   side_effect=lambda s, r: records_written.append((s, r))):
            sync({**self.config, "start_date": None}, {}, catalog)

        resp_records = [r for s, r in records_written if s == "responses"]
        self.assertEqual(len(resp_records), 51)

    @patch("tap_surveymonkey.client.requests.request")
    def test_page_size_defaults_to_50(self, mock_req):
        """Default page size of 50 is sent as per_page in the API request params."""
        mock_req.return_value = _page([], has_next=False)

        catalog = self._make_catalog(["surveys"])
        with patch("tap_surveymonkey.sync.singer.write_schema"), \
             patch("tap_surveymonkey.sync.singer.write_state"), \
             patch("tap_surveymonkey.sync.singer.write_record"):
            sync({**self.config, "start_date": None}, {}, catalog)

        called_params = [
            call.kwargs.get("params") or {}
            for call in mock_req.call_args_list
        ]
        per_page_values = [p["per_page"] for p in called_params if "per_page" in p]
        self.assertTrue(len(per_page_values) > 0)
        for pv in per_page_values:
            self.assertEqual(pv, 50)

    @patch("tap_surveymonkey.client.requests.request")
    def test_custom_page_size_overrides_default(self, mock_req):
        """Config page_size overrides the default of 50."""
        mock_req.return_value = _page([], has_next=False)

        config = {**self.config, "page_size": "25", "start_date": None}
        catalog = self._make_catalog(["surveys"])
        with patch("tap_surveymonkey.sync.singer.write_schema"), \
             patch("tap_surveymonkey.sync.singer.write_state"), \
             patch("tap_surveymonkey.sync.singer.write_record"):
            sync(config, {}, catalog)

        called_params = [
            call.kwargs.get("params") or {}
            for call in mock_req.call_args_list
        ]
        per_page_values = [p["per_page"] for p in called_params if "per_page" in p]
        self.assertTrue(len(per_page_values) > 0)
        for pv in per_page_values:
            self.assertEqual(pv, 25)
