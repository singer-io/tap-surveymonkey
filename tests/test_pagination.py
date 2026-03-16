"""Test pagination through PaginatedStream.fetch_data."""
from unittest.mock import patch
from tap_surveymonkey.streams import PaginatedStream
from tap_surveymonkey.client import SurveyMonkeyClient
from base import SurveyMonkeyBaseTest

_STREAM = PaginatedStream(stream_id="test", path="test")
_CLIENT = SurveyMonkeyClient("token")


class SurveyMonkeyPaginationTest(SurveyMonkeyBaseTest):
    """Verify PaginatedStream pages through all results and respects page-size config."""

    @patch.object(SurveyMonkeyClient, "make_request")
    def test_fetches_all_pages(self, mock_request):
        """All records across multiple pages are returned; next link is followed."""
        mock_request.side_effect = [
            {"data": [{"id": "1"}, {"id": "2"}], "links": {"next": "url"}},
            {"data": [{"id": "3"}],               "links": {}},
        ]
        results = list(_STREAM.fetch_data(_CLIENT, None, {}, {}))
        self.assertEqual(len(results), 3)
        self.assertEqual(mock_request.call_count, 2)

    @patch.object(SurveyMonkeyClient, "make_request")
    def test_page_param_sent_on_every_request(self, mock_request):
        """page query param is included in every API call."""
        mock_request.side_effect = [
            {"data": [{"id": "1"}], "links": {"next": "url"}},
            {"data": [{"id": "2"}], "links": {}},
        ]
        list(_STREAM.fetch_data(_CLIENT, None, {}, {}))
        for call in mock_request.call_args_list:
            self.assertIn("page", call[1]["params"])

    @patch.object(SurveyMonkeyClient, "make_request")
    def test_per_page_respects_config(self, mock_request):
        """Default per_page=50; page_size config key overrides it."""
        mock_request.return_value = {"data": [], "links": {}}
        list(_STREAM.fetch_data(_CLIENT, None, {}, {}))
        self.assertEqual(mock_request.call_args[1]["params"]["per_page"], 50)

        mock_request.reset_mock()
        list(_STREAM.fetch_data(_CLIENT, None, {"page_size": "100"}, {}))
        self.assertEqual(mock_request.call_args[1]["params"]["per_page"], 100)
