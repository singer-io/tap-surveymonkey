import unittest
from unittest.mock import patch

from tap_surveymonkey.streams import PaginatedStream


class MockResponse:

    def __init__(self, json_data):
        self.json_data = json_data

    def json(self):
        return self.json_data


mock_config = {
    "anchor_time": "2023-09-08T14:05:31.979Z",
    "cron_expression": None,
    "frequency_in_minutes": "60",
    "start_date": "2021-09-01T15:40:00Z",
    "survey_id": "12345678",
    "access_token": "abc.123.456"
}


class TestSurveymonkey(unittest.TestCase):

    def test_get_params(self):
        """
        # Written a function to test the params while making requests
        """
        expected_return_value = {"per_page": 50, "page": 1}

        actual_return_value = PaginatedStream.get_params(
            self, stream={}, config=mock_config, state={}, bookmark_value=None)

        self.assertEqual(expected_return_value, actual_return_value)
