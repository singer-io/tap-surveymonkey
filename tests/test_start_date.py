"""Test start_date filtering and API parameter translation."""
from unittest.mock import Mock
from tap_surveymonkey.streams import Surveys
from base import SurveyMonkeyBaseTest

_SURVEYS = Surveys(stream_id="surveys", path="surveys")
_MOCK_STREAM = Mock(metadata=[])


class SurveyMonkeyStartDateTest(SurveyMonkeyBaseTest):
    """Verify start_date is translated into the correct API start_modified_at param."""

    def test_start_date_used_as_initial_bookmark(self):
        """With no prior bookmark, start_date (minus 1 min guard) becomes start_modified_at."""
        params = _SURVEYS.get_params(
            _MOCK_STREAM, {"start_date": "2021-01-01T00:00:00Z"}, {}, bookmark_value=None
        )
        self.assertIn("start_modified_at", params)
        self.assertIn("2020-12-31T23:59", params["start_modified_at"])

    def test_bookmark_overrides_start_date(self):
        """An existing bookmark takes precedence over start_date."""
        params = _SURVEYS.get_params(
            _MOCK_STREAM, {"start_date": "2021-01-01T00:00:00Z"}, {},
            bookmark_value="2022-06-01T00:00:00.000000Z"
        )
        self.assertIn("2022", params["start_modified_at"])

    def test_start_date_required_in_config(self):
        """start_date must be declared in REQUIRED_CONFIG_KEYS."""
        from tap_surveymonkey import REQUIRED_CONFIG_KEYS
        self.assertIn("start_date", REQUIRED_CONFIG_KEYS)
