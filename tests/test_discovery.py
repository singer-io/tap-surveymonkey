from base import SurveyMonkeyBaseTest
from tap_tester.base_suite_tests.discovery_test import DiscoveryTest


class SurveyMonkeyDiscoveryTest(DiscoveryTest, SurveyMonkeyBaseTest):
    """Validate discovery schema and metadata for tap-surveymonkey."""

    @staticmethod
    def name():
        return "tap_tester_surveymonkey_discovery_test"

    def streams_to_test(self):
        return self.expected_stream_names()
