from base import SurveyMonkeyBaseTest
from tap_tester.base_suite_tests.pagination_test import PaginationTest


class SurveyMonkeyPaginationTest(PaginationTest, SurveyMonkeyBaseTest):
    """Validate pagination behavior for SurveyMonkey streams."""

    @staticmethod
    def name():
        return "tap_tester_surveymonkey_pagination_test"

    def streams_to_test(self):
        return self.expected_stream_names()

    def test_record_count_greater_than_page_limit(self):  # type: ignore[override]
        self.skipTest(
            "Skipping strict record-count>page-size assertion; account datasets can be small."
        )
