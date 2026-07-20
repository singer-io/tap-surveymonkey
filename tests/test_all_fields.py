from base import SurveyMonkeyBaseTest
from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest


class SurveyMonkeyAllFieldsTest(AllFieldsTest, SurveyMonkeyBaseTest):
    """Validate that selected fields are replicated for SurveyMonkey streams."""

    MISSING_FIELDS = {
        "survey_details": {"quiz_options"},
    }

    @staticmethod
    def name():
        return "tap_tester_surveymonkey_all_fields_test"

    def streams_to_test(self):
        return {"surveys", "survey_details"}
