from base import SurveyMonkeyBaseTest
from tap_tester.base_suite_tests.automatic_fields_test import MinimumSelectionTest


class SurveyMonkeyAutomaticFieldsTest(MinimumSelectionTest, SurveyMonkeyBaseTest):
    """Validate automatic fields are emitted when user fields are deselected."""

    @staticmethod
    def name():
        return "tap_tester_surveymonkey_automatic_fields_test"

    def streams_to_test(self):
        return {"surveys", "survey_details"}
