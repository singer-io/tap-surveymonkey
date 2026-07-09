import os

from tap_tester.base_suite_tests.base_case import BaseCase


class SurveyMonkeyBaseTest(BaseCase):
    """Tap-tester base configuration for tap-surveymonkey."""

    start_date = "2020-01-01T00:00:00Z"

    @staticmethod
    def tap_name():
        return "tap-surveymonkey"

    @staticmethod
    def get_type():
        return "platform.surveymonkey"

    @classmethod
    def expected_metadata(cls):
        return {
            "surveys": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"date_modified"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 50,
            },
            "survey_details": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"date_modified"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 50,
                cls.PARENT_STREAM: "surveys",
            },
            "responses": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"date_modified"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 50,
                cls.PARENT_STREAM: "surveys",
            },
            "simplified_responses": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"date_modified"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 50,
                cls.PARENT_STREAM: "surveys",
            },
        }

    def expected_stream_names(self):
        return set(self.expected_metadata().keys())

    @staticmethod
    def get_credentials():
        return {
            "access_token": os.getenv("TAP_SURVEYMONKEY_ACCESS_TOKEN"),
        }

    def get_properties(self, original=True):
        props = {
            "start_date": os.getenv("TAP_SURVEYMONKEY_START_DATE", self.start_date),
            "page_size": os.getenv("TAP_SURVEYMONKEY_PAGE_SIZE", "50"),
        }

        survey_id = os.getenv("TAP_SURVEYMONKEY_SURVEY_ID")
        if survey_id:
            props["survey_id"] = survey_id

        if original:
            return props

        props["start_date"] = self.start_date
        return props
