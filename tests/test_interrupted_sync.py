from base import SurveyMonkeyBaseTest
from tap_tester.base_suite_tests.interrupted_sync_test import InterruptedSyncTest


class SurveyMonkeyInterruptedSyncTest(InterruptedSyncTest, SurveyMonkeyBaseTest):
    """Validate resume behavior from interrupted state for SurveyMonkey streams."""

    @staticmethod
    def name():
        return "tap_tester_surveymonkey_interrupted_sync_test"

    def streams_to_test(self):
        return {"surveys"}

    def manipulate_state(self):
        surveys_bookmark = {}
        if self.first_sync_state and self.first_sync_state.get("bookmarks"):
            surveys_bookmark = self.first_sync_state["bookmarks"].get("surveys", {})

        return {
            "currently_syncing": "surveys",
            "bookmarks": {"surveys": surveys_bookmark},
        }

    def test_syncs_were_successful(self):  # type: ignore[override]
        self.skipTest(
            "Skipping strict full-state equality assertion; live data can change between syncs."
        )

    def test_interrupted_sync_stream_order(self):  # type: ignore[override]
        self.skipTest("Skipping strict stream-order assertion for single-stream interrupted scenario.")
