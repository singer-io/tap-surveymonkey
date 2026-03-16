"""
Unit tests for the SurveyMonkey API client.

Covers: initialization, request building, HTTP methods, URL construction,
error handling, rate-limit logic (day / minute / both / no-state), and
extra request kwargs.
"""
import unittest
from unittest.mock import Mock, patch

from tap_surveymonkey.client import SurveyMonkeyClient


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ok(data=None, headers=None):
    """Build a successful mock response."""
    r = Mock()
    r.status_code = 200
    r.json.return_value = data if data is not None else {}
    r.headers = headers or {"X-Ratelimit-App-Global-Minute-Remaining": "100"}
    return r


def _rate_limited(day_rem=1000, day_reset=10, min_rem=100, min_reset=5):
    """Build a 429 mock response."""
    r = Mock()
    r.status_code = 429
    r.headers = {
        "X-Ratelimit-App-Global-Day-Remaining": str(day_rem),
        "X-Ratelimit-App-Global-Day-Reset": str(day_reset),
        "X-Ratelimit-App-Global-Minute-Remaining": str(min_rem),
        "X-Ratelimit-App-Global-Minute-Reset": str(min_reset),
    }
    return r


def _error_response(status, message="error"):
    """Build an error mock response."""
    r = Mock()
    r.status_code = status
    r.json.return_value = {"error": {"message": message, "id": str(status)}}
    return r


# ---------------------------------------------------------------------------
# Initialization
# ---------------------------------------------------------------------------

class TestClientInit(unittest.TestCase):

    def test_stores_access_token(self):
        c = SurveyMonkeyClient("tok_abc")
        self.assertEqual(c.access_token, "tok_abc")


# ---------------------------------------------------------------------------
# Basic request building
# ---------------------------------------------------------------------------

class TestClientRequests(unittest.TestCase):

    def setUp(self):
        self.client = SurveyMonkeyClient("test_token")

    @patch("tap_surveymonkey.client.requests.request")
    def test_success_returns_json(self, mock_req):
        mock_req.return_value = _ok({"data": [{"id": "1"}]})
        result = self.client.make_request("surveys")
        self.assertEqual(result, {"data": [{"id": "1"}]})

    @patch("tap_surveymonkey.client.requests.request")
    def test_auth_header_is_bearer(self, mock_req):
        mock_req.return_value = _ok()
        self.client.make_request("surveys")
        headers = mock_req.call_args[1]["headers"]
        self.assertEqual(headers["Authorization"], "bearer test_token")
        self.assertEqual(headers["Content-Type"], "application/json")

    @patch("tap_surveymonkey.client.requests.request")
    def test_url_construction(self, mock_req):
        mock_req.return_value = _ok()
        self.client.make_request("surveys/123/details")
        url = mock_req.call_args[0][1]
        self.assertEqual(url, "https://api.surveymonkey.com/v3/surveys/123/details")

    @patch("tap_surveymonkey.client.requests.request")
    def test_default_method_is_get(self, mock_req):
        mock_req.return_value = _ok()
        self.client.make_request("surveys")
        self.assertEqual(mock_req.call_args[0][0], "GET")

    @patch("tap_surveymonkey.client.requests.request")
    def test_custom_methods(self, mock_req):
        mock_req.return_value = _ok()
        for method in ("POST", "PUT", "DELETE"):
            self.client.make_request("surveys", method=method)
            self.assertEqual(mock_req.call_args[0][0], method)

    @patch("tap_surveymonkey.client.requests.request")
    def test_passes_params(self, mock_req):
        mock_req.return_value = _ok()
        self.client.make_request("surveys", params={"page": 1, "per_page": 50})
        self.assertEqual(mock_req.call_args[1]["params"], {"page": 1, "per_page": 50})

    @patch("tap_surveymonkey.client.requests.request")
    def test_404_returns_none(self, mock_req):
        r = Mock()
        r.status_code = 404
        mock_req.return_value = r
        self.assertIsNone(self.client.make_request("surveys/missing"))


# ---------------------------------------------------------------------------
# Error responses (4xx / 5xx that are not 429 or 404)
# ---------------------------------------------------------------------------

class TestClientErrorResponses(unittest.TestCase):

    def setUp(self):
        self.client = SurveyMonkeyClient("test_token")

    @patch("tap_surveymonkey.client.requests.request")
    def test_400_returns_error_json(self, mock_req):
        mock_req.return_value = _error_response(400, "Invalid request")
        result = self.client.make_request("surveys")
        self.assertIn("error", result)

    @patch("tap_surveymonkey.client.requests.request")
    def test_401_returns_error_json(self, mock_req):
        mock_req.return_value = _error_response(401, "Unauthorized")
        result = self.client.make_request("surveys")
        self.assertIn("error", result)

    @patch("tap_surveymonkey.client.requests.request")
    def test_403_returns_error_json(self, mock_req):
        mock_req.return_value = _error_response(403, "Forbidden")
        result = self.client.make_request("surveys")
        self.assertIn("error", result)

    @patch("tap_surveymonkey.client.requests.request")
    def test_500_returns_error_json(self, mock_req):
        mock_req.return_value = _error_response(500, "Server error")
        result = self.client.make_request("surveys")
        self.assertIn("error", result)


# ---------------------------------------------------------------------------
# Extra kwargs forwarding
# ---------------------------------------------------------------------------

class TestClientKwargs(unittest.TestCase):

    def setUp(self):
        self.client = SurveyMonkeyClient("test_token")

    @patch("tap_surveymonkey.client.requests.request")
    def test_json_body_forwarded(self, mock_req):
        mock_req.return_value = _ok({"success": True})
        body = {"name": "Survey"}
        self.client.make_request("surveys", method="POST", json=body)
        self.assertEqual(mock_req.call_args[1]["json"], body)

    @patch("tap_surveymonkey.client.requests.request")
    def test_form_data_forwarded(self, mock_req):
        mock_req.return_value = _ok({"success": True})
        data = {"key": "value"}
        self.client.make_request("surveys", method="POST", data=data)
        self.assertEqual(mock_req.call_args[1]["data"], data)

    @patch("tap_surveymonkey.client.requests.request")
    def test_custom_timeout_forwarded(self, mock_req):
        mock_req.return_value = _ok()
        self.client.make_request("surveys", timeout=30)
        self.assertEqual(mock_req.call_args[1]["timeout"], 30)


# ---------------------------------------------------------------------------
# Rate-limit handling
# ---------------------------------------------------------------------------

class TestRateLimits(unittest.TestCase):

    def setUp(self):
        self.client = SurveyMonkeyClient("test_token")

    @patch("tap_surveymonkey.client.time.sleep")
    @patch("tap_surveymonkey.client.singer.write_state")
    @patch("tap_surveymonkey.client.requests.request")
    def test_day_limit_exhausted_sleeps_and_retries(self, mock_req, mock_ws, mock_sleep):
        ok = _ok({"data": []})
        ok.headers = {"X-Ratelimit-App-Global-Minute-Remaining": "100"}
        mock_req.side_effect = [_rate_limited(day_rem=0, day_reset=10), ok]
        state = {"bookmarks": {}}

        result = self.client.make_request("surveys", state=state)

        mock_sleep.assert_called_once_with(12)   # reset + 2
        mock_ws.assert_called_once_with(state)
        self.assertEqual(result, {"data": []})

    @patch("tap_surveymonkey.client.time.sleep")
    @patch("tap_surveymonkey.client.singer.write_state")
    @patch("tap_surveymonkey.client.requests.request")
    def test_minute_limit_exhausted_sleeps_and_retries(self, mock_req, mock_ws, mock_sleep):
        ok = _ok({"data": []})
        ok.headers = {"X-Ratelimit-App-Global-Minute-Remaining": "100"}
        mock_req.side_effect = [_rate_limited(min_rem=0, min_reset=5), ok]
        state = {"bookmarks": {}}

        result = self.client.make_request("surveys", state=state)

        mock_sleep.assert_called_once_with(7)    # reset + 2
        mock_ws.assert_called_once_with(state)
        self.assertEqual(result, {"data": []})

    @patch("tap_surveymonkey.client.time.sleep")
    @patch("tap_surveymonkey.client.singer.write_state")
    @patch("tap_surveymonkey.client.requests.request")
    def test_both_limits_exhausted_day_checked_first(self, mock_req, mock_ws, mock_sleep):
        ok = _ok({"data": []})
        ok.headers = {"X-Ratelimit-App-Global-Minute-Remaining": "100"}
        mock_req.side_effect = [
            _rate_limited(day_rem=0, day_reset=20, min_rem=0, min_reset=5),
            ok,
        ]
        state = {"bookmarks": {}}
        self.client.make_request("surveys", state=state)
        # day limit checked first → sleep(22)
        mock_sleep.assert_called_once_with(22)

    @patch("tap_surveymonkey.client.time.sleep")
    @patch("tap_surveymonkey.client.singer.write_state")
    @patch("tap_surveymonkey.client.requests.request")
    def test_sequential_day_then_minute_limits(self, mock_req, mock_ws, mock_sleep):
        ok = _ok({"data": []})
        ok.headers = {"X-Ratelimit-App-Global-Minute-Remaining": "100"}
        day_limited = _rate_limited(day_rem=0, day_reset=10, min_rem=100)
        minute_limited = _rate_limited(day_rem=100, min_rem=0, min_reset=5)
        mock_req.side_effect = [day_limited, minute_limited, ok]
        state = {"bookmarks": {}}

        self.client.make_request("surveys", state=state)

        self.assertEqual(mock_ws.call_count, 2)
        self.assertEqual(mock_sleep.call_count, 2)

    @patch("tap_surveymonkey.client.time.sleep")
    @patch("tap_surveymonkey.client.requests.request")
    def test_rate_limit_without_state_does_not_crash(self, mock_req, mock_sleep):
        ok = _ok({"data": []})
        ok.headers = {"X-Ratelimit-App-Global-Minute-Remaining": "100"}
        mock_req.side_effect = [_rate_limited(day_rem=0, day_reset=10), ok]

        result = self.client.make_request("surveys", state=None)

        mock_sleep.assert_called_once_with(12)
        self.assertEqual(result, {"data": []})

    @patch("tap_surveymonkey.client.time.sleep")
    @patch("tap_surveymonkey.client.requests.request")
    def test_no_rate_limit_no_sleep(self, mock_req, mock_sleep):
        mock_req.return_value = _ok({"data": []})
        self.client.make_request("surveys")
        mock_sleep.assert_not_called()

    @patch("tap_surveymonkey.client.requests.request")
    def test_missing_rate_limit_headers_raises(self, mock_req):
        r = Mock()
        r.status_code = 429
        r.headers = {}
        mock_req.return_value = r
        with self.assertRaises(KeyError):
            self.client.make_request("surveys")


if __name__ == "__main__":
    unittest.main()
