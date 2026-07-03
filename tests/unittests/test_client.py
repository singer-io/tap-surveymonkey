"""Unit tests for tap_surveymonkey/client.py (updated for PR #44)."""

import unittest
from unittest.mock import MagicMock, patch

from tap_surveymonkey.client import (
    SurveyMonkeyClient,
    _get_rate_limit_sleep_seconds,
    _on_backoff,
)
from tap_surveymonkey.exceptions import (
    SurveyMonkeyBadGatewayError,
    SurveyMonkeyBadRequestError,
    SurveyMonkeyError,
    SurveyMonkeyForbiddenError,
    SurveyMonkeyGatewayTimeoutError,
    SurveyMonkeyInternalServerError,
    SurveyMonkeyMethodNotAllowedError,
    SurveyMonkeyNotFoundError,
    SurveyMonkeyRateLimitError,
    SurveyMonkeyRequestEntityTooLargeError,
    SurveyMonkeyServiceUnavailableError,
    SurveyMonkeyUnauthorizedError,
    SurveyMonkeyUnprocessableEntityError,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_resp(status_code, json_data=None, headers=None, text=""):
    """Build a minimal mock HTTP response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data if json_data is not None else {}
    resp.headers = headers or {}
    resp.text = text
    return resp


# ---------------------------------------------------------------------------
# Client initialisation
# ---------------------------------------------------------------------------

class TestClientInit(unittest.TestCase):

    def test_stores_access_token(self):
        """Client stores the access_token passed at construction."""
        client = SurveyMonkeyClient("my-token")
        self.assertEqual(client.access_token, "my-token")


class TestClientTokenValidation(unittest.TestCase):

    @patch.object(SurveyMonkeyClient, "make_request")
    def test_validate_access_token_calls_users_me_endpoint(self, mock_make_request):
        """validate_access_token checks token by calling users/me."""
        client = SurveyMonkeyClient("my-token")
        client.validate_access_token()

        mock_make_request.assert_called_once_with("users/me")


# ---------------------------------------------------------------------------
# make_request � happy path
# ---------------------------------------------------------------------------

class TestClientMakeRequestSuccess(unittest.TestCase):

    @patch("tap_surveymonkey.client.requests.request")
    def test_bearer_auth_header_sent(self, mock_request):
        """make_request includes the correct Bearer authorization header."""
        mock_request.return_value = _make_resp(200, {"id": "1"})
        SurveyMonkeyClient("tok123").make_request("surveys")
        _, kwargs = mock_request.call_args
        self.assertEqual(kwargs["headers"]["Authorization"], "bearer tok123")

    @patch("tap_surveymonkey.client.requests.request")
    def test_content_type_header_sent(self, mock_request):
        """make_request includes Content-Type: application/json header."""
        mock_request.return_value = _make_resp(200, {})
        SurveyMonkeyClient("tok").make_request("surveys")
        _, kwargs = mock_request.call_args
        self.assertEqual(kwargs["headers"]["Content-Type"], "application/json")

    @patch("tap_surveymonkey.client.requests.request")
    def test_correct_url_built(self, mock_request):
        """make_request constructs https://api.surveymonkey.com/v3/<endpoint>."""
        mock_request.return_value = _make_resp(200, {})
        SurveyMonkeyClient("tok").make_request("surveys/123/details")
        args, _ = mock_request.call_args
        self.assertEqual(args[1], "https://api.surveymonkey.com/v3/surveys/123/details")

    @patch("tap_surveymonkey.client.requests.request")
    def test_default_http_method_is_get(self, mock_request):
        """make_request uses GET when no method argument is provided."""
        mock_request.return_value = _make_resp(200, {})
        SurveyMonkeyClient("tok").make_request("surveys")
        args, _ = mock_request.call_args
        self.assertEqual(args[0], "GET")

    @patch("tap_surveymonkey.client.requests.request")
    def test_post_method_forwarded(self, mock_request):
        """make_request uses the HTTP method passed as an argument."""
        mock_request.return_value = _make_resp(200, {})
        SurveyMonkeyClient("tok").make_request("surveys", method="POST")
        args, _ = mock_request.call_args
        self.assertEqual(args[0], "POST")

    @patch("tap_surveymonkey.client.requests.request")
    def test_returns_parsed_json_on_success(self, mock_request):
        """make_request returns the parsed JSON body on a 200 response."""
        payload = {"id": "42", "title": "Survey"}
        mock_request.return_value = _make_resp(200, payload)
        result = SurveyMonkeyClient("tok").make_request("surveys/42")
        self.assertEqual(result, payload)

    @patch("tap_surveymonkey.client.requests.request")
    def test_returns_parsed_json_on_201(self, mock_request):
        """make_request returns the parsed JSON body on a 201 Created response."""
        payload = {"id": "99"}
        mock_request.return_value = _make_resp(201, payload)
        result = SurveyMonkeyClient("tok").make_request("surveys", method="POST")
        self.assertEqual(result, payload)

    @patch("tap_surveymonkey.client.requests.request")
    def test_timeout_30_passed_to_requests(self, mock_request):
        """make_request passes timeout=30 to requests.request (PR #44 addition)."""
        mock_request.return_value = _make_resp(200, {})
        SurveyMonkeyClient("tok").make_request("surveys")
        _, kwargs = mock_request.call_args
        self.assertEqual(kwargs.get("timeout"), 30)


# ---------------------------------------------------------------------------
# make_request � 429 rate-limit handling
# ---------------------------------------------------------------------------

class TestMakeRequestRateLimit(unittest.TestCase):
    """429 tests patch time.sleep so backoff retries complete instantly."""

    @patch("tap_surveymonkey.client.time.sleep")
    @patch("tap_surveymonkey.client.requests.request")
    def test_429_raises_rate_limit_error(self, mock_request, mock_sleep):
        """make_request eventually raises SurveyMonkeyRateLimitError on persistent 429."""
        mock_request.return_value = _make_resp(429)
        with self.assertRaises(SurveyMonkeyRateLimitError):
            SurveyMonkeyClient("tok").make_request("surveys")

    @patch("tap_surveymonkey.client.time.sleep")
    @patch("tap_surveymonkey.client.requests.request")
    def test_429_exception_carries_response(self, mock_request, mock_sleep):
        """The raised SurveyMonkeyRateLimitError has .response set to the HTTP response."""
        resp = _make_resp(429)
        mock_request.return_value = resp
        with self.assertRaises(SurveyMonkeyRateLimitError) as ctx:
            SurveyMonkeyClient("tok").make_request("surveys")
        self.assertIs(ctx.exception.response, resp)

    @patch("tap_surveymonkey.client.time.sleep")
    @patch("tap_surveymonkey.client.singer.write_state")
    @patch("tap_surveymonkey.client.requests.request")
    def test_429_writes_state_when_provided(self, mock_request, mock_write_state, mock_sleep):
        """make_request calls singer.write_state on each 429 attempt when state is supplied."""
        mock_request.return_value = _make_resp(429)
        state = {"bookmarks": {"surveys": "2024-01-01"}}
        with self.assertRaises(SurveyMonkeyRateLimitError):
            SurveyMonkeyClient("tok").make_request("surveys", state=state)
        # write_state is called once per attempt before raising (backoff retries max_tries=5)
        mock_write_state.assert_called_with(state)
        self.assertGreaterEqual(mock_write_state.call_count, 1)

    @patch("tap_surveymonkey.client.time.sleep")
    @patch("tap_surveymonkey.client.singer.write_state")
    @patch("tap_surveymonkey.client.requests.request")
    def test_429_no_write_state_when_state_is_none(self, mock_request, mock_write_state, mock_sleep):
        """make_request does NOT call singer.write_state when state=None."""
        mock_request.return_value = _make_resp(429)
        with self.assertRaises(SurveyMonkeyRateLimitError):
            SurveyMonkeyClient("tok").make_request("surveys", state=None)
        mock_write_state.assert_not_called()


# ---------------------------------------------------------------------------
# make_request � typed HTTP error mapping
# ---------------------------------------------------------------------------

class TestMakeRequestHttpErrorMapping(unittest.TestCase):
    """Non-2xx, non-429 responses raise the mapped exception class.

    time.sleep is patched because 5xx codes are in BACKOFF_EXCEPTIONS and the
    backoff decorator retries them, causing real sleeps without the patch.
    """

    ERROR_CASES = [
        (400, SurveyMonkeyBadRequestError),
        (401, SurveyMonkeyUnauthorizedError),
        (403, SurveyMonkeyForbiddenError),
        (404, SurveyMonkeyNotFoundError),
        (405, SurveyMonkeyMethodNotAllowedError),
        (413, SurveyMonkeyRequestEntityTooLargeError),
        (422, SurveyMonkeyUnprocessableEntityError),
        (500, SurveyMonkeyInternalServerError),
        (502, SurveyMonkeyBadGatewayError),
        (503, SurveyMonkeyServiceUnavailableError),
        (504, SurveyMonkeyGatewayTimeoutError),
    ]

    @patch("tap_surveymonkey.client.time.sleep")
    @patch("tap_surveymonkey.client.requests.request")
    def test_each_error_code_raises_correct_exception(self, mock_request, mock_sleep):
        for status_code, exc_class in self.ERROR_CASES:
            with self.subTest(status_code=status_code):
                mock_request.return_value = _make_resp(
                    status_code, json_data={"error": "oops"}
                )
                with self.assertRaises(exc_class):
                    SurveyMonkeyClient("tok").make_request("surveys")

    @patch("tap_surveymonkey.client.time.sleep")
    @patch("tap_surveymonkey.client.requests.request")
    def test_404_raises_not_returns_none(self, mock_request, mock_sleep):
        """404 now raises SurveyMonkeyNotFoundError instead of returning None (PR #44)."""
        mock_request.return_value = _make_resp(404, json_data={"error": "not found"})
        with self.assertRaises(SurveyMonkeyNotFoundError):
            SurveyMonkeyClient("tok").make_request("surveys/missing")

    @patch("tap_surveymonkey.client.time.sleep")
    @patch("tap_surveymonkey.client.requests.request")
    def test_unknown_error_code_raises_base_error(self, mock_request, mock_sleep):
        """An unmapped status code raises the base SurveyMonkeyError."""
        mock_request.return_value = _make_resp(418, json_data={"error": "teapot"})
        with self.assertRaises(SurveyMonkeyError):
            SurveyMonkeyClient("tok").make_request("surveys")

    @patch("tap_surveymonkey.client.time.sleep")
    @patch("tap_surveymonkey.client.requests.request")
    def test_error_message_includes_status_code(self, mock_request, mock_sleep):
        """Exception message contains the HTTP status code."""
        mock_request.return_value = _make_resp(401, json_data={"error": "bad token"})
        with self.assertRaises(SurveyMonkeyUnauthorizedError) as ctx:
            SurveyMonkeyClient("tok").make_request("surveys")
        self.assertIn("401", str(ctx.exception))

    @patch("tap_surveymonkey.client.time.sleep")
    @patch("tap_surveymonkey.client.requests.request")
    def test_error_falls_back_to_resp_text_when_json_fails(self, mock_request, mock_sleep):
        """When resp.json() raises, the error body falls back to resp.text."""
        resp = _make_resp(500, text="Internal error plain text")
        resp.json.side_effect = ValueError("No JSON")
        mock_request.return_value = resp
        with self.assertRaises(SurveyMonkeyInternalServerError) as ctx:
            SurveyMonkeyClient("tok").make_request("surveys")
        self.assertIn("Internal error plain text", str(ctx.exception))


# ---------------------------------------------------------------------------
# _get_rate_limit_sleep_seconds
# ---------------------------------------------------------------------------

class TestGetRateLimitSleepSeconds(unittest.TestCase):

    def _resp_with_headers(self, headers):
        resp = MagicMock()
        resp.headers = headers
        return resp

    def test_day_remaining_zero_returns_day_reset_plus_two(self):
        """Day quota exhausted -> sleep = Day-Reset + 2."""
        resp = self._resp_with_headers({
            "X-Ratelimit-App-Global-Day-Remaining": "0",
            "X-Ratelimit-App-Global-Day-Reset": "3600",
        })
        self.assertEqual(_get_rate_limit_sleep_seconds(resp), 3602)

    def test_day_remaining_zero_uses_default_60_when_reset_missing(self):
        """Day quota exhausted but Day-Reset header absent -> 60 + 2 = 62."""
        resp = self._resp_with_headers({
            "X-Ratelimit-App-Global-Day-Remaining": "0",
        })
        self.assertEqual(_get_rate_limit_sleep_seconds(resp), 62)

    def test_minute_remaining_zero_returns_minute_reset_plus_two(self):
        """Minute quota exhausted -> sleep = Minute-Reset + 2."""
        resp = self._resp_with_headers({
            "X-Ratelimit-App-Global-Day-Remaining": "5",
            "X-Ratelimit-App-Global-Minute-Remaining": "0",
            "X-Ratelimit-App-Global-Minute-Reset": "30",
        })
        self.assertEqual(_get_rate_limit_sleep_seconds(resp), 32)

    def test_minute_remaining_zero_uses_default_60_when_reset_missing(self):
        """Minute quota exhausted but Minute-Reset header absent -> 60 + 2 = 62."""
        resp = self._resp_with_headers({
            "X-Ratelimit-App-Global-Day-Remaining": "5",
            "X-Ratelimit-App-Global-Minute-Remaining": "0",
        })
        self.assertEqual(_get_rate_limit_sleep_seconds(resp), 62)

    def test_returns_zero_when_limits_not_exhausted(self):
        """No quota exhausted -> return 0 (caller uses exponential back-off)."""
        resp = self._resp_with_headers({
            "X-Ratelimit-App-Global-Day-Remaining": "100",
            "X-Ratelimit-App-Global-Minute-Remaining": "5",
        })
        self.assertEqual(_get_rate_limit_sleep_seconds(resp), 0)

    def test_returns_zero_when_all_headers_absent(self):
        """No rate-limit headers present -> returns 0."""
        resp = self._resp_with_headers({})
        self.assertEqual(_get_rate_limit_sleep_seconds(resp), 0)

    def test_day_quota_takes_priority_over_minute_quota(self):
        """When both quotas are exhausted, day reset takes priority."""
        resp = self._resp_with_headers({
            "X-Ratelimit-App-Global-Day-Remaining": "0",
            "X-Ratelimit-App-Global-Day-Reset": "3600",
            "X-Ratelimit-App-Global-Minute-Remaining": "0",
            "X-Ratelimit-App-Global-Minute-Reset": "30",
        })
        self.assertEqual(_get_rate_limit_sleep_seconds(resp), 3602)

    def test_returns_zero_on_non_integer_day_remaining(self):
        """Malformed (non-integer) header value is silently ignored -> returns 0."""
        resp = self._resp_with_headers({
            "X-Ratelimit-App-Global-Day-Remaining": "not-a-number",
        })
        self.assertEqual(_get_rate_limit_sleep_seconds(resp), 0)

    def test_missing_header_default_minus_one_does_not_trigger_sleep(self):
        """Missing Remaining header defaults to -1, which is not 0, so no sleep."""
        # Only Minute header present, Day-Remaining missing -> default -1 -> no day sleep
        resp = self._resp_with_headers({
            "X-Ratelimit-App-Global-Minute-Remaining": "10",
        })
        self.assertEqual(_get_rate_limit_sleep_seconds(resp), 0)


# ---------------------------------------------------------------------------
# _on_backoff
# ---------------------------------------------------------------------------

class TestOnBackoff(unittest.TestCase):

    def _details(self, exc, tries=1):
        return {"exception": exc, "tries": tries}

    @patch("tap_surveymonkey.client.time.sleep")
    def test_rate_limit_with_valid_day_headers_sleeps_header_value(self, mock_sleep):
        """_on_backoff sleeps for the header-derived duration on a 429 with day headers."""
        resp = MagicMock()
        resp.headers = {
            "X-Ratelimit-App-Global-Day-Remaining": "0",
            "X-Ratelimit-App-Global-Day-Reset": "10",
        }
        exc = SurveyMonkeyRateLimitError("rate limit", response=resp)
        _on_backoff(self._details(exc, tries=1))
        mock_sleep.assert_called_once_with(12)  # 10 + 2

    @patch("tap_surveymonkey.client.time.sleep")
    def test_rate_limit_with_minute_headers_sleeps_minute_value(self, mock_sleep):
        """_on_backoff sleeps for minute reset + 2 when only minute quota is exhausted."""
        resp = MagicMock()
        resp.headers = {
            "X-Ratelimit-App-Global-Day-Remaining": "50",
            "X-Ratelimit-App-Global-Minute-Remaining": "0",
            "X-Ratelimit-App-Global-Minute-Reset": "30",
        }
        exc = SurveyMonkeyRateLimitError("rate limit", response=resp)
        _on_backoff(self._details(exc, tries=1))
        mock_sleep.assert_called_once_with(32)  # 30 + 2

    @patch("tap_surveymonkey.client.time.sleep")
    def test_rate_limit_missing_headers_falls_back_to_exponential(self, mock_sleep):
        """_on_backoff uses exponential back-off when 429 response has no reset headers."""
        resp = MagicMock()
        resp.headers = {}
        exc = SurveyMonkeyRateLimitError("rate limit", response=resp)
        _on_backoff(self._details(exc, tries=2))
        mock_sleep.assert_called_once_with(4)  # min(2**2, 300)

    @patch("tap_surveymonkey.client.time.sleep")
    def test_rate_limit_none_response_falls_back_to_exponential(self, mock_sleep):
        """_on_backoff uses exponential back-off when the exception carries no response."""
        exc = SurveyMonkeyRateLimitError("rate limit", response=None)
        _on_backoff(self._details(exc, tries=3))
        mock_sleep.assert_called_once_with(8)  # min(2**3, 300)

    @patch("tap_surveymonkey.client.time.sleep")
    def test_server_error_uses_exponential_backoff(self, mock_sleep):
        """_on_backoff uses capped exponential sleep for 5xx exceptions."""
        exc = SurveyMonkeyInternalServerError("500 error")
        _on_backoff(self._details(exc, tries=3))
        mock_sleep.assert_called_once_with(8)  # min(2**3, 300)

    @patch("tap_surveymonkey.client.time.sleep")
    def test_exponential_backoff_is_capped_at_300(self, mock_sleep):
        """_on_backoff caps the exponential sleep at 300 seconds."""
        exc = SurveyMonkeyInternalServerError("500 error")
        _on_backoff(self._details(exc, tries=100))
        mock_sleep.assert_called_once_with(300)


# ---------------------------------------------------------------------------
# make_request � no side-effects on success
# ---------------------------------------------------------------------------

class TestMakeRequestNoSideEffectsOnSuccess(unittest.TestCase):

    @patch("tap_surveymonkey.client.time.sleep")
    @patch("tap_surveymonkey.client.requests.request")
    def test_no_sleep_on_200(self, mock_request, mock_sleep):
        """Successful 200 response does not trigger any sleep."""
        mock_request.return_value = _make_resp(200, {"ok": True})
        SurveyMonkeyClient("tok").make_request("surveys")
        mock_sleep.assert_not_called()

    @patch("tap_surveymonkey.client.singer.write_state")
    @patch("tap_surveymonkey.client.requests.request")
    def test_no_write_state_on_200(self, mock_request, mock_write_state):
        """Successful 200 response does not call singer.write_state."""
        mock_request.return_value = _make_resp(200, {"ok": True})
        SurveyMonkeyClient("tok").make_request("surveys", state={"bookmarks": {}})
        mock_write_state.assert_not_called()
