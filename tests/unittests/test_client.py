import unittest
from unittest.mock import MagicMock, patch

import requests as req_lib

from tap_surveymonkey.client import SurveyMonkeyClient, _get_rate_limit_sleep_seconds
from tap_surveymonkey.exceptions import (
    ERROR_CODE_EXCEPTION_MAPPING,
    SurveyMonkeyBadGatewayError,
    SurveyMonkeyBadRequestError,
    SurveyMonkeyError,
    SurveyMonkeyForbiddenError,
    SurveyMonkeyInternalServerError,
    SurveyMonkeyNotFoundError,
    SurveyMonkeyRateLimitError,
    SurveyMonkeyServiceUnavailableError,
    SurveyMonkeyUnauthorizedError,
    SurveyMonkeyUnprocessableEntityError,
)


def _make_resp(status_code, json_data=None, headers=None):
    """Build a minimal mock HTTP response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.ok = (200 <= status_code < 300)
    resp.json.return_value = json_data or {}
    resp.text = str(json_data or {})
    resp.headers = headers or {}
    return resp


def _rate_limit_headers(day_remaining=100, day_reset=86400,
                        minute_remaining=100, minute_reset=60):
    """Helper to build rate-limit headers dict."""
    return {
        "X-Ratelimit-App-Global-Day-Remaining": str(day_remaining),
        "X-Ratelimit-App-Global-Day-Reset": str(day_reset),
        "X-Ratelimit-App-Global-Minute-Remaining": str(minute_remaining),
        "X-Ratelimit-App-Global-Minute-Reset": str(minute_reset),
    }


# ---------------------------------------------------------------------------
# SurveyMonkeyClient initialisation
# ---------------------------------------------------------------------------

class TestClientInit(unittest.TestCase):

    def test_stores_access_token(self):
        client = SurveyMonkeyClient("my-token")
        self.assertEqual(client.access_token, "my-token")


# ---------------------------------------------------------------------------
# Successful requests
# ---------------------------------------------------------------------------

class TestClientMakeRequestSuccess(unittest.TestCase):

    @patch("tap_surveymonkey.client.requests.request")
    def test_bearer_auth_header_sent(self, mock_request):
        mock_request.return_value = _make_resp(200, {"id": "1"})
        SurveyMonkeyClient("tok123").make_request("surveys")
        _, kwargs = mock_request.call_args
        self.assertEqual(kwargs["headers"]["Authorization"], "bearer tok123")

    @patch("tap_surveymonkey.client.requests.request")
    def test_content_type_header_sent(self, mock_request):
        mock_request.return_value = _make_resp(200, {})
        SurveyMonkeyClient("tok").make_request("surveys")
        _, kwargs = mock_request.call_args
        self.assertEqual(kwargs["headers"]["Content-Type"], "application/json")

    @patch("tap_surveymonkey.client.requests.request")
    def test_correct_url_built(self, mock_request):
        mock_request.return_value = _make_resp(200, {})
        SurveyMonkeyClient("tok").make_request("surveys/123/details")
        args, _ = mock_request.call_args
        self.assertEqual(args[1], "https://api.surveymonkey.com/v3/surveys/123/details")

    @patch("tap_surveymonkey.client.requests.request")
    def test_default_http_method_is_get(self, mock_request):
        mock_request.return_value = _make_resp(200, {})
        SurveyMonkeyClient("tok").make_request("surveys")
        args, _ = mock_request.call_args
        self.assertEqual(args[0], "GET")

    @patch("tap_surveymonkey.client.requests.request")
    def test_returns_parsed_json_on_success(self, mock_request):
        payload = {"id": "42", "title": "Survey"}
        mock_request.return_value = _make_resp(200, payload)
        result = SurveyMonkeyClient("tok").make_request("surveys/42")
        self.assertEqual(result, payload)

    @patch("tap_surveymonkey.client.requests.request")
    def test_raises_not_found_on_404(self, mock_request):
        mock_request.return_value = _make_resp(404)
        with self.assertRaises(SurveyMonkeyNotFoundError):
            SurveyMonkeyClient("tok").make_request("surveys/nonexistent")

    @patch("tap_surveymonkey.client.requests.request")
    def test_post_method_forwarded(self, mock_request):
        mock_request.return_value = _make_resp(200, {})
        SurveyMonkeyClient("tok").make_request("surveys", method="POST")
        args, _ = mock_request.call_args
        self.assertEqual(args[0], "POST")


# ---------------------------------------------------------------------------
# _get_rate_limit_sleep_seconds helper
# ---------------------------------------------------------------------------

class TestGetRateLimitSleepSeconds(unittest.TestCase):

    def _resp(self, headers):
        r = MagicMock()
        r.headers = headers
        return r

    def test_day_limit_returns_day_reset_plus_two(self):
        resp = self._resp(_rate_limit_headers(day_remaining=0, day_reset=10))
        self.assertEqual(_get_rate_limit_sleep_seconds(resp), 12)

    def test_minute_limit_returns_minute_reset_plus_two(self):
        resp = self._resp(_rate_limit_headers(day_remaining=5, minute_remaining=0, minute_reset=30))
        self.assertEqual(_get_rate_limit_sleep_seconds(resp), 32)

    def test_no_limit_returns_zero(self):
        resp = self._resp(_rate_limit_headers(day_remaining=100, minute_remaining=50))
        self.assertEqual(_get_rate_limit_sleep_seconds(resp), 0)

    def test_missing_headers_returns_zero(self):
        """Absent rate-limit headers default to -1 (not 0), so no sleep is triggered."""
        resp = self._resp({})
        self.assertEqual(_get_rate_limit_sleep_seconds(resp), 0)

    def test_malformed_header_returns_zero(self):
        resp = self._resp({"X-Ratelimit-App-Global-Day-Remaining": "bad"})
        self.assertEqual(_get_rate_limit_sleep_seconds(resp), 0)

    def test_day_limit_takes_priority_over_minute_limit(self):
        """When both limits are exhausted, the daily reset time is used."""
        resp = self._resp(_rate_limit_headers(
            day_remaining=0, day_reset=86400,
            minute_remaining=0, minute_reset=30,
        ))
        self.assertEqual(_get_rate_limit_sleep_seconds(resp), 86402)


# ---------------------------------------------------------------------------
# 429 rate-limiting with backoff
# ---------------------------------------------------------------------------

class TestClientRateLimiting(unittest.TestCase):

    @patch("tap_surveymonkey.client.time.sleep")
    @patch("tap_surveymonkey.client.requests.request")
    def test_day_rate_limit_sleeps_day_reset_plus_two(self, mock_request, mock_sleep):
        """429 with day-remaining=0 sleeps (day_reset + 2) seconds before retrying."""
        rate_429 = _make_resp(429, headers=_rate_limit_headers(
            day_remaining=0, day_reset=10,
            minute_remaining=5, minute_reset=60,
        ))
        retry_ok = _make_resp(200, {"data": "ok"})
        mock_request.side_effect = [rate_429, retry_ok]

        result = SurveyMonkeyClient("tok").make_request("surveys")

        mock_sleep.assert_any_call(12)  # 10 + 2
        self.assertEqual(result, {"data": "ok"})

    @patch("tap_surveymonkey.client.time.sleep")
    @patch("tap_surveymonkey.client.requests.request")
    def test_minute_rate_limit_sleeps_minute_reset_plus_two(self, mock_request, mock_sleep):
        """429 with minute-remaining=0 sleeps (minute_reset + 2) seconds before retrying."""
        rate_429 = _make_resp(429, headers=_rate_limit_headers(
            day_remaining=100, day_reset=86400,
            minute_remaining=0, minute_reset=30,
        ))
        retry_ok = _make_resp(200, {"result": "done"})
        mock_request.side_effect = [rate_429, retry_ok]

        result = SurveyMonkeyClient("tok").make_request("surveys")

        mock_sleep.assert_any_call(32)  # 30 + 2
        self.assertEqual(result, {"result": "done"})

    @patch("tap_surveymonkey.client.time.sleep")
    @patch("tap_surveymonkey.client.requests.request")
    def test_rate_limit_writes_state_if_provided(self, mock_request, mock_sleep):
        """429 with day-remaining=0 writes state before sleeping when state is supplied."""
        rate_429 = _make_resp(429, headers=_rate_limit_headers(
            day_remaining=0, day_reset=5,
            minute_remaining=5, minute_reset=60,
        ))
        retry_ok = _make_resp(200, {})
        mock_request.side_effect = [rate_429, retry_ok]

        with patch("tap_surveymonkey.client.singer.write_state") as mock_write_state:
            state = {"bookmarks": {}}
            SurveyMonkeyClient("tok").make_request("surveys", state=state)
            mock_write_state.assert_called_once_with(state)

    @patch("tap_surveymonkey.client.time.sleep")
    @patch("tap_surveymonkey.client.requests.request")
    def test_no_sleep_when_limits_not_exhausted(self, mock_request, mock_sleep):
        """200 response causes no explicit sleep."""
        mock_request.return_value = _make_resp(200, {"ok": True})
        SurveyMonkeyClient("tok").make_request("surveys")
        mock_sleep.assert_not_called()

    @patch("tap_surveymonkey.client.time.sleep")
    @patch("tap_surveymonkey.client.requests.request")
    def test_missing_rate_limit_headers_falls_back_to_exponential(self, mock_request, mock_sleep):
        """429 with no rate-limit headers falls back to capped exponential sleep (2^tries)."""
        rate_429 = _make_resp(429, headers={})  # no rate-limit headers at all
        retry_ok = _make_resp(200, {"ok": True})
        mock_request.side_effect = [rate_429, retry_ok]

        SurveyMonkeyClient("tok").make_request("surveys")

        # On first backoff details["tries"]==1, so exponential wait == 2**1 == 2 seconds
        mock_sleep.assert_any_call(2)

    @patch("tap_surveymonkey.client.time.sleep")
    @patch("tap_surveymonkey.client.requests.request")
    def test_429_raises_rate_limit_error_after_max_tries(self, mock_request, mock_sleep):
        """Persistent 429 exhausting all retries raises SurveyMonkeyRateLimitError.

        The error message must NOT imply a further retry will occur (the caller
        has no retry mechanism left at this point).
        """
        rate_429 = _make_resp(429, headers=_rate_limit_headers(
            day_remaining=100, minute_remaining=0, minute_reset=1,
        ))
        mock_request.return_value = rate_429

        with self.assertRaises(SurveyMonkeyRateLimitError) as ctx:
            SurveyMonkeyClient("tok").make_request("surveys")

        msg = str(ctx.exception)
        self.assertIn("rate limit exceeded", msg.lower())
        self.assertNotIn("Will retry", msg)

    @patch("tap_surveymonkey.client.time.sleep")
    @patch("tap_surveymonkey.client.requests.request")
    def test_no_state_write_when_state_not_provided(self, mock_request, mock_sleep):
        """429 does not attempt to write state when state=None."""
        rate_429 = _make_resp(429, headers=_rate_limit_headers(
            day_remaining=0, day_reset=5,
        ))
        retry_ok = _make_resp(200, {})
        mock_request.side_effect = [rate_429, retry_ok]

        with patch("tap_surveymonkey.client.singer.write_state") as mock_write_state:
            SurveyMonkeyClient("tok").make_request("surveys")  # no state kwarg
            mock_write_state.assert_not_called()


# ---------------------------------------------------------------------------
# 4xx error handling — non-retryable, raises immediately
# ---------------------------------------------------------------------------

class TestClientFourXxErrors(unittest.TestCase):

    @patch("tap_surveymonkey.client.requests.request")
    def test_400_raises_bad_request_error(self, mock_request):
        mock_request.return_value = _make_resp(400, {"error": "bad params"})
        with self.assertRaises(SurveyMonkeyBadRequestError):
            SurveyMonkeyClient("tok").make_request("surveys")

    @patch("tap_surveymonkey.client.requests.request")
    def test_401_raises_unauthorized_error(self, mock_request):
        mock_request.return_value = _make_resp(401, {"error": "unauthorized"})
        with self.assertRaises(SurveyMonkeyUnauthorizedError):
            SurveyMonkeyClient("tok").make_request("surveys")

    @patch("tap_surveymonkey.client.requests.request")
    def test_403_raises_forbidden_error(self, mock_request):
        mock_request.return_value = _make_resp(403, {"error": "forbidden"})
        with self.assertRaises(SurveyMonkeyForbiddenError):
            SurveyMonkeyClient("tok").make_request("surveys")

    @patch("tap_surveymonkey.client.requests.request")
    def test_422_raises_unprocessable_entity_error(self, mock_request):
        mock_request.return_value = _make_resp(422, {"error": "validation failed"})
        with self.assertRaises(SurveyMonkeyUnprocessableEntityError):
            SurveyMonkeyClient("tok").make_request("surveys")

    @patch("tap_surveymonkey.client.requests.request")
    def test_error_message_included_in_exception(self, mock_request):
        """The raised exception message contains the HTTP status code."""
        mock_request.return_value = _make_resp(400, {"error": "bad params"})
        with self.assertRaises(SurveyMonkeyBadRequestError) as ctx:
            SurveyMonkeyClient("tok").make_request("surveys")
        self.assertIn("400", str(ctx.exception))

    @patch("tap_surveymonkey.client.requests.request")
    def test_unknown_4xx_raises_base_survey_monkey_error(self, mock_request):
        """Unmapped status codes raise the base SurveyMonkeyError."""
        mock_request.return_value = _make_resp(418)  # I'm a teapot
        with self.assertRaises(SurveyMonkeyError):
            SurveyMonkeyClient("tok").make_request("surveys")

    @patch("tap_surveymonkey.client.requests.request")
    def test_json_decode_error_on_error_body_uses_text(self, mock_request):
        """A non-JSON error body falls back to response.text in the exception."""
        bad_resp = _make_resp(400)
        bad_resp.json.side_effect = ValueError("no JSON")
        bad_resp.text = "plain text error"
        mock_request.return_value = bad_resp
        with self.assertRaises(SurveyMonkeyBadRequestError) as ctx:
            SurveyMonkeyClient("tok").make_request("surveys")
        self.assertIn("plain text error", str(ctx.exception))


# ---------------------------------------------------------------------------
# 5xx error handling — retryable via backoff
# ---------------------------------------------------------------------------

class TestClientFiveXxErrors(unittest.TestCase):

    @patch("tap_surveymonkey.client.time.sleep")
    @patch("tap_surveymonkey.client.requests.request")
    def test_500_retries_and_succeeds(self, mock_request, mock_sleep):
        """Transient 500 is retried; subsequent 200 returns parsed JSON."""
        mock_request.side_effect = [
            _make_resp(500, {"error": "oops"}),
            _make_resp(200, {"id": "1"}),
        ]
        result = SurveyMonkeyClient("tok").make_request("surveys")
        self.assertEqual(result, {"id": "1"})

    @patch("tap_surveymonkey.client.time.sleep")
    @patch("tap_surveymonkey.client.requests.request")
    def test_502_retries_and_succeeds(self, mock_request, mock_sleep):
        mock_request.side_effect = [
            _make_resp(502),
            _make_resp(200, {"id": "2"}),
        ]
        result = SurveyMonkeyClient("tok").make_request("surveys")
        self.assertEqual(result, {"id": "2"})

    @patch("tap_surveymonkey.client.time.sleep")
    @patch("tap_surveymonkey.client.requests.request")
    def test_503_retries_and_succeeds(self, mock_request, mock_sleep):
        mock_request.side_effect = [
            _make_resp(503),
            _make_resp(200, {"id": "3"}),
        ]
        result = SurveyMonkeyClient("tok").make_request("surveys")
        self.assertEqual(result, {"id": "3"})

    @patch("tap_surveymonkey.client.time.sleep")
    @patch("tap_surveymonkey.client.requests.request")
    def test_persistent_500_raises_after_max_tries(self, mock_request, mock_sleep):
        """5xx responses that persist beyond max_tries raise the mapped exception."""
        mock_request.return_value = _make_resp(500, {"error": "server error"})
        with self.assertRaises(SurveyMonkeyInternalServerError):
            SurveyMonkeyClient("tok").make_request("surveys")

    @patch("tap_surveymonkey.client.time.sleep")
    @patch("tap_surveymonkey.client.requests.request")
    def test_persistent_502_raises_after_max_tries(self, mock_request, mock_sleep):
        mock_request.return_value = _make_resp(502)
        with self.assertRaises(SurveyMonkeyBadGatewayError):
            SurveyMonkeyClient("tok").make_request("surveys")

    @patch("tap_surveymonkey.client.time.sleep")
    @patch("tap_surveymonkey.client.requests.request")
    def test_persistent_503_raises_after_max_tries(self, mock_request, mock_sleep):
        mock_request.return_value = _make_resp(503)
        with self.assertRaises(SurveyMonkeyServiceUnavailableError):
            SurveyMonkeyClient("tok").make_request("surveys")


# ---------------------------------------------------------------------------
# Network-level error handling — retryable via backoff
# ---------------------------------------------------------------------------

class TestClientNetworkErrors(unittest.TestCase):

    @patch("tap_surveymonkey.client.time.sleep")
    @patch("tap_surveymonkey.client.requests.request")
    def test_connection_error_retries_and_succeeds(self, mock_request, mock_sleep):
        """ConnectionError is retried; subsequent 200 returns parsed JSON."""
        mock_request.side_effect = [
            req_lib.exceptions.ConnectionError("connection reset"),
            _make_resp(200, {"id": "1"}),
        ]
        result = SurveyMonkeyClient("tok").make_request("surveys")
        self.assertEqual(result, {"id": "1"})

    @patch("tap_surveymonkey.client.time.sleep")
    @patch("tap_surveymonkey.client.requests.request")
    def test_timeout_retries_and_succeeds(self, mock_request, mock_sleep):
        mock_request.side_effect = [
            req_lib.exceptions.Timeout("timed out"),
            _make_resp(200, {"id": "2"}),
        ]
        result = SurveyMonkeyClient("tok").make_request("surveys")
        self.assertEqual(result, {"id": "2"})

    @patch("tap_surveymonkey.client.time.sleep")
    @patch("tap_surveymonkey.client.requests.request")
    def test_chunked_encoding_error_retries_and_succeeds(self, mock_request, mock_sleep):
        mock_request.side_effect = [
            req_lib.exceptions.ChunkedEncodingError("chunked error"),
            _make_resp(200, {"id": "3"}),
        ]
        result = SurveyMonkeyClient("tok").make_request("surveys")
        self.assertEqual(result, {"id": "3"})

    @patch("tap_surveymonkey.client.time.sleep")
    @patch("tap_surveymonkey.client.requests.request")
    def test_persistent_connection_error_raises_after_max_tries(self, mock_request, mock_sleep):
        mock_request.side_effect = req_lib.exceptions.ConnectionError("no connection")
        with self.assertRaises(req_lib.exceptions.ConnectionError):
            SurveyMonkeyClient("tok").make_request("surveys")


# ---------------------------------------------------------------------------
# ERROR_CODE_EXCEPTION_MAPPING coverage
# ---------------------------------------------------------------------------

class TestErrorCodeExceptionMapping(unittest.TestCase):

    def test_all_expected_codes_are_mapped(self):
        expected_codes = {400, 401, 403, 404, 405, 413, 422, 429, 500, 502, 503, 504}
        self.assertEqual(set(ERROR_CODE_EXCEPTION_MAPPING.keys()), expected_codes)

    def test_all_mapped_exceptions_are_survey_monkey_errors(self):
        for code, exc_class in ERROR_CODE_EXCEPTION_MAPPING.items():
            with self.subTest(code=code):
                self.assertTrue(
                    issubclass(exc_class, SurveyMonkeyError),
                    f"{exc_class} is not a subclass of SurveyMonkeyError",
                )
