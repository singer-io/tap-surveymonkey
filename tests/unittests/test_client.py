import unittest
from unittest.mock import patch, MagicMock

from tap_surveymonkey.client import SurveyMonkeyClient


def _make_resp(status_code, json_data=None, headers=None):
    """Build a minimal mock HTTP response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data or {}
    resp.headers = headers or {}
    return resp


class TestClientInit(unittest.TestCase):

    def test_stores_access_token(self):
        """Client stores the access_token passed at construction."""
        client = SurveyMonkeyClient("my-token")
        self.assertEqual(client.access_token, "my-token")


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
    def test_returns_parsed_json_on_success(self, mock_request):
        """make_request returns the parsed JSON body on a 200 response."""
        payload = {"id": "42", "title": "Survey"}
        mock_request.return_value = _make_resp(200, payload)
        result = SurveyMonkeyClient("tok").make_request("surveys/42")
        self.assertEqual(result, payload)

    @patch("tap_surveymonkey.client.requests.request")
    def test_returns_none_on_404(self, mock_request):
        """make_request returns None when the API responds with 404."""
        resp = MagicMock()
        resp.status_code = 404
        mock_request.return_value = resp
        result = SurveyMonkeyClient("tok").make_request("surveys/nonexistent")
        self.assertIsNone(result)

    @patch("tap_surveymonkey.client.requests.request")
    def test_post_method_forwarded(self, mock_request):
        """make_request uses the HTTP method passed as an argument."""
        mock_request.return_value = _make_resp(200, {})
        SurveyMonkeyClient("tok").make_request("surveys", method="POST")
        args, _ = mock_request.call_args
        self.assertEqual(args[0], "POST")


class TestClientRateLimiting(unittest.TestCase):

    @patch("tap_surveymonkey.client.time.sleep")
    @patch("tap_surveymonkey.client.requests.request")
    def test_day_rate_limit_sleeps_day_reset_plus_two(self, mock_request, mock_sleep):
        """429 with day-remaining=0 sleeps for (day_reset + 2) seconds."""
        rate_429 = _make_resp(429, headers={
            "X-Ratelimit-App-Global-Day-Remaining": "0",
            "X-Ratelimit-App-Global-Day-Reset": "10",
            "X-Ratelimit-App-Global-Minute-Remaining": "5",
            "X-Ratelimit-App-Global-Minute-Reset": "60",
        })
        retry_ok = _make_resp(200, {"data": "ok"}, headers={
            "X-Ratelimit-App-Global-Minute-Remaining": "5",
            "X-Ratelimit-App-Global-Minute-Reset": "60",
        })
        mock_request.side_effect = [rate_429, retry_ok]

        result = SurveyMonkeyClient("tok").make_request("surveys")

        mock_sleep.assert_called_with(12)  # 10 + 2
        self.assertEqual(result, {"data": "ok"})

    @patch("tap_surveymonkey.client.time.sleep")
    @patch("tap_surveymonkey.client.requests.request")
    def test_minute_rate_limit_sleeps_minute_reset_plus_two(self, mock_request, mock_sleep):
        """429 with minute-remaining=0 sleeps for (minute_reset + 2) seconds."""
        rate_429 = _make_resp(429, headers={
            "X-Ratelimit-App-Global-Day-Remaining": "100",
            "X-Ratelimit-App-Global-Day-Reset": "86400",
            "X-Ratelimit-App-Global-Minute-Remaining": "0",
            "X-Ratelimit-App-Global-Minute-Reset": "30",
        })
        retry_ok = _make_resp(200, {"result": "done"})
        mock_request.side_effect = [rate_429, retry_ok]

        result = SurveyMonkeyClient("tok").make_request("surveys")

        mock_sleep.assert_called_with(32)  # 30 + 2
        self.assertEqual(result, {"result": "done"})

    @patch("tap_surveymonkey.client.time.sleep")
    @patch("tap_surveymonkey.client.requests.request")
    def test_rate_limit_writes_state_if_provided(self, mock_request, mock_sleep):
        """429 with day-remaining=0 writes state before sleeping when state is supplied."""
        rate_429 = _make_resp(429, headers={
            "X-Ratelimit-App-Global-Day-Remaining": "0",
            "X-Ratelimit-App-Global-Day-Reset": "5",
            "X-Ratelimit-App-Global-Minute-Remaining": "5",
            "X-Ratelimit-App-Global-Minute-Reset": "60",
        })
        retry_ok = _make_resp(200, {}, headers={
            "X-Ratelimit-App-Global-Minute-Remaining": "5",
            "X-Ratelimit-App-Global-Minute-Reset": "60",
        })
        mock_request.side_effect = [rate_429, retry_ok]

        with patch("tap_surveymonkey.client.singer.write_state") as mock_write_state:
            state = {"bookmarks": {}}
            SurveyMonkeyClient("tok").make_request("surveys", state=state)
            mock_write_state.assert_called_once_with(state)

    @patch("tap_surveymonkey.client.time.sleep")
    @patch("tap_surveymonkey.client.requests.request")
    def test_no_sleep_when_limits_not_exhausted(self, mock_request, mock_sleep):
        """200 response causes no sleep at all."""
        mock_request.return_value = _make_resp(200, {"ok": True})
        SurveyMonkeyClient("tok").make_request("surveys")
        mock_sleep.assert_not_called()
