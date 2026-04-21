import time

import backoff
import requests
import singer
import urllib3
from requests.exceptions import ChunkedEncodingError, ConnectionError, Timeout

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from tap_surveymonkey.exceptions import (  # noqa: E402
    ERROR_CODE_EXCEPTION_MAPPING,
    SurveyMonkeyBadGatewayError,
    SurveyMonkeyError,
    SurveyMonkeyGatewayTimeoutError,
    SurveyMonkeyInternalServerError,
    SurveyMonkeyRateLimitError,
    SurveyMonkeyServiceUnavailableError,
)

LOGGER = singer.get_logger()

# Exceptions that trigger an automatic backoff-and-retry.
# SurveyMonkeyRateLimitError is intentionally excluded: 429 responses are
# handled with a header-driven sleep+retry loop entirely inside make_request,
# so adding it here would cause a double wait (manual sleep + backoff sleep).
BACKOFF_EXCEPTIONS = (
    ConnectionError,
    Timeout,
    ChunkedEncodingError,
    SurveyMonkeyInternalServerError,
    SurveyMonkeyBadGatewayError,
    SurveyMonkeyServiceUnavailableError,
    SurveyMonkeyGatewayTimeoutError,
)

MAX_RATE_LIMIT_RETRIES = 5


def _get_rate_limit_sleep_seconds(resp):
    """Return the number of seconds to sleep based on rate-limit response headers.

    Prefers the daily reset window over the per-minute window when both limits
    are exhausted.  Returns 0 if no header indicates the limit was reached.
    """
    try:
        day_remaining = int(resp.headers.get("X-Ratelimit-App-Global-Day-Remaining", -1))
        if day_remaining == 0:
            return int(resp.headers.get("X-Ratelimit-App-Global-Day-Reset", 60)) + 2

        minute_remaining = int(resp.headers.get("X-Ratelimit-App-Global-Minute-Remaining", -1))
        if minute_remaining == 0:
            return int(resp.headers.get("X-Ratelimit-App-Global-Minute-Reset", 60)) + 2
    except (ValueError, TypeError):
        pass

    return 0


class SurveyMonkeyClient:
    def __init__(self, access_token):
        self.access_token = access_token

    @backoff.on_exception(
        backoff.expo,
        BACKOFF_EXCEPTIONS,
        max_tries=5,
        factor=2,
        logger=LOGGER,
    )
    def make_request(self, endpoint, state=None, method="GET", **request_kwargs):
        headers = {
            "Authorization": "bearer %s" % self.access_token,
            "Content-Type": "application/json",
        }
        url = "https://api.surveymonkey.com/v3/%s" % endpoint

        # 429 retries are handled entirely here using the API's own rate-limit
        # headers, so SurveyMonkeyRateLimitError is NOT in BACKOFF_EXCEPTIONS.
        # Adding it there would cause a double wait (header sleep + backoff sleep).
        for attempt in range(1, MAX_RATE_LIMIT_RETRIES + 1):
            resp = requests.request(method, url, headers=headers, **request_kwargs)

            if resp.status_code != 429:
                break

            if attempt == MAX_RATE_LIMIT_RETRIES:
                raise SurveyMonkeyRateLimitError(
                    "HTTP 429 — rate limit exceeded after %d retries." % MAX_RATE_LIMIT_RETRIES
                )

            sleep_seconds = _get_rate_limit_sleep_seconds(resp)
            LOGGER.info(
                "Rate limit reached (attempt %d/%d). Sleeping %d seconds before retrying...",
                attempt, MAX_RATE_LIMIT_RETRIES, sleep_seconds,
            )
            if state:
                singer.write_state(state)
            time.sleep(sleep_seconds)

        # 404 — resource missing; callers check for None.
        if resp.status_code == 404:
            return None

        # Any other non-2xx status code — map to the appropriate exception.
        if not (200 <= resp.status_code < 300):
            exc_class = ERROR_CODE_EXCEPTION_MAPPING.get(resp.status_code, SurveyMonkeyError)
            try:
                error_body = resp.json()
            except Exception:
                error_body = resp.text
            raise exc_class("HTTP %d: %s" % (resp.status_code, error_body))

        return resp.json()
