import time

import backoff
import requests
import singer
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from tap_surveymonkey.exceptions import (
    BACKOFF_EXCEPTIONS,
    ERROR_CODE_EXCEPTION_MAPPING,
    SurveyMonkeyError,
    SurveyMonkeyRateLimitError,
)

LOGGER = singer.get_logger()


def _get_rate_limit_sleep_seconds(resp):
    """Return how many *seconds* to sleep after receiving a 429 response.

    SurveyMonkey enforces two independent rate-limit quotas, both reported as
    HTTP response headers on every request:

    **1. Daily quota** — total requests allowed per UTC day.

        X-Ratelimit-App-Global-Day-Remaining: 0      ← 0 means exhausted
        X-Ratelimit-App-Global-Day-Reset:     3600   ← seconds until refill

        "Day-Reset: 3600" does NOT mean sleep for 1 day.
        It means the quota refills in 3600 seconds (~1 hour from now).
        We sleep for 3602 s (+ 2 s safety buffer).

    **2. Per-minute quota** — burst limit reset every 60 seconds.

        X-Ratelimit-App-Global-Minute-Remaining: 0
        X-Ratelimit-App-Global-Minute-Reset:     30   ← seconds until refill

        We sleep for 32 s (30 + 2 s buffer).

    **Decision logic:**
        - If the daily quota is exhausted  → sleep for Day-Reset  + 2 s.
        - Else if minute quota exhausted   → sleep for Minute-Reset + 2 s.
        - If headers are missing/malformed → return 0 (caller uses exponential
          back-off instead).

    The default of ``-1`` for the *Remaining* headers ensures we never treat a
    missing header as exhausted (only ``0`` triggers a sleep).
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


def _on_backoff(details):
    """
    Unified wait strategy (called by backoff before each retry):
    - 429 SurveyMonkeyRateLimitError  → header-driven sleep (day or minute reset)
    - everything else                 → capped exponential back-off
    """
    exc = details.get("exception")
    if isinstance(exc, SurveyMonkeyRateLimitError) and exc.response is not None:
        sleep = _get_rate_limit_sleep_seconds(exc.response)
        if sleep > 0:
            LOGGER.info(
                "Rate limit reached (attempt %d). Sleeping %d seconds...",
                details["tries"], sleep,
            )
            time.sleep(sleep)
            return
        # headers absent / malformed → fall through to exponential
        LOGGER.warning(
            "Rate limit reached (attempt %d) but no reset headers found. "
            "Falling back to exponential wait.", details["tries"],
        )
    time.sleep(min(2 ** details["tries"], 300))  # capped exponential for 5xx / network


class SurveyMonkeyClient:
    def __init__(self, access_token):
        self.access_token = access_token

    def validate_token(self):
        """Validate the access token by making a request to the 'users/me' endpoint."""
        self.make_request("users/me")

    def __enter__(self):
        self.validate_token()
        return self
    
    def __exit__(self, exc_type, exc, tb):
        return False

    @backoff.on_exception(
        backoff.constant,
        BACKOFF_EXCEPTIONS,
        max_tries=5,
        interval=0,          # actual sleeping is handled by _on_backoff
        on_backoff=_on_backoff,
        logger=LOGGER,
    )
    def make_request(self, endpoint, state=None, method="GET", **request_kwargs):
        headers = {
            "Authorization": "bearer %s" % self.access_token,
            "Content-Type": "application/json",
        }
        url = "https://api.surveymonkey.com/v3/%s" % endpoint
        resp = requests.request(method, url, headers=headers, timeout=30, **request_kwargs)

        if resp.status_code == 429:
            if state:
                singer.write_state(state)
            raise SurveyMonkeyRateLimitError(
                "HTTP 429 — rate limit exceeded.", response=resp
            )

        if not (200 <= resp.status_code < 300):
            exc_class = ERROR_CODE_EXCEPTION_MAPPING.get(resp.status_code, SurveyMonkeyError)
            try:
                error_body = resp.json()
            except Exception:
                error_body = resp.text
            raise exc_class("HTTP %d: %s" % (resp.status_code, error_body))

        return resp.json()
