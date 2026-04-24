"""Custom exceptions for the SurveyMonkey tap."""


class SurveyMonkeyError(Exception):
    """Base exception for all SurveyMonkey API errors."""


class SurveyMonkeyBadRequestError(SurveyMonkeyError):
    """HTTP 400 — Bad Request."""


class SurveyMonkeyUnauthorizedError(SurveyMonkeyError):
    """HTTP 401 — Unauthorized (invalid or expired access token)."""


class SurveyMonkeyForbiddenError(SurveyMonkeyError):
    """HTTP 403 — Forbidden (insufficient permissions)."""


class SurveyMonkeyNotFoundError(SurveyMonkeyError):
    """HTTP 404 — Resource not found."""


class SurveyMonkeyMethodNotAllowedError(SurveyMonkeyError):
    """HTTP 405 — HTTP method not allowed on this endpoint."""


class SurveyMonkeyRequestEntityTooLargeError(SurveyMonkeyError):
    """HTTP 413 — Request payload too large."""


class SurveyMonkeyUnprocessableEntityError(SurveyMonkeyError):
    """HTTP 422 — Unprocessable entity (validation error)."""


class SurveyMonkeyRateLimitError(SurveyMonkeyError):
    """HTTP 429 — Rate limit exceeded."""
    def __init__(self, message, response=None):
        super().__init__(message)
        self.response = response  # carry the raw response for header inspection


class SurveyMonkeyInternalServerError(SurveyMonkeyError):
    """HTTP 500 — Internal server error."""


class SurveyMonkeyBadGatewayError(SurveyMonkeyError):
    """HTTP 502 — Bad gateway."""


class SurveyMonkeyServiceUnavailableError(SurveyMonkeyError):
    """HTTP 503 — Service unavailable."""


class SurveyMonkeyGatewayTimeoutError(SurveyMonkeyError):
    """HTTP 504 — Gateway timeout."""


# Maps HTTP status codes to the appropriate exception class.
ERROR_CODE_EXCEPTION_MAPPING = {
    400: SurveyMonkeyBadRequestError,
    401: SurveyMonkeyUnauthorizedError,
    403: SurveyMonkeyForbiddenError,
    404: SurveyMonkeyNotFoundError,
    405: SurveyMonkeyMethodNotAllowedError,
    413: SurveyMonkeyRequestEntityTooLargeError,
    422: SurveyMonkeyUnprocessableEntityError,
    429: SurveyMonkeyRateLimitError,
    500: SurveyMonkeyInternalServerError,
    502: SurveyMonkeyBadGatewayError,
    503: SurveyMonkeyServiceUnavailableError,
    504: SurveyMonkeyGatewayTimeoutError,
}
