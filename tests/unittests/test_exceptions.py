"""Unit tests for tap_surveymonkey/exceptions.py (introduced in PR #44)."""

import unittest

from requests.exceptions import ChunkedEncodingError, ConnectionError, Timeout

from tap_surveymonkey.exceptions import (
    BACKOFF_EXCEPTIONS,
    ERROR_CODE_EXCEPTION_MAPPING,
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


class TestExceptionHierarchy(unittest.TestCase):
    """All custom exceptions inherit from SurveyMonkeyError (and thus Exception)."""

    def _assert_subclass(self, exc_class):
        self.assertTrue(
            issubclass(exc_class, SurveyMonkeyError),
            "%s is not a subclass of SurveyMonkeyError" % exc_class.__name__,
        )
        self.assertTrue(
            issubclass(exc_class, Exception),
            "%s is not a subclass of Exception" % exc_class.__name__,
        )

    def test_bad_request_inherits_base(self):
        self._assert_subclass(SurveyMonkeyBadRequestError)

    def test_unauthorized_inherits_base(self):
        self._assert_subclass(SurveyMonkeyUnauthorizedError)

    def test_forbidden_inherits_base(self):
        self._assert_subclass(SurveyMonkeyForbiddenError)

    def test_not_found_inherits_base(self):
        self._assert_subclass(SurveyMonkeyNotFoundError)

    def test_method_not_allowed_inherits_base(self):
        self._assert_subclass(SurveyMonkeyMethodNotAllowedError)

    def test_request_entity_too_large_inherits_base(self):
        self._assert_subclass(SurveyMonkeyRequestEntityTooLargeError)

    def test_unprocessable_entity_inherits_base(self):
        self._assert_subclass(SurveyMonkeyUnprocessableEntityError)

    def test_rate_limit_inherits_base(self):
        self._assert_subclass(SurveyMonkeyRateLimitError)

    def test_internal_server_error_inherits_base(self):
        self._assert_subclass(SurveyMonkeyInternalServerError)

    def test_bad_gateway_inherits_base(self):
        self._assert_subclass(SurveyMonkeyBadGatewayError)

    def test_service_unavailable_inherits_base(self):
        self._assert_subclass(SurveyMonkeyServiceUnavailableError)

    def test_gateway_timeout_inherits_base(self):
        self._assert_subclass(SurveyMonkeyGatewayTimeoutError)

    def test_base_is_catchable_as_exception(self):
        """SurveyMonkeyError instances are catchable with both their own class and Exception."""
        exc = SurveyMonkeyError("test")
        self.assertIsInstance(exc, Exception)

    def test_typed_exceptions_catchable_as_base(self):
        """Typed subclass instances are catchable via SurveyMonkeyError."""
        for exc_class in [
            SurveyMonkeyBadRequestError,
            SurveyMonkeyUnauthorizedError,
            SurveyMonkeyInternalServerError,
        ]:
            with self.subTest(exc_class=exc_class.__name__):
                try:
                    raise exc_class("msg")
                except SurveyMonkeyError:
                    pass
                else:
                    self.fail("%s not caught as SurveyMonkeyError" % exc_class.__name__)


class TestRateLimitExceptionResponseAttachment(unittest.TestCase):
    """SurveyMonkeyRateLimitError carries an optional response object."""

    def test_response_stored_when_provided(self):
        mock_resp = object()
        exc = SurveyMonkeyRateLimitError("rate limited", response=mock_resp)
        self.assertIs(exc.response, mock_resp)

    def test_response_defaults_to_none(self):
        exc = SurveyMonkeyRateLimitError("rate limited")
        self.assertIsNone(exc.response)

    def test_message_preserved(self):
        exc = SurveyMonkeyRateLimitError("HTTP 429 — rate limit exceeded.", response=None)
        self.assertEqual(str(exc), "HTTP 429 — rate limit exceeded.")


class TestErrorCodeExceptionMapping(unittest.TestCase):
    """ERROR_CODE_EXCEPTION_MAPPING maps every expected HTTP status to the right class."""

    EXPECTED = {
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

    def test_all_expected_codes_present(self):
        for code in self.EXPECTED:
            with self.subTest(code=code):
                self.assertIn(code, ERROR_CODE_EXCEPTION_MAPPING)

    def test_each_code_maps_to_correct_class(self):
        for code, exc_class in self.EXPECTED.items():
            with self.subTest(code=code):
                self.assertIs(ERROR_CODE_EXCEPTION_MAPPING[code], exc_class)

    def test_mapping_contains_exactly_expected_codes(self):
        self.assertEqual(set(ERROR_CODE_EXCEPTION_MAPPING.keys()), set(self.EXPECTED.keys()))


class TestBackoffExceptions(unittest.TestCase):
    """BACKOFF_EXCEPTIONS contains exactly the right exception types for retrying."""

    EXPECTED_TYPES = {
        ConnectionError,
        Timeout,
        ChunkedEncodingError,
        SurveyMonkeyRateLimitError,
        SurveyMonkeyInternalServerError,
        SurveyMonkeyBadGatewayError,
        SurveyMonkeyServiceUnavailableError,
        SurveyMonkeyGatewayTimeoutError,
    }

    def test_is_tuple(self):
        self.assertIsInstance(BACKOFF_EXCEPTIONS, tuple)

    def test_contains_all_expected_types(self):
        for exc_type in self.EXPECTED_TYPES:
            with self.subTest(exc_type=exc_type.__name__):
                self.assertIn(exc_type, BACKOFF_EXCEPTIONS)

    def test_contains_exactly_expected_types(self):
        self.assertEqual(set(BACKOFF_EXCEPTIONS), self.EXPECTED_TYPES)

    def test_non_retriable_errors_not_included(self):
        """Client errors (4xx except 429) must NOT be in BACKOFF_EXCEPTIONS."""
        non_retriable = [
            SurveyMonkeyBadRequestError,
            SurveyMonkeyUnauthorizedError,
            SurveyMonkeyForbiddenError,
            SurveyMonkeyNotFoundError,
            SurveyMonkeyMethodNotAllowedError,
            SurveyMonkeyRequestEntityTooLargeError,
            SurveyMonkeyUnprocessableEntityError,
        ]
        for exc_type in non_retriable:
            with self.subTest(exc_type=exc_type.__name__):
                self.assertNotIn(exc_type, BACKOFF_EXCEPTIONS)
