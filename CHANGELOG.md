# Changelog

## 2.2.3
  * Bump `requests` to 2.34.2 to address CVE security advisories [#48](https://github.com/singer-io/tap-surveymonkey/pull/48)
  * Bump `urllib3` to 2.7.0 (dev dependency) to fix two HIGH severity CVEs: cross-origin redirect sensitive header leak and compressed response resource exhaustion [#48](https://github.com/singer-io/tap-surveymonkey/pull/48)

## 2.2.2
  * Fix TypeError in parent_row path substitution [#43](https://github.com/singer-io/tap-surveymonkey/pull/43)

## 2.2.1
  * Add `backoff` as an explicit install dependency [#44](https://github.com/singer-io/tap-surveymonkey/pull/44)
  * Refactor error handling: introduce typed exceptions (`exceptions.py`) and `ERROR_CODE_EXCEPTION_MAPPING` for all HTTP error codes [#44](https://github.com/singer-io/tap-surveymonkey/pull/44)
  * Replace inline 429 retry logic with `backoff`-driven retry; add header-aware sleep (`Day-Reset` / `Minute-Reset`) with exponential fallback [#44](https://github.com/singer-io/tap-surveymonkey/pull/44)

## 2.2.0
  * Upgrade Python version to 3.12
  * Unit tests and Integration tests

## 2.1.1
  * Updates requests to 2.32.4 [#35](https://github.com/singer-io/tap-surveymonkey/pull/35)
  * Updates singer-python to 6.0.1 [#35](https://github.com/singer-io/tap-surveymonkey/pull/35)

## 2.1.0
  * Updates to run on python 3.11 [#30](https://github.com/singer-io/tap-surveymonkey/pull/30)

## 2.0.1
  * Get `heading` field in response for `simplified_responses` stream [#29](https://github.com/singer-io/tap-surveymonkey/pull/29)

## 2.0.0
  * Add new stream `surveys` to fetch all the surveys data [#24](https://github.com/singer-io/tap-surveymonkey/pull/24)
  * Add support to extract survey_details, responses and simplified_responses data with/without survey_id
  * Schema updates
  * Upgrade singer-python version to 5.13.0

## 1.0.2
  * Dependabot update [#23](https://github.com/singer-io/tap-surveymonkey/pull/23)

## 1.0.1
  * Add layout to Question object schema
  * Set `additionalProperties` to true on all schemas

## 1.0.0
  * Releasing GA

## 0.1.6
  * Adds to survey_details schema [#12](https://github.com/singer-io/tap-surveymonkey/pull/12)

## 0.1.5
  * Add additional fields to the responses and simplified_responses schemas [#10](https://github.com/singer-io/tap-surveymonkey/pull/10)
  * Adds an optional config parameter `page_size` [#9](https://github.com/singer-io/tap-surveymonkey/pull/9)
