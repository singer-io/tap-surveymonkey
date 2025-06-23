# Changelog

## 2.1.1
  * Updates requests to 2.32.4
  * Updates singer-python to 6.0.1

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
