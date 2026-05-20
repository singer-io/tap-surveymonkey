# tap-surveymonkey

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/).

This tap:

-   Pulls raw data from [SurveyMonkey](http://www.surveymonkey.com)
-   Extracts the following resources:

    -   Surveys
    -   Responses
    -   Simplified Responses
    -   Survey Details

-   Outputs the schema for each resource
-   Incrementally pulls data based on the input state

To pull all surveys, the configuration parameters `access_token` and `start_date` are required.

- To pull all responses or simplified responses or survey_details for a specific survey, the configuration parameters `access_token`, `start_date`, and `survey_id` are required. 
- To pull all data of responses or simplified responses or survey_details, the configuration parameters `access_token`, `start_date`, and `survey_id` are required. 

The parameter `page_size` _(default: 50, max: 100)_ is optional to adjust the response-size for faster response times or larger batches thereby and reduced number of API-calls.

The [surveys](https://developer.surveymonkey.com/api/v3/#surveys-id-details) and [responses](https://developer.surveymonkey.com/api/v3/#surveys-id-responses-bulk) resources will pull data in the form described on the SurveyMonkey API docs.

The Simplified Responses resource will pull a Response schema, with an extra key `simple_text` embedded in each of the `answer` dictionaries,
which is a human-readable form of the survey respondent's response to question. It also contains the `family`, `subtype`, and `heading` keys in the `question` object, for easy reference.

# Quick Start

1.  Install

    Clone this repo

    ```
    git clone ...
    ```

    We recommend using a virtualenv:

    ```
    python3 -m venv ~/.virtualenvs/tap-surveymonkey
    source ~/.virtualenvs/tap-surveymonkey/bin/activate
    pip install -e .
    ```

2.  Create a SurveyMonkey access token

    Login to your SurveyMonkey account, go to [SurveyMonkey app directory](https://www.surveymonkey.com/apps), and put `stitchdata` in the search box to find the Stitchdata app. In there, you can authorize to get an access token.

3.  Set up your config file.

    An example config file is provided in `sample_config.json`, the access token and survey in that file are invalid, and will error out. Replace them with your own valid ones.

4.  Run the tap in discovery mode to get catalog.json file.

    ```
    tap-surveymonkey --config config.json --discover > catalog.json
    ```

5.  In the generated `catalog.json` file, select the streams to sync.

    Each stream in the `catalog.json` file has a `schema` entry. To select a stream to sync, add **"selected": true** to that stream's `schema` entry. For example, to sync the survey_details stream:

    ```
    "tap_stream_id": "survey_details",
        "schema": {
            "selected": true,
            "properties": {
                ...
            }
        }
    ...
    ```

6.  Run the application

    tap-surveymonkey can be run with:

    ```
    tap-surveymonkey --config config.json --catalog catalog.json
    ```

7.  To run with [Stitch Import API](https://www.stitchdata.com/docs/integrations/import-api/) with dry run:

    ```
    tap-surveymonkey --config config.json --catalog catalog.json | target-stitch --config target_config.json --dry-run > state.json
    ```

## Configuration

| Config property    | Required  | Description
| ------------------ | --------- | --------------
| `access_token`     | Yes       | See https://developer.surveymonkey.com/api/v3/#oauth-2-0-flow
| `start_date`       | Yes        | For streams with replication method `INCREMENTAL` the start date time to be used
| `page_size`        | No, default `"50"` | The page size for paginated streams
| `survey_id`        | No        | In case you just want to get data for just one survey. Does not work with stream `surveys`.

## Creating Test Data

The script `.spike/create_test_data.py` creates a survey with questions, optionally seeds a synthetic response, fetches all four tap streams, and writes the results to a JSON file. It also keeps `tmp/configs/config.json` up to date with the active `survey_id`.

### Prerequisites

1. A SurveyMonkey account with an OAuth2 access token.
2. Config file at `tmp/configs/config.json`:

```json
{
    "access_token": "YOUR_ACCESS_TOKEN",
    "start_date": "2024-01-01T00:00:00Z"
}
```

The script will populate `survey_id` automatically on first run.

### Usage

**Create a new survey and fetch all streams** _(most common)_:

```bash
python .spike/create_test_data.py --config tmp/configs/config.json
```

This will:
- Create a survey with 3 questions (single-choice, open-ended, multiple-choice)
- Attempt to create a collector and seed one synthetic response
- Rename the survey title to `TAP Test Survey (updated)`
- Write the active `survey_id` back to `tmp/configs/config.json`
- Fetch all four streams and save to `tmp/stream_data.json`

**Fetch streams only** (no new survey created):

```bash
python .spike/create_test_data.py --config tmp/configs/config.json --fetch-only
```

**Scope to a specific survey**:

```bash
python .spike/create_test_data.py --config tmp/configs/config.json --fetch-only --survey-id 123456789
```

**Change the output file**:

```bash
python .spike/create_test_data.py --config tmp/configs/config.json --output tmp/outputs/my_streams.json
```

**Clean up after a run** (deletes the survey and collector created):

```bash
python .spike/create_test_data.py --config tmp/configs/config.json --cleanup
```

### Output

`tmp/stream_data.json` contains a JSON object keyed by stream name:

```json
{
  "surveys": [ ... ],
  "survey_details": [ ... ],
  "responses": [ ... ],
  "simplified_responses": [ ... ]
}
```

### Note on responses

Creating a collector (required to seed responses) requires a **verified email** on the SurveyMonkey account. If the account email is unverified, `responses` and `simplified_responses` will be empty. Verify your email at [app.surveymonkey.com](https://app.surveymonkey.com) → Account → Settings, then re-run the script.

### Verifying with the tap

After creating test data, run the tap to confirm it picks up the new survey:

**Windows:**
```powershell
.\venv\Scripts\tap-surveymonkey.exe --config tmp/configs/config.json --catalog tmp/catalogs/catalog.json > tmp/outputs/output.json
```

**macOS / Linux:**
```bash
tap-surveymonkey --config tmp/configs/config.json --catalog tmp/catalogs/catalog.json > tmp/outputs/output.json
```



## Streams

### surveys

-   Endpoint: https://api.surveymonkey.com/v3/surveys
-   Primary keys: id
-   Replication strategy: INCREMENTAL
    -   Bookmark: date_modified (date-time)

### survey_details

-   Endpoint: https://api.surveymonkey.com/v3/surveys/[survey_id]/details
-   Primary keys: id
-   Replication strategy: INCREMENTAL
    -   Bookmark: date_modified (date-time)

### responses

-   Endpoint: https://api.surveymonkey.com/v3/surveys/[survey_id]/responses/bulk
-   Primary keys: id
-   Replication strategy: INCREMENTAL
    -   Bookmark: date_modified (date-time)

### simplified_responses

-   Endpoint: https://api.surveymonkey.com/v3/surveys/[survey_id]/responses/bulk
-   Primary keys: id
-   Replication strategy: INCREMENTAL
    -   Bookmark: date_modified (date-time)

## Developing

While developing the tap, run pylint to improve better code quality which is recommended by [Singer.io best practices](https://github.com/singer-io/getting-started/blob/master/docs/BEST_PRACTICES.md).

```
pylint tap_surveymonkey -d missing-docstring -d logging-format-interpolation -d too-many-locals -d too-many-arguments
```

To check the tap and verify working, install [singer-tools](https://github.com/singer-io/singer-tools).

```
tap-surveymonkey --config tap_config.json --catalog catalog.json | singer-check-tap
```

---

Copyright &copy; 2019 Stitch
