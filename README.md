# tap-surveymonkey

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/).

This tap:

- Pulls raw data from [SurveyMonkey](http://www.surveymonkey.com)
- Extracts the following resources:
  - Surveys
  - Responses
  - Simplified Responses

- Outputs the schema for each resource
- Incrementally pulls data based on the input state

To pull all surveys, the configuration parameters `access_token` and `start_date` are required.

To pull responses or simplified responses for a survey, the configuration parameters `access_token`, `start_date`, and `survey_id` are required.

The [surveys](https://developer.surveymonkey.com/api/v3/#surveys-id-details) and [responses](https://developer.surveymonkey.com/api/v3/#surveys-id-responses-bulk) resources will pull data in the form described on the SurveyMonkey API docs.

The Simplified Responses resource will pull a Response schema, with an extra key `simple_text` embedded in each of the `answer` dictionaries, 
which is a human-readable form of the survey respondent's response to question.  It also contains the `faimly`, `subtype`, and `heading` keys in the `question` object, for easy reference.

# To run this tap

1. Install

    Clone this repo

		git clone ...

    We recommend using a virtualenv:

	    virtualenv -p python3 venv
    	source venv/bin/activate
	    pip3 install -e .

2. Create a SurveyMonkey access token

    Login to your SurveyMonkey account, go to developer.surveymonkey.com/apps, and create a new app (`private` if you aren't sure what to pick), with the scopes you require (for complete functionality of this tap, you'll need `View Surveys`, `View Responses`, `View Response Details`)

    You can then get an access token from the `Settings` page of your newly created app

3. Set up your config file.

    An example config file is provided in `sample_config.json`, the access token and survey in that file are invalid, and will error out.  Replace them
    with your own valid ones.

    To find `survey_id` is required only for the `responses` and `simplified_responses` streams, it can be aquired either by running
    the tap with the `survey_details` stream, or by using the `/v3/surveys` endpoint on the SurveyMonkey API.

4. Run the tap in discovery mode to get properties.json file

		tap-surveymonkey --config config.json --discover > properties.json

5. In the properties.json file, select the streams to sync

    Each stream in the properties.json file has a "schema" entry. To select a stream to sync, add "selected": true to that stream's "schema" entry. For example, to sync the pull_requests stream:

        "tap_stream_id": "survey_details",
	        "schema": {
    		    "selected": true,
		        "properties": {
       			 	...
                 }
        	...

6. Run the application

    tap-surveymonkey can be run with:

		tap-surveymonkey --config config.json --properties properties.json

---