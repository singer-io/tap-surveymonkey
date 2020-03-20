import datetime
import pytz
import singer
from tap_surveymonkey.schema import get_schemas, STREAMS
from tap_surveymonkey.data import SurveyMonkey

DATETIME_PARSE = "%Y-%m-%dT%H:%M:%SZ"
DATETIME_FMT = "%04Y-%m-%dT%H:%M:%S.%fZ"
DATETIME_FMT_MAC = "%Y-%m-%dT%H:%M:%S.%fZ"
SM_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"
SM_RESPONSE_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S%z"


LOGGER = singer.get_logger()


def discover():
    '''
    Run discovery mode
    '''
    schemas, schemas_metadata = get_schemas()
    streams = []

    for schema_name, schema in schemas.items():
        schema_meta = schemas_metadata[schema_name]

        # create and add catalog entry
        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': schema,
            'metadata': schema_meta,
            'key_properties': STREAMS[schema_name]['key_properties'],
        }
        streams.append(catalog_entry)

    return {'streams': streams}


def sync(config, state, catalog):
    '''
    Run sync mode
    '''

    selected_stream_ids = get_selected_streams(catalog)

    # Loop over streams in catalog
    for stream in catalog['streams']:
        stream_id = stream['tap_stream_id']

        if stream_id in selected_stream_ids:
            LOGGER.info('Syncing stream: %s', stream_id)

            sync_func = SYNC_FUNCTIONS[stream_id]
            singer.write_schema(stream_id, stream['schema'], ['id'])

            ret_state = sync_func(
                config,
                state
            )
            if ret_state:
                state = ret_state


def strptime(dtime):
    try:
        return datetime.datetime.strptime(dtime, DATETIME_FMT)
    except Exception:
        try:
            return datetime.datetime.strptime(dtime, DATETIME_FMT_MAC)
        except Exception:
            return datetime.datetime.strptime(dtime, DATETIME_PARSE)


def find_max_timestamp(state, stream_id):
    max_time = pytz.utc.localize(datetime.datetime.min)
    for _, last_modified in state['bookmarks'].get(stream_id, {}).items():
        if max_time < pytz.utc.localize(strptime(last_modified)):
            max_time = pytz.utc.localize(strptime(last_modified))
    return max_time


def get_selected_streams(catalog):
    '''
    Gets selected streams.  Checks schema's 'selected' first (legacy)
    and then checks metadata (current), looking for an empty breadcrumb
    and mdata with a 'selected' entry
    '''
    selected_streams = []
    for stream in catalog['streams']:
        stream_metadata = stream['metadata']
        if stream['schema'].get('selected', False):
            selected_streams.append(stream['tap_stream_id'])
        else:
            for entry in stream_metadata:
                # stream metadata will have empty breadcrumb
                if not entry['breadcrumb'] and entry['metadata'].get('selected', None):
                    selected_streams.append(stream['tap_stream_id'])

    return selected_streams


def patch_time_str(obj_dict):
    # The target expects [yyyy-MM-dd'T'HH:mm:ssZ, yyyy-MM-dd'T'HH:mm:ss.[0-9]{1,9}Z] only
    if obj_dict.get('date_modified'):
        time_obj = singer.utils.strptime_to_utc(obj_dict['date_modified'])
        time_str = singer.utils.strftime(time_obj)
        obj_dict['date_modified'] = time_str
    if obj_dict.get('date_created'):
        time_obj = singer.utils.strptime_to_utc(obj_dict['date_created'])
        time_str = singer.utils.strftime(time_obj)
        obj_dict['date_created'] = time_str


def sync_survey_details(config, state):
    stream_id = 'survey_details'
    access_token = config['access_token']
    per_page = int(config.get("page_size", "50"))
    sm_client = SurveyMonkey(access_token)
    params = {
        'per_page': per_page,
        'page': 1,
        'include': 'date_modified'
    }
    surveys = sm_client.make_request('surveys', params=params, state=state)
    while True:
        if not surveys:
            raise Exception("Resource not found")
        if surveys.get('error'):
            raise Exception(surveys)

        for survey in surveys['data']:
            survey_modified = datetime.datetime.strptime(survey['date_modified'], SM_DATE_FORMAT)
            survey_modified = pytz.utc.localize(survey_modified)
            survey_modified_str = singer.utils.strftime(survey_modified)
            if state['bookmarks'].get(stream_id, {}).get(survey['id']) == survey_modified_str:
                continue

            survey_details = sm_client.make_request(
                'surveys/%s/details' % survey['id'], state=state)
            patch_time_str(survey_details)
            singer.write_records(stream_id, [survey_details])

            state['bookmarks'][stream_id] = {} if not state['bookmarks'].get(
                stream_id) else state['bookmarks'][stream_id]
            state['bookmarks'][stream_id][survey_details['id']] = survey_modified_str
            singer.write_state(state)

        if not surveys['links'].get('next'):
            break

        params['page'] += 1
        surveys = sm_client.make_request('surveys', params=params)

    max_time = pytz.utc.localize(datetime.datetime.min)
    for _, last_modified in state['bookmarks'].get(stream_id, {}).items():
        if max_time < pytz.utc.localize(strptime(last_modified)):
            max_time = pytz.utc.localize(strptime(last_modified))

    state['bookmarks'][stream_id] = {} if not state['bookmarks'].get(
        stream_id) else state['bookmarks'][stream_id]
    state['bookmarks'][stream_id]['full_sync'] = singer.utils.strftime(max_time)
    singer.write_state(state)

    return state


def sync_responses(config, state, simplify=False):
    survey_id = config.get('survey_id')
    if not survey_id:
        raise Exception("Survey ID not provided. Syncing Responses requires a Survey ID. ")

    stream_id = 'simplified_responses' if simplify else 'responses'
    access_token = config['access_token']
    per_page = int(config.get("page_size", "50"))  # Max 100
    sm_client = SurveyMonkey(access_token)
    last_modified_at = None

    if state['bookmarks'].get(stream_id, {}).get('page_sync'):
        last_modified_at = state['bookmarks'][stream_id]['page_sync']

    if state['bookmarks'].get(stream_id, {}).get('full_sync'):
        last_modified_at = state['bookmarks'][stream_id]['full_sync']

    params = {
        'page': 1,
        'per_page': per_page
    }

    if last_modified_at:
        params['start_modified_at'] = last_modified_at
    if simplify:
        params['simple'] = True

    responses = sm_client.make_request(
        'surveys/%s/responses/bulk' % survey_id, params=params, state=state)

    while True:
        if not responses:
            raise Exception("Resource not found")
        if responses.get('error'):
            raise Exception(responses)

        for response in responses['data']:
            date_modified = response['date_modified']
            if date_modified[-3:-2] == ":":
                date_modified = date_modified[:-3] + date_modified[-2:]
            response_modified = datetime.datetime.strptime(date_modified, SM_RESPONSE_DATE_FORMAT)
            response_modified_str = singer.utils.strftime(response_modified)
            if state['bookmarks'].get(stream_id, {}).get(response['id']) == response_modified_str:
                continue

            patch_time_str(response)

            singer.write_records(stream_id,
                                 [response]
                                 )

            state['bookmarks'][stream_id] = {} if not state['bookmarks'].get(
                stream_id) else state['bookmarks'][stream_id]
            state['bookmarks'][stream_id][response['id']] = response_modified_str
            singer.write_state(state)

        if not responses['links'].get('next'):
            break

        state['bookmarks'][stream_id]['page_sync'] = singer.utils.strftime(
            find_max_timestamp(state, stream_id))
        params['page'] += 1
        responses = sm_client.make_request(
            'surveys/%s/responses/bulk' % survey_id, params=params, state=state)

    max_time = find_max_timestamp(state, stream_id)

    state['bookmarks'][stream_id] = {} if not state['bookmarks'].get(
        stream_id) else state['bookmarks'][stream_id]
    state['bookmarks'][stream_id]['full_sync'] = singer.utils.strftime(max_time)
    singer.write_state(state)

    return state


def sync_simplified_responses(config, state):
    return sync_responses(config, state, simplify=True)


SYNC_FUNCTIONS = {
    'survey_details': sync_survey_details,
    'responses': sync_responses,
    'simplified_responses': sync_simplified_responses
}
