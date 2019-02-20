#!/usr/bin/env python3
import os
import json
import singer
import requests

import time
import datetime
import pytz

from singer import utils

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SM_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"
SM_RESPONSE_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S%z"
REQUIRED_CONFIG_KEYS = ["start_date", "access_token"]
LOGGER = singer.get_logger()


DATETIME_PARSE = "%Y-%m-%dT%H:%M:%SZ"
DATETIME_FMT = "%04Y-%m-%dT%H:%M:%S.%fZ"
DATETIME_FMT_MAC = "%Y-%m-%dT%H:%M:%S.%fZ"
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
    for response_id, last_modified in state['bookmarks'].get(stream_id, {}).items():
        if max_time < pytz.utc.localize(strptime(last_modified)):
            max_time = pytz.utc.localize(strptime(last_modified))
    return max_time


class SurveyMonkey(object):
    def __init__(self, access_token):
        self.access_token = access_token
        
    def make_request(self, endpoint, method='GET', state={}, **request_kwargs):
        headers = {
            'Authorization': 'bearer %s' % self.access_token,
            'Content-Type': 'application/json'
        }
        url = 'https://api.surveymonkey.com/v3/%s' % endpoint
        resp = requests.request(method, url, headers=headers, **request_kwargs)

        # check rate limit
        day_remaining = int(resp.headers['X-Ratelimit-App-Global-Day-Remaining'])
        if day_remaining == 0:
            day_reset = int(resp.headers['X-Ratelimit-App-Global-Day-Reset'])
            day_reset += 2
            LOGGER.info('Sleeping for %s seconds due to SurveyMonkey API rate limit... printing state' % day_reset)
            singer.write_state(state)
            time.sleep(day_reset)
            resp = requests.request(method, url, headers=headers, **request_kwargs)

        minute_remaining = int(resp.headers['X-Ratelimit-App-Global-Minute-Remaining'])
        if minute_remaining == 0:
            minute_reset = int(resp.headers['X-Ratelimit-App-Global-Minute-Reset'])
            minute_reset += 2
            LOGGER.info('Sleeping for %s seconds due to SurveyMonkey API rate limit... printing state' % minute_reset)
            singer.write_state(state)
            time.sleep(minute_reset)
            resp = requests.request(method, url, headers=headers, **request_kwargs)

        return resp.json()

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

# Load schemas from schemas folder
def load_schemas():
    schemas = {}

    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = json.load(file)

    return schemas

def discover():
    raw_schemas = load_schemas()
    streams = []

    for schema_name, schema in raw_schemas.items():

        # TODO: populate any metadata and stream's key properties here..
        stream_metadata = []
        stream_key_properties = []

        # create and add catalog entry
        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': schema,
            'metadata' : [],
            'key_properties': []
        }
        streams.append(catalog_entry)

    return {'streams': streams}

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
                if not entry['breadcrumb'] and entry['metadata'].get('selected',None):
                    selected_streams.append(stream['tap_stream_id'])

    return selected_streams

def sync(config, state, catalog):

    selected_stream_ids = get_selected_streams(catalog)
    
    # Loop over streams in catalog
    for stream in catalog['streams']:
        stream_id = stream['tap_stream_id']
        stream_schema = stream['schema']
        mdata = stream.get('metadata')
        if stream_id in selected_stream_ids:
            sync_func = SYNC_FUNCTIONS[stream_id]
            
            singer.write_schema(stream_id, stream['schema'], ['id'])

            state = sync_func(
                stream_schema,
                config,
                state,
                mdata)
            LOGGER.info('Syncing stream:' + stream_id)
    singer.write_state(state)
    return

def sync_survey_details(schema, config, state, mdata):
    stream_id = 'survey_details'
    access_token = config['access_token']
    sm_client = SurveyMonkey(access_token)
    params = {
        'per_page': 50,
        'page': 1,
        'include': 'date_modified'
    }
    surveys = sm_client.make_request('surveys', params=params, state=state)
    while True:
        for survey in surveys['data']:
            survey_modified = datetime.datetime.strptime(survey['date_modified'], SM_DATE_FORMAT)
            survey_modified = pytz.utc.localize(survey_modified)
            survey_modified_str = singer.utils.strftime(survey_modified)
            if state['bookmarks'].get(stream_id, {}).get(survey['id']) == survey_modified_str:
                continue

            survey_details = sm_client.make_request('surveys/%s/details' % survey['id'], state=state)
            singer.write_records(stream_id,
                [survey_details]
            )
            
            state['bookmarks'][stream_id] = {} if not state['bookmarks'].get(stream_id) else state['bookmarks'][stream_id]
            state['bookmarks'][stream_id][survey_details['id']] = survey_modified_str
            singer.write_state(state)

        if not surveys['links'].get('next'):
            break
        
        params['page'] += 1
        surveys = sm_client.make_request('surveys', params=params)

    max_time = pytz.utc.localize(datetime.datetime.min)
    for survey_id, last_modified in state['bookmarks'].get(stream_id, {}).items():
        if max_time < pytz.utc.localize(strptime(last_modified)):
            max_time = pytz.utc.localize(strptime(last_modified))
    

    state['bookmarks'][stream_id] = {} if not state['bookmarks'].get(stream_id) else state['bookmarks'][stream_id]
    state['bookmarks'][stream_id]['full_sync'] = singer.utils.strftime(max_time)

    return state

def sync_responses(schema, config, state, mdata, simplify=False):
    survey_id = config['survey_id']
    stream_id = 'simplified_responses' if simplify else 'responses'
    access_token = config['access_token']
    sm_client = SurveyMonkey(access_token)
    last_modified_at = None

    if state['bookmarks'].get(stream_id, {}).get('page_sync'):
        last_modified_at = state['bookmarks'][stream_id]['page_sync']

    if state['bookmarks'].get(stream_id, {}).get('full_sync'):
        last_modified_at = state['bookmarks'][stream_id]['full_sync']

    params = {
        'page': 1
    }

    if last_modified_at:
        params['start_modified_at'] = last_modified_at
    if simplify:
        params['simple'] = True
        responses = sm_client.make_request('surveys/%s/responses/bulk' % survey_id, params=params, state=state)
    else:
        responses = sm_client.make_request('surveys/%s/responses/bulk' % survey_id, params=params, state=state)
    while True:
        for response in responses['data']:
            date_modified = response['date_modified']
            if ":" == date_modified[-3:-2]:
                date_modified = date_modified[:-3]+date_modified[-2:]
            response_modified = datetime.datetime.strptime(date_modified, SM_RESPONSE_DATE_FORMAT)
            response_modified_str = singer.utils.strftime(response_modified)
            if state['bookmarks'].get(stream_id, {}).get(response['id']) == response_modified_str:
                continue

            singer.write_records(stream_id,
                [response]
            )

            state['bookmarks'][stream_id] = {} if not state['bookmarks'].get(stream_id) else state['bookmarks'][stream_id]
            state['bookmarks'][stream_id][response['id']] = response_modified_str
            singer.write_state(state)

        if not responses['links'].get('next'):
            break

        state['bookmarks'][stream_id]['page_sync'] = singer.utils.strftime(find_max_timestamp(state, stream_id))
        params['page'] += 1
        responses = sm_client.make_request('surveys/%s/responses/bulk' % survey_id, params=params, state=state)
    
    max_time = find_max_timestamp(state, stream_id)

    state['bookmarks'][stream_id] = {} if not state['bookmarks'].get(stream_id) else state['bookmarks'][stream_id]
    state['bookmarks'][stream_id]['full_sync'] = singer.utils.strftime(max_time)

    return state


def sync_simplified_responses(schema, config, state, mdata, **kwargs): 
    return sync_responses(schema, config, state, mdata, simplify=True)


SYNC_FUNCTIONS = {
    'survey_details': sync_survey_details,
    'responses': sync_responses,
    'simplified_responses': sync_simplified_responses
}
@utils.handle_top_exception(LOGGER)
def main():

    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        print(json.dumps(catalog, indent=2))
    # Otherwise run in sync mode
    else:

        # 'properties' is the legacy name of the catalog
        if args.properties:
            catalog = args.properties
        # 'catalog' is the current name
        elif args.catalog:
            catalog = args.catalog
        else:
            catalog =  discover()
        
        state = args.state or {
            'bookmarks' : {}
        }

        sync(args.config, state, catalog)

if __name__ == "__main__":
    main()
