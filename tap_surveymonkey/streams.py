import datetime
import pytz
import singer.utils
from singer import bookmarks, metadata

from tap_surveymonkey.client import SurveyMonkeyClient


LOGGER = singer.get_logger()


DATETIME_PARSE = "%Y-%m-%dT%H:%M:%SZ"
DATETIME_FMT = "%04Y-%m-%dT%H:%M:%S.%fZ"
DATETIME_FMT_MAC = "%Y-%m-%dT%H:%M:%S.%fZ"
SM_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"
SM_RESPONSE_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S%z"

DEFAULT_PAGE_SIZE = "50"


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


class Stream:
    key_properties = None
    replication_method = None
    replication_key = None
    replication_key_from_parent = False # for streams which just return a single record and iterate by its parent, e.g. 'SurveyDetails'
    is_sorted = False # indicate whether data is sorted ascending on bookmark value
    mandatory_properties = []

    def __init__(self, stream_id: str, path: str, parent_stream = None):
        self.stream_id = stream_id
        self.path = path
        self._params = {}
        self.parent_stream = parent_stream

    def format_response(self, response):
        return response

    def _modify_record(self, raw_record):
        pass

    def fetch_data(self, client: SurveyMonkeyClient, stream, config, state, parent_row=None, bookmark_value=None):
        path = self.path
        if parent_row:
            for key, value in parent_row.items():
                path = path.replace(f'{{parent_{key}}}', value)

        resp = client.make_request(path, params=None, state=state)
        if not resp:
            raise Exception("Resource not found")
        if resp.get('error'):
            raise Exception(resp)

        self._modify_record(resp)
        yield resp


class PaginatedStream(Stream):
    def get_params(self, stream, config, state, bookmark_value):
        return {
            'per_page': int(config.get('page_size', DEFAULT_PAGE_SIZE)),
            'page': 1
        }

    def format_response(self, response):
        return response.get('data')

    def fetch_data(self, client: SurveyMonkeyClient, stream, config, state, parent_row=None, bookmark_value=None):
        params = self.get_params(stream, config, state, bookmark_value)
        page = 1
        while True:
            if self.stream_id:
                LOGGER.info("Fetching page {} for {}".format(page, self.stream_id))

            path = self.path
            if parent_row:
                for key, value in parent_row.items():
                    path = path.replace(f'{{parent_{key}}}', value)

            resp = client.make_request(path, params=params, state=state)
            if not resp:
                raise Exception("Resource not found")
            if resp.get('error'):
                raise Exception(resp)

            raw_records = self.format_response(resp)

            for raw_record in raw_records:
                self._modify_record(raw_record)
                yield raw_record

            if not resp['links'].get('next'):
                break

            page += 1
            params.update({'page': page})


class SurveyStream(PaginatedStream):
    key_properties = ['id']

    def get_params(self, stream, config, state, bookmark_value):
        params = super().get_params(stream, config, state, bookmark_value)

        params.update(
            {
                'sort_by': 'date_modified',
                'sort_order': 'ASC',
                'include': 'date_modified'
            })

        if not bookmark_value and config.get('start_date'):
            bookmark_value = config['start_date']

        if bookmark_value:
            params.update({'start_modified_at': bookmark_value})

        return params

    def fetch_data(self, client: SurveyMonkeyClient, stream, config, state, parent_row=None, bookmark_value=None):
        survey_id = config.get('survey_id')
        if survey_id:
            yield {'id': survey_id}
        else:
            for survey_raw_record in super().fetch_data(client, None, config, state):
                yield survey_raw_record


class Surveys(PaginatedStream):
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    replication_key = 'date_modified'
    is_sorted = True

    OPTIONAL_INCLUDE_FIELDS = [
        'response_count',
        'date_created',
        'date_modified',
        'language',
        'question_count'
    ]

    def get_params(self, stream, config, state, bookmark_value):
        params = super().get_params(stream, config, state, bookmark_value=None)
        include_list = params['include'].split(',') if 'include' in params else []

        include_list.append('date_modified')

        # add optional fields
        mdata = metadata.to_map(stream.metadata)
        for optional_field in self.OPTIONAL_INCLUDE_FIELDS:
            if metadata.get(mdata, ('properties', optional_field), 'selected'):
                include_list.append(optional_field)

        # remove duplicates
        include_list = list(dict.fromkeys(include_list))

        params.update(
            {
                'sort_by': 'date_modified',
                'sort_order': 'ASC',
                'include': ','.join(include_list)
            })

        if not bookmark_value and config.get('start_date'):
            bookmark_value = config['start_date']

        if bookmark_value:
            params.update({'start_modified_at': bookmark_value})

        return params

    def _modify_record(self, raw_record):
        super()._modify_record(raw_record)

        patch_time_str(raw_record)


class SurveyDetails(Stream):
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    replication_key = 'date_modified'
    replication_key_from_parent = True
    is_sorted = True

    def _modify_record(self, raw_record):
        super()._modify_record(raw_record)

        patch_time_str(raw_record)

    """
    def sync(self, client: SurveyMonkeyClient, stream, config, state):
        page = 1
        surveys = client.make_request('surveys', params=self.get_params(page, stream, config), state=state)
        while True:
            if not surveys:
                raise Exception("Resource not found")
            if surveys.get('error'):
                raise Exception(surveys)

            for survey in surveys['data']:
                survey_modified = datetime.datetime.strptime(survey['date_modified'], SM_DATE_FORMAT)
                survey_modified = pytz.utc.localize(survey_modified)
                survey_modified_str = singer.utils.strftime(survey_modified)
                if state['bookmarks'].get(self.stream_id, {}).get(survey['id']) == survey_modified_str:
                    continue

                survey_details = client.make_request(
                    'surveys/%s/details' % survey['id'], state=state)
                patch_time_str(survey_details)
                singer.write_records(self.stream_id, [survey_details])

                state['bookmarks'][self.stream_id] = {} if not state['bookmarks'].get(
                    self.stream_id) else state['bookmarks'][self.stream_id]
                state['bookmarks'][self.stream_id][survey_details['id']] = survey_modified_str
                singer.write_state(state)

            if not surveys['links'].get('next'):
                break

            surveys = client.make_request('surveys', params=self.get_params(page, config=config))

        max_time = pytz.utc.localize(datetime.datetime.min)
        for _, last_modified in state['bookmarks'].get(self.stream_id, {}).items():
            if max_time < pytz.utc.localize(strptime(last_modified)):
                max_time = pytz.utc.localize(strptime(last_modified))

        state['bookmarks'][self.stream_id] = {} if not state['bookmarks'].get(
            self.stream_id) else state['bookmarks'][self.stream_id]
        state['bookmarks'][self.stream_id]['full_sync'] = singer.utils.strftime(max_time)
        singer.write_state(state)

        return state
    """


class Responses(PaginatedStream):
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    replication_key = 'date_modified'
    is_sorted = True

    def __init__(self, stream_id: str, path: str, parent_stream, simple: bool = False):
        super().__init__(stream_id=stream_id, path=path, parent_stream=parent_stream)
        self.simple = simple

    def get_params(self, stream, config, state, bookmark_value):
        params = super().get_params(stream, config, state, bookmark_value=None)

        params.update(
            {
                'sort_by': 'date_modified',
                'sort_order': 'ASC'
            })

        if not bookmark_value and config.get('start_date'):
            bookmark_value = config['start_date']

        if bookmark_value:
            params.update({'start_modified_at': bookmark_value})

        return params

    def _modify_record(self, raw_record):
        super()._modify_record(raw_record)

        patch_time_str(raw_record)

    """
    def sync_responses(self, client: SurveyMonkeyClient, config, state, simplify=False):
        survey_id = config.get('survey_id')
        if not survey_id:
            raise Exception("Survey ID not provided. Syncing Responses requires a Survey ID. ")

        stream_id = 'simplified_responses' if simplify else 'responses'
        per_page = int(config.get("page_size", "50"))  # Max 100
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

        responses = client.make_request(
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
            responses = client.make_request(
                'surveys/%s/responses/bulk' % survey_id, params=params, state=state)

        max_time = find_max_timestamp(state, stream_id)

        state['bookmarks'][stream_id] = {} if not state['bookmarks'].get(
            stream_id) else state['bookmarks'][stream_id]
        state['bookmarks'][stream_id]['full_sync'] = singer.utils.strftime(max_time)
        singer.write_state(state)

        return state
    """


STREAMS = {
    'surveys': Surveys(
        stream_id='surveys',
        path='surveys'),
    'survey_details': SurveyDetails(
        stream_id='survey_details',
        path='surveys/{parent_id}/details',
        parent_stream=SurveyStream(
            stream_id=None,
            path='surveys'
        )),
    'responses': Responses(
        stream_id='responses',
        path='surveys/{parent_id}/responses/bulk',
        parent_stream=SurveyStream(
            stream_id=None,
            path='surveys'
        )),
    'simplified_responses': Responses(
        stream_id='simplified_responses',
        path='surveys/{parent_id}/responses/bulk',
        parent_stream=SurveyStream(
            stream_id=None,
            path='surveys'
        ),
        simple=True),
}
