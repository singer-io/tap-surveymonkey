import datetime
import pytz
import singer.utils
from singer import metadata

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
    for _, last_modified in state["bookmarks"].get(stream_id, {}).items():
        if max_time < pytz.utc.localize(strptime(last_modified)):
            max_time = pytz.utc.localize(strptime(last_modified))
    return max_time


def patch_time_str(obj_dict):
    # The target expects [yyyy-MM-dd"T"HH:mm:ssZ, yyyy-MM-dd"T"HH:mm:ss.[0-9]{1,9}Z] only
    if obj_dict.get("date_modified"):
        time_obj = singer.utils.strptime_to_utc(obj_dict["date_modified"])
        time_str = singer.utils.strftime(time_obj)
        obj_dict["date_modified"] = time_str
    if obj_dict.get("date_created"):
        time_obj = singer.utils.strptime_to_utc(obj_dict["date_created"])
        time_str = singer.utils.strftime(time_obj)
        obj_dict["date_created"] = time_str


class Stream:
    key_properties = None
    replication_method = None
    replication_key = None
    replication_key_from_parent = False # for streams which just return a single record and iterate by its parent, e.g. "SurveyDetails"
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
                path = path.replace(f"{{parent_{key}}}", value)

        resp = client.make_request(path, params=None, state=state)
        if not resp:
            raise Exception("Resource not found")
        if resp.get("error"):
            raise Exception(resp)

        self._modify_record(resp)
        yield resp


class PaginatedStream(Stream):
    def get_params(self, stream, config, state, bookmark_value):
        return {
            "per_page": int(config.get("page_size", DEFAULT_PAGE_SIZE)),
            "page": 1
        }

    def format_response(self, response):
        return response.get("data")

    def fetch_data(self, client: SurveyMonkeyClient, stream, config, state, parent_row=None, bookmark_value=None):
        params = self.get_params(stream, config, state, bookmark_value)
        page = 1
        while True:
            if self.stream_id:
                LOGGER.info("Fetching page {} for {}".format(page, self.stream_id))

            path = self.path
            if parent_row:
                for key, value in parent_row.items():
                    path = path.replace(f"{{parent_{key}}}", value)

            resp = client.make_request(path, params=params, state=state)
            if not resp:
                raise Exception("Resource not found")
            if resp.get("error"):
                raise Exception(resp)

            raw_records = self.format_response(resp)

            for raw_record in raw_records:
                self._modify_record(raw_record)
                yield raw_record

            if not resp["links"].get("next"):
                break

            page += 1
            params.update({"page": page})


class SurveyStream(PaginatedStream):
    key_properties = ["id"]

    def get_params(self, stream, config, state, bookmark_value):
        params = super().get_params(stream, config, state, bookmark_value)

        params.update(
            {
                "sort_by": "date_modified",
                "sort_order": "ASC",
                "include": "date_modified"
            })

        if not bookmark_value and config.get("start_date"):
            bookmark_value = config["start_date"]

        if bookmark_value:
            params.update({"start_modified_at": bookmark_value})

        return params

    def fetch_data(self, client: SurveyMonkeyClient, stream, config, state, parent_row=None, bookmark_value=None):
        survey_id = config.get("survey_id")
        if survey_id:
            yield {"id": survey_id}
        else:
            for survey_raw_record in super().fetch_data(client, None, config, state):
                yield survey_raw_record


class Surveys(PaginatedStream):
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_key = "date_modified"
    is_sorted = True

    OPTIONAL_INCLUDE_FIELDS = [
        "response_count",
        "date_created",
        "date_modified",
        "language",
        "question_count"
    ]

    def get_params(self, stream, config, state, bookmark_value):
        params = super().get_params(stream, config, state, bookmark_value=None)
        include_list = params["include"].split(",") if "include" in params else []

        include_list.append("date_modified")

        # add optional fields
        mdata = metadata.to_map(stream.metadata)
        for optional_field in self.OPTIONAL_INCLUDE_FIELDS:
            if metadata.get(mdata, ("properties", optional_field), "selected"):
                include_list.append(optional_field)

        # remove duplicates
        include_list = list(dict.fromkeys(include_list))

        params.update(
            {
                "sort_by": "date_modified",
                "sort_order": "ASC",
                "include": ",".join(include_list)
            })

        if not bookmark_value and config.get("start_date"):
            bookmark_value = config["start_date"]

        elif bookmark_value:
            bookmark_value_minus_1_min = (datetime.datetime.strptime(bookmark_value, DATETIME_FMT_MAC) - datetime.timedelta(minutes = 1)).strftime(DATETIME_FMT_MAC)
            params.update({"start_modified_at": bookmark_value_minus_1_min})

        return params

    def _modify_record(self, raw_record):
        super()._modify_record(raw_record)

        patch_time_str(raw_record)


class SurveyDetails(Stream):
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_key = "date_modified"
    replication_key_from_parent = True
    is_sorted = True

    def _modify_record(self, raw_record):
        super()._modify_record(raw_record)

        patch_time_str(raw_record)


class Responses(PaginatedStream):
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_key = "date_modified"
    is_sorted = True

    def __init__(self, stream_id: str, path: str, parent_stream, simple: bool = False):
        super().__init__(stream_id=stream_id, path=path, parent_stream=parent_stream)
        self.simple = simple

    def get_params(self, stream, config, state, bookmark_value):
        params = super().get_params(stream, config, state, bookmark_value=None)
        if self.simple:
            params["simple"] = True

        params.update(
            {
                "sort_by": "date_modified",
                "sort_order": "ASC"
            })

        if not bookmark_value and config.get("start_date"):
            bookmark_value = config["start_date"]

        if bookmark_value:
            params.update({"start_modified_at": bookmark_value})

        return params

    def _modify_record(self, raw_record):
        super()._modify_record(raw_record)

        patch_time_str(raw_record)


STREAMS = {
    "surveys": Surveys(
        stream_id="surveys",
        path="surveys"),
    "survey_details": SurveyDetails(
        stream_id="survey_details",
        path="surveys/{parent_id}/details",
        parent_stream=SurveyStream(
            stream_id=None,
            path="surveys"
        )),
    "responses": Responses(
        stream_id="responses",
        path="surveys/{parent_id}/responses/bulk",
        parent_stream=SurveyStream(
            stream_id=None,
            path="surveys"
        )),
    "simplified_responses": Responses(
        stream_id="simplified_responses",
        path="surveys/{parent_id}/responses/bulk",
        parent_stream=SurveyStream(
            stream_id=None,
            path="surveys"
        ),
        simple=True),
}
