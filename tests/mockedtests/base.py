"""Base test class for tap-surveymonkey mock-based integration tests.

Plain mixin — no tap-tester dependency. Mix with unittest.TestCase in each
test class. Run with: python -m pytest tests/ -v
"""
import json
import os

from singer import metadata
from singer.catalog import Catalog, CatalogEntry

from tap_surveymonkey.discover import discover


# ---------------------------------------------------------------------------
# MockResponse — minimal stand-in for requests.Response
# ---------------------------------------------------------------------------
class MockResponse:
    """Minimal requests.Response stand-in used by patched HTTP calls."""

    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code
        self.headers = {}

    def json(self):
        return self._payload

    def raise_for_status(self):
        pass


# ---------------------------------------------------------------------------
# Base mixin
# ---------------------------------------------------------------------------
class SurveyMonkeyBaseTest:
    """Base test case for tap-surveymonkey integration tests with mocked data.

    Not a TestCase itself — mix with unittest.TestCase in each test class.

    Stream metadata table:
    ┌──────────────────────┬───────────┬─────────────┬────────────────┬────────────┬──────────────────┬─────────────┐
    │ stream_name          │ prim_keys │ rep_method  │ rep_keys       │ api_limit  │ obeys_start_date │ parent      │
    ├──────────────────────┼───────────┼─────────────┼────────────────┼────────────┼──────────────────┼─────────────┤
    │ surveys              │ {id}      │ INCREMENTAL │ {date_modified}│ 50         │ True             │ —           │
    │ survey_details       │ {id}      │ INCREMENTAL │ {date_modified}│ 50         │ True             │ surveys     │
    │ responses            │ {id}      │ INCREMENTAL │ {date_modified}│ 50         │ True             │ surveys     │
    │ simplified_responses │ {id}      │ INCREMENTAL │ {date_modified}│ 50         │ True             │ surveys     │
    └──────────────────────┴───────────┴─────────────┴────────────────┴────────────┴──────────────────┴─────────────┘
    """

    # ── Metadata key constants ───────────────────────────────────────────────
    PRIMARY_KEYS       = "primary_keys"
    REPLICATION_METHOD = "replication_method"
    REPLICATION_KEYS   = "replication_keys"
    OBEYS_START_DATE   = "obeys_start_date"
    API_LIMIT          = "api_limit"
    PARENT_STREAM      = "parent_stream"
    IS_FORBIDDEN_STREAM = "is_forbidden_stream"
    INCREMENTAL        = "INCREMENTAL"
    FULL_TABLE         = "FULL_TABLE"

    default_start_date = "2020-01-01T00:00:00Z"

    # ── Expected stream metadata ─────────────────────────────────────────────
    @classmethod
    def expected_metadata(cls):
        """The expected streams and metadata about the streams."""
        return {
            "surveys": {
                cls.PRIMARY_KEYS:       {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS:   {"date_modified"},
                cls.OBEYS_START_DATE:   True,
                cls.API_LIMIT:          50,
            },
            "survey_details": {
                cls.PRIMARY_KEYS:       {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS:   {"date_modified"},
                cls.OBEYS_START_DATE:   True,
                cls.API_LIMIT:          50,
                cls.PARENT_STREAM:      "surveys",
            },
            "responses": {
                cls.PRIMARY_KEYS:       {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS:   {"date_modified"},
                cls.OBEYS_START_DATE:   True,
                cls.API_LIMIT:          50,
                cls.PARENT_STREAM:      "surveys",
            },
            "simplified_responses": {
                cls.PRIMARY_KEYS:       {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS:   {"date_modified"},
                cls.OBEYS_START_DATE:   True,
                cls.API_LIMIT:          50,
                cls.PARENT_STREAM:      "surveys",
            },
        }

    def expected_stream_names(self):
        """The expected stream names, excludes forbidden streams."""
        return {
            stream_name
            for stream_name, meta in self.expected_metadata().items()
            if not meta.get(self.IS_FORBIDDEN_STREAM, False)
        }

    # ── Setup / teardown ────────────────────────────────────────────────────
    def setUp(self):
        """Set up test fixtures."""
        self.config = self.get_mock_config()
        self.state = {}

    def tearDown(self):
        """Clean up after tests."""

    # ── Mock configuration ──────────────────────────────────────────────────
    @staticmethod
    def get_mock_config():
        """Return mock configuration with dummy values — no real credentials."""
        return {
            "access_token": "mock_test_access_token",
            "start_date":   "2020-01-01T00:00:00Z",
        }

    @staticmethod
    def get_mock_state():
        """Return a blank initial mock state."""
        return {}

    # ── Catalog helpers ─────────────────────────────────────────────────────
    @staticmethod
    def _make_catalog(stream_names=None):
        """Return a Singer Catalog with the requested streams selected.

        Calls discover() so schemas are real; marks each stream as selected
        so catalog.get_selected_streams() yields them.

        Args:
            stream_names: iterable of stream names to select. If None, selects all.
        """
        cat = discover()
        if stream_names is None:
            stream_names = {e.tap_stream_id for e in cat.streams}
        else:
            stream_names = set(stream_names)

        selected = []
        for entry in cat.streams:
            if entry.tap_stream_id not in stream_names:
                continue
            mdata = metadata.to_map(entry.metadata)
            mdata[()] = dict(mdata.get((), {}))
            mdata[()]["selected"] = True
            entry.metadata = metadata.to_list(mdata)
            selected.append(entry)

        return Catalog(selected)

    # ── Schema-driven mock data generation ─────────────────────────────────
    @staticmethod
    def _schema_path(stream_name):
        base_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        return os.path.join(base_dir, "tap_surveymonkey", "schemas",
                            f"{stream_name}.json")

    @classmethod
    def _load_schema(cls, stream_name):
        """Load and return the JSON schema dict for the given stream."""
        with open(cls._schema_path(stream_name), "r", encoding="utf-8") as fh:
            return json.load(fh)

    @staticmethod
    def _schema_type(schema):
        """Return the concrete JSON-schema type, resolving null unions."""
        t = schema.get("type", "object")
        if isinstance(t, list):
            non_null = [x for x in t if x != "null"]
            return non_null[0] if non_null else "null"
        return t

    @staticmethod
    def _generate_value(schema, date_value="2024-01-01T00:00:00.000000Z"):
        """Recursively generate one valid mock value for a JSON-schema fragment."""
        if "enum" in schema and schema["enum"]:
            return schema["enum"][0]

        schema_type = SurveyMonkeyBaseTest._schema_type(schema)

        if schema_type == "object":
            props = schema.get("properties", {})
            return {k: SurveyMonkeyBaseTest._generate_value(v, date_value)
                   for k, v in props.items()}

        if schema_type == "array":
            return [SurveyMonkeyBaseTest._generate_value(
                schema.get("items", {"type": "string"}), date_value)]

        if schema_type == "string":
            fmt = schema.get("format")
            if fmt == "date-time":
                return date_value
            if fmt == "email":
                return "mock@example.com"
            return "mock"

        if schema_type == "integer":
            return 1
        if schema_type == "number":
            return 1.0
        if schema_type == "boolean":
            return True

        return None

    @classmethod
    def _generate_stream_record(cls, stream_name, date_value="2024-01-01T00:00:00.000000Z"):
        """Generate one schema-valid mock record for the given stream."""
        return cls._generate_value(cls._load_schema(stream_name), date_value=date_value)

    # ── HTTP mock factory ───────────────────────────────────────────────────
    # patch target: "tap_surveymonkey.client.requests.request"
    # All HTTP traffic flows through SurveyMonkeyClient.make_request which
    # calls requests.request(method, url, headers=..., **kwargs).
    # The mock receives (method, url, ...) positional args.

    @classmethod
    def _make_surveys_page(cls, survey_ids=("s1",), date_value="2024-01-01T00:00:00.000000Z", has_next=False):
        """Build a paginated /surveys API response."""
        data = [{"id": sid, "date_modified": date_value} for sid in survey_ids]
        links = {"next": "?page=2"} if has_next else {}
        return {"data": data, "links": links, "total": len(data)}

    @classmethod
    def _make_survey_details_record(cls, survey_id="s1", date_value="2024-01-01T00:00:00.000000Z"):
        """Build a /surveys/<id>/details API response (single record)."""
        rec = cls._generate_stream_record("survey_details", date_value=date_value)
        rec["id"] = survey_id
        rec["date_modified"] = date_value
        return rec

    @classmethod
    def _make_responses_page(cls, survey_id="s1", count=2,
                              date_value="2024-01-01T00:00:00.000000Z", has_next=False):
        """Build a paginated /surveys/<id>/responses/bulk API response."""
        data = []
        for i in range(count):
            r = cls._generate_stream_record("responses", date_value=date_value)
            r["id"] = f"{survey_id}_resp_{i}"
            r["survey_id"] = survey_id
            r["date_modified"] = date_value
            data.append(r)
        links = {"next": "?page=2"} if has_next else {}
        return {"data": data, "links": links, "total": len(data)}

    @classmethod
    def _mock_request_fn(cls, survey_id="s1", date_value="2024-01-01T00:00:00.000000Z"):
        """Return a side-effect callable that routes URLs to fixture responses.

        Routes:
          /surveys              → paginated surveys list (single page)
          /surveys/<id>/details → single survey-details record
          /surveys/<id>/responses/bulk → paginated responses (single page)
        """
        def _fn(method, url, **kwargs):
            if url.endswith("/details"):
                return MockResponse(cls._make_survey_details_record(
                    survey_id=survey_id, date_value=date_value))
            if "/responses/bulk" in url:
                return MockResponse(cls._make_responses_page(
                    survey_id=survey_id, date_value=date_value))
            # This is the /surveys list endpoint used by SurveyStream (parent iterator)
            return MockResponse(cls._make_surveys_page(
                survey_ids=(survey_id,), date_value=date_value))
        return _fn
