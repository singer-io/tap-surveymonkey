import os
import json
from singer import metadata

STREAMS = {
    'responses': {
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['date_modified']
    },
    'simplified_responses': {
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['date_modified']
    },
    'survey_details': {
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['date_modified']
    }
}


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def get_schemas():
    schemas = {}
    schemas_metadata = {}

    for stream_name, stream_metadata in STREAMS.items():
        schema = None
        schema_path = get_abs_path('schemas/{}.json'.format(stream_name))
        with open(schema_path) as file:
            schema = json.load(file)
        meta = metadata.get_standard_metadata(
            schema=schema,
            key_properties=stream_metadata.get('key_properties', None),
            valid_replication_keys=stream_metadata.get('replication_keys', None),
            replication_method=stream_metadata.get('replication_method', None)
        )
        schemas[stream_name] = schema
        schemas_metadata[stream_name] = meta

    return schemas, schemas_metadata
