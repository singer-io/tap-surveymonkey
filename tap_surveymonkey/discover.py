import os
from singer import utils
from singer.catalog import Catalog
from singer import metadata

from tap_surveymonkey.streams import STREAMS


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(tap_stream_id):
    path = "schemas/{}.json".format(tap_stream_id)
    return utils.load_json(get_abs_path(path))


def discover():
    """
    Run discovery mode
    """
    streams = []

    for stream_id, stream_object in STREAMS.items():
        schema = load_schema(stream_id)
        mdata = metadata.to_map(metadata.get_standard_metadata(
            schema=schema,
            schema_name=stream_id,
            key_properties=stream_object.key_properties,
            valid_replication_keys=[stream_object.replication_key],
            replication_method=stream_object.replication_method
        ))

        # make sure that the replication key field is mandatory
        if stream_object.replication_key:
            metadata.write(mdata, ("properties", stream_object.replication_key), "inclusion", "automatic")
        catalog_entry = {
            "stream": stream_id,
            "tap_stream_id": stream_id,
            "key_properties": stream_object.key_properties,
            "schema": schema,
            "metadata": metadata.to_list(mdata)
        }
        streams.append(catalog_entry)
    return Catalog.from_dict({"streams": streams})
