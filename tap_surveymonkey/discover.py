import os
import singer
from singer import utils
from singer.catalog import Catalog
from singer import metadata
from tap_surveymonkey.exceptions import SurveyMonkeyForbiddenError
from tap_surveymonkey.streams import STREAMS


LOGGER = singer.get_logger()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def get_schemas():
    """
    Builds the singer schema and metadata dictionaries.
    """

    schemas = {}
    schemas_metadata = {}

    for stream_name, stream_object in STREAMS.items():

        schema_path = get_abs_path(f"schemas/{stream_name}.json")
        schema = utils.load_json(schema_path)

        if stream_object.replication_method == "INCREMENTAL":
            replication_keys = [stream_object.replication_key]
        else:
            replication_keys = None

        meta = metadata.to_map(metadata.get_standard_metadata(schema=schema,
                                              key_properties=stream_object.key_properties,
                                              replication_method=stream_object.replication_method,
                                              valid_replication_keys=replication_keys,))

        if replication_keys:
            for replication_key in replication_keys:
                meta = metadata.write(meta,
                                      ("properties", replication_key),
                                      "inclusion",
                                      "automatic")

        parent_tap_stream_id = getattr(stream_object, "parent", None)
        if parent_tap_stream_id:
            meta = metadata.write(meta, (), 'parent-tap-stream-id', parent_tap_stream_id)

        meta = metadata.to_list(meta)

        schemas[stream_name] = schema
        schemas_metadata[stream_name] = meta

    return schemas, schemas_metadata


def _apply_access_checks(client, schemas, field_metadata):
    """
    Probe each parent stream for read access and remove inaccessible streams
    (and their children) from schemas and field_metadata in place.
    Raises SurveyMonkeyForbiddenError if no parent streams are accessible.
    """
    inaccessible_streams = [
        stream_name
        for stream_name, stream_obj in STREAMS.items()
        if stream_name in schemas
        and not stream_obj.parent
        and not stream_obj.check_access(client)
    ]

    for stream_name in inaccessible_streams:
        schemas.pop(stream_name, None)
        field_metadata.pop(stream_name, None)

    _prune_inaccessible_children(schemas, field_metadata)

    if not schemas:
        raise SurveyMonkeyForbiddenError(
            "No streams are accessible. Ensure the credentials have read permission for at least one stream."
        )
    elif inaccessible_streams:
        LOGGER.warning(
            "These streams have been excluded due to HTTP-Error-Code:403 Forbidden: %s",
            ", ".join(inaccessible_streams),
        )


def _prune_inaccessible_children(schemas, field_metadata):
    """
    Remove child streams from the catalog whose parent stream was excluded.
    Mutates schemas and field_metadata in place.
    """
    for name, stream_obj in list(STREAMS.items()):
        if name in schemas and stream_obj.parent and stream_obj.parent not in schemas:
            LOGGER.warning(
                "Stream '%s' excluded from catalog because its parent stream '%s' is not accessible.",
                name, stream_obj.parent,
            )
            schemas.pop(name, None)
            field_metadata.pop(name, None)


def discover(client):
    """
    Builds the singer catalog for all the streams in the schemas directory.
    Access to each stream is verified using the provided client and streams
    the credentials cannot read are excluded from the returned catalog.
    """

    schemas, schemas_metadata = get_schemas()
    _apply_access_checks(client, schemas, schemas_metadata)

    streams = []

    for schema_name, schema in schemas.items():
        schema_meta = schemas_metadata[schema_name]

        catalog_entry = {
            "stream": schema_name,
            "tap_stream_id": schema_name,
            "schema": schema,
            "metadata": schema_meta,
            "key_properties": STREAMS[schema_name].key_properties,
        }

        streams.append(catalog_entry)

    return Catalog.from_dict({"streams": streams})
