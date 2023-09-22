import singer
from singer.transform import Transformer
from singer import bookmarks, metadata, metrics

from tap_surveymonkey.client import SurveyMonkeyClient
from tap_surveymonkey.streams import STREAMS

LOGGER = singer.get_logger()


def sync(config, state, catalog):
    """ Sync data from tap source """
    access_token = config["access_token"]
    survey_id = config.get("survey_id")
    client = SurveyMonkeyClient(access_token)

    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        stream_object = STREAMS[stream.tap_stream_id]
        mdata = metadata.to_map(stream.metadata)
        raw_schema = stream.schema.to_dict()
        LOGGER.info("Syncing stream: " + stream.tap_stream_id)

        bookmark_column = stream_object.replication_key
        is_sorted = stream_object.is_sorted # indicate whether data is sorted ascending on bookmark value

        # Publish schema to singer.
        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=raw_schema,
            key_properties=stream.key_properties,
        )

        with metrics.record_counter(stream.tap_stream_id) as counter:
            max_bookmark = None
            bookmark_value = None
            row_brk_value = None
            with Transformer() as transformer:

                for parent_row in stream_object.parent_stream.fetch_data(client, None, config, state, bookmark_value) if stream_object.parent_stream else [{}]:
                    bookmark_value = bookmarks.get_bookmark(state, stream_object.stream_id, survey_id)

                    if not bookmark_value and not survey_id:
                        bookmark_value = bookmarks.get_bookmark(state, stream_object.stream_id, "full_sync")

                    elif not bookmark_value and stream.tap_stream_id == "surveys":
                        bookmark_value = bookmarks.get_bookmark(state, stream_object.stream_id, "full_sync")

                    for row in stream_object.fetch_data(client, stream, config, state, parent_row=parent_row, bookmark_value=bookmark_value):
                        # write one or more rows to the stream:
                        if not bookmark_value or (bookmark_value and row[bookmark_column] >= bookmark_value):
                            singer.write_record(stream.tap_stream_id, transformer.transform(row, raw_schema, mdata))
                            row_brk_value = row[bookmark_column]
                            counter.increment()

                        if not stream_object.replication_key_from_parent:
                            if is_sorted and stream.tap_stream_id != "surveys":
                                # update bookmark to latest value
                                state = bookmarks.write_bookmark(state, stream.tap_stream_id, row["survey_id"], row[bookmark_column])
                                singer.write_state(state)
                            max_bookmark = max(max_bookmark, row[bookmark_column]) if max_bookmark else row[bookmark_column]
                        
                    if row_brk_value and parent_row and stream_object.replication_key_from_parent:
                        if is_sorted:
                            # update bookmark to latest value
                            state = bookmarks.write_bookmark(state, stream.tap_stream_id, parent_row["id"], parent_row.get(bookmark_column, row_brk_value))
                            singer.write_state(state)
                        max_bookmark = max(max_bookmark, parent_row.get(bookmark_column, row_brk_value)) if max_bookmark else parent_row.get(bookmark_column, row_brk_value)

            if not survey_id or stream.tap_stream_id == "surveys":
                state = bookmarks.write_bookmark(state, stream.tap_stream_id, "full_sync", max_bookmark)
                singer.write_state(state)

            LOGGER.info("Stream: {}, Processed {} records.".format(stream.tap_stream_id, counter.value))
