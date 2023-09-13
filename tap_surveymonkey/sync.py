import singer
from singer.transform import Transformer
from singer import bookmarks, metadata, metrics

from tap_surveymonkey.client import SurveyMonkeyClient
from tap_surveymonkey.streams import STREAMS


LOGGER = singer.get_logger()


def build_bookmark_key_prefix(parent_row, parent_stream_object):
    if not parent_row:
        return ''
    key_values = []
    for key in parent_stream_object.key_properties:
        key_values.append(parent_row[key])
    return '_'.join(key_values) + '_'


def sync(config, state, catalog):
    """ Sync data from tap source """
    access_token = config['access_token']
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
            bookmark_key_prefix = ''
            with Transformer() as transformer:
                if stream_object.replication_key_from_parent:
                    if bookmarks.get_bookmark(state, stream_object.stream_id, 'page_sync'):
                        bookmark_value = bookmarks.get_bookmark(state, stream_object.stream_id, 'page_sync')

                    if bookmarks.get_bookmark(state, stream_object.stream_id, 'full_sync'):
                        bookmark_value = bookmarks.get_bookmark(state, stream_object.stream_id, 'full_sync')

                for parent_row in stream_object.parent_stream.fetch_data(client, None, config, state, bookmark_value=bookmark_value) if stream_object.parent_stream else [{}]:
                    bookmark_value = None

                    if not stream_object.replication_key_from_parent:
                        bookmark_key_prefix = build_bookmark_key_prefix(parent_row, stream_object.parent_stream)

                        if bookmarks.get_bookmark(state, stream_object.stream_id, 'page_sync'):
                            bookmark_value = bookmarks.get_bookmark(state, stream_object.stream_id, f'{bookmark_key_prefix}page_sync')

                        if bookmarks.get_bookmark(state, stream_object.stream_id, 'full_sync'):
                            bookmark_value = bookmarks.get_bookmark(state, stream_object.stream_id, f'{bookmark_key_prefix}full_sync')

                    for row in stream_object.fetch_data(client, stream, config, state, parent_row=parent_row, bookmark_value=bookmark_value):
                        # write one or more rows to the stream:
                        singer.write_record(stream.tap_stream_id, transformer.transform(row, raw_schema, mdata))

                        if bookmark_column and not stream_object.replication_key_from_parent:
                            if is_sorted:
                                # update bookmark to latest value
                                state = bookmarks.write_bookmark(state, stream.tap_stream_id, f'{bookmark_key_prefix}page_sync', row[bookmark_column])
                                singer.write_state(state)
                            else:
                                # if data unsorted, save max value until end of writes
                                max_bookmark = max(max_bookmark, row[bookmark_column])
                        
                        counter.increment()

                    if bookmark_column and parent_row and stream_object.replication_key_from_parent:
                        if is_sorted:
                            # update bookmark to latest value
                            state = bookmarks.write_bookmark(state, stream.tap_stream_id, f'{bookmark_key_prefix}page_sync', parent_row[bookmark_column])
                            singer.write_state(state)
                        else:
                            # if data unsorted, save max value until end of writes
                            max_bookmark = max(max_bookmark, parent_row[bookmark_column])
                if bookmark_column and not is_sorted:
                    state = bookmarks.write_bookmark(state, stream.tap_stream_id, f'{bookmark_key_prefix}full_sync', row[bookmark_column])
                    singer.write_state(state)

            if bookmark_column and not is_sorted and parent_row and stream_object.replication_key_from_parent:
                state = bookmarks.write_bookmark(state, stream.tap_stream_id, f'{bookmark_key_prefix}full_sync', parent_row[bookmark_column])
                singer.write_state(state)

        LOGGER.info('Stream: {}, Processed {} records'.format(stream.tap_stream_id, counter.value))
