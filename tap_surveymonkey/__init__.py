#!/usr/bin/env python3
import json
import singer
from tap_surveymonkey.data import SurveyMonkey
from tap_surveymonkey.mode import sync, discover


REQUIRED_CONFIG_KEYS = ["start_date", "access_token"]
LOGGER = singer.get_logger()


@singer.utils.handle_top_exception(LOGGER)
def main():

    # Parse command line arguments
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

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
            catalog = args.catalog.to_dict()
        else:
            catalog = discover()

        state = args.state or {
            'bookmarks': {}
        }

        sync(args.config, state, catalog)


if __name__ == "__main__":
    main()
