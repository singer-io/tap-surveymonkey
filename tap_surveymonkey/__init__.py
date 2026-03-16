#!/usr/bin/env python3
# pylint: disable=E1111
import singer

REQUIRED_CONFIG_KEYS = ["start_date", "access_token"]
LOGGER = singer.get_logger()


@singer.utils.handle_top_exception(LOGGER)
def main():
    # Import here to avoid shadowing submodule names on the package namespace,
    # which would break mock.patch targets like 'tap_surveymonkey.sync.singer'.
    from tap_surveymonkey.discover import discover
    from tap_surveymonkey.sync import sync

    # Parse command line arguments
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode
    if args.discover:
        catalog = discover().dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()

        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
