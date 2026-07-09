import unittest
import runpy
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from tap_surveymonkey import main


class TestMainTokenValidation(unittest.TestCase):

    @patch("tap_surveymonkey.sync")
    @patch("tap_surveymonkey.discover")
    @patch("tap_surveymonkey.SurveyMonkeyClient")
    @patch("tap_surveymonkey.singer.utils.parse_args")
    def test_discover_mode_constructs_client_before_discover(
        self,
        mock_parse_args,
        mock_client_cls,
        mock_discover,
        mock_sync,
    ):
        args = SimpleNamespace(
            discover=True,
            catalog=None,
            config={"access_token": "token"},
            state={},
        )
        mock_parse_args.return_value = args

        catalog_obj = MagicMock()
        mock_discover.return_value = catalog_obj

        main()

        mock_client_cls.assert_called_once_with("token")
        mock_discover.assert_called_once_with()
        catalog_obj.dump.assert_called_once_with()
        mock_sync.assert_not_called()

    @patch("tap_surveymonkey.sync")
    @patch("tap_surveymonkey.discover")
    @patch("tap_surveymonkey.SurveyMonkeyClient")
    @patch("tap_surveymonkey.singer.utils.parse_args")
    def test_sync_without_catalog_constructs_client_before_discover(
        self,
        mock_parse_args,
        mock_client_cls,
        mock_discover,
        mock_sync,
    ):
        args = SimpleNamespace(
            discover=False,
            catalog=None,
            config={"access_token": "token"},
            state={"bookmarks": {}},
        )
        mock_parse_args.return_value = args

        catalog_obj = MagicMock()
        mock_discover.return_value = catalog_obj

        main()

        mock_client_cls.assert_called_once_with("token")
        mock_discover.assert_called_once_with()
        mock_sync.assert_called_once_with(args.config, args.state, catalog_obj)

    @patch("tap_surveymonkey.sync")
    @patch("tap_surveymonkey.discover")
    @patch("tap_surveymonkey.SurveyMonkeyClient")
    @patch("tap_surveymonkey.singer.utils.parse_args")
    def test_sync_with_catalog_constructs_client_without_discovery(
        self,
        mock_parse_args,
        mock_client_cls,
        mock_discover,
        mock_sync,
    ):
        provided_catalog = MagicMock()
        args = SimpleNamespace(
            discover=False,
            catalog=provided_catalog,
            config={"access_token": "token"},
            state={"bookmarks": {}},
        )
        mock_parse_args.return_value = args

        main()

        mock_client_cls.assert_called_once_with("token")
        mock_discover.assert_not_called()
        mock_sync.assert_called_once_with(args.config, args.state, provided_catalog)


class TestMainEntrypoint(unittest.TestCase):

    @patch("tap_surveymonkey.sync.sync")
    @patch("tap_surveymonkey.discover.discover")
    @patch("tap_surveymonkey.client.SurveyMonkeyClient")
    @patch("singer.utils.parse_args")
    def test_run_as_main_invokes_main_block(
        self,
        mock_parse_args,
        mock_client_cls,
        mock_discover,
        mock_sync,
    ):
        args = SimpleNamespace(
            discover=False,
            catalog=MagicMock(),
            config={"access_token": "token"},
            state={"bookmarks": {}},
        )
        mock_parse_args.return_value = args

        runpy.run_path(
            "/home/akkumar/projects/taps/tap-surveymonkey/tap_surveymonkey/__init__.py",
            run_name="__main__",
        )

        mock_client_cls.assert_called_once_with("token")
        mock_sync.assert_called_once_with(args.config, args.state, args.catalog)
        mock_discover.assert_not_called()
