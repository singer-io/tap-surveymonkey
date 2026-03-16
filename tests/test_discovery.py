"""Test tap discovery mode and metadata."""
import json

from tap_surveymonkey.discover import discover
from base import SurveyMonkeyBaseTest


class SurveyMonkeyDiscoveryTest(SurveyMonkeyBaseTest):
    """Test tap discovery mode and metadata conforms to standards."""

    @staticmethod
    def name():
        return "tap_tester_surveymonkey_discovery_test"

    def streams_to_test(self):
        return self.expected_stream_names()

    def test_discovery(self):
        """Discover catalog and verify streams, schemas, keys, and replication metadata."""
        catalog = discover()

        # Discovered stream names match expectations
        discovered = {s.tap_stream_id for s in catalog.streams}
        self.assertSetEqual(discovered, self.streams_to_test())

        # Catalog is JSON-serialisable
        json.dumps(catalog.to_dict())

        expected_pks = self.expected_primary_keys()
        expected_rep_keys = self.expected_replication_keys()
        expected_rep_methods = self.expected_replication_methods()

        for stream in catalog.streams:
            stream_name = stream.tap_stream_id
            schema = stream.schema.to_dict()
            props = schema.get("properties", {})

            # Schema is a valid JSON-Schema object
            self.assertEqual(schema.get("type"), "object")
            self.assertIsInstance(props, dict)

            # stream name matches tap_stream_id
            self.assertEqual(stream.stream, stream_name)

            # All primary keys present in schema
            for pk in expected_pks.get(stream_name, []):
                self.assertIn(pk, props)

            # Replication key present in schema and marked automatic
            rep_key = expected_rep_keys.get(stream_name)
            if rep_key:
                self.assertIn(rep_key, props)
                rep_key_meta = next(
                    (m["metadata"] for m in stream.metadata
                     if m.get("breadcrumb") == ("properties", rep_key)),
                    {}
                )
                self.assertEqual(rep_key_meta.get("inclusion"), "automatic")

            # Root metadata has correct replication method and key properties
            root_meta = next(
                (m["metadata"] for m in stream.metadata
                 if m.get("breadcrumb") == ()),
                {}
            )
            self.assertEqual(
                root_meta.get("forced-replication-method"),
                expected_rep_methods.get(stream_name),
            )
            self.assertEqual(
                root_meta.get("table-key-properties"),
                expected_pks.get(stream_name),
            )
