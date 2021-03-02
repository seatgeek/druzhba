import unittest
from unittest.mock import patch

from druzhba.main import set_up_database


class ProcessDbTest(unittest.TestCase):
    def test_basic_config(self):
        db, dbconfig = set_up_database(
            "pgtest",
            "postgres",
            None,
            None,
            "test/integration/config",
        )

        self.assertEqual(db.database_alias, "pgtest")
        self.assertEqual(db.database_type, "postgres")
        self.assertEqual(db._connection_string, None)
        self.assertEqual(len(dbconfig["tables"]), 1)

    def test_config_name(self):
        db, dbconfig = set_up_database(
            "pgtest",
            "postgres",
            "pgtest_data",
            None,
            "test/integration/config",
        )

        self.assertEqual(db.database_alias, "pgtest")
        self.assertEqual(db.database_type, "postgres")
        self.assertEqual(db._connection_string, "postgresql://user:password@host:1234/test_db")
        self.assertEqual(len(dbconfig["tables"]), 1)

    def test_conn_string(self):
        db, _ = set_up_database(
            "pgtest",
            "postgres",
            None,
            {"connection_string": "fake_connection_string"},
            "test/integration/config",
        )

        self.assertEqual(db._connection_string, "fake_connection_string")

    def test_conn_string_override(self):
        db, _ = set_up_database(
            "pgtest",
            "postgres",
            "pgtest_data",
            {"connection_string": "fake_connection_string"},
            "test/integration/config",
        )

        self.assertEqual(db._connection_string, "fake_connection_string")

    def test_conn_string_env(self):
        db, _ = set_up_database(
            "pgtest",
            "postgres",
            None,
            {"connection_string_env": "FAKE_CONNECTION_STRING_ENV"},
            "test/integration/config",
        )

        self.assertEqual(db._connection_string_env, "FAKE_CONNECTION_STRING_ENV")

    def test_data(self):
        db, _ = set_up_database(
            "pgtest",
            "postgres",
            None,
            {"data": {"foo": "bar", "foobar": "hello"}},
            "test/integration/config",
        )

        self.assertEqual(db._db_template_data, {"foo": "bar", "foobar": "hello"})

    def test_data_add(self):
        db, _ = set_up_database(
            "pgtest",
            "postgres",
            "pgtest_data",
            {"data": {"foo": "bar", "foobar": "hello"}},
            "test/integration/config",
        )

        self.assertEqual(
            db._db_template_data, {"foo": "bar", "foobar": "hello", "bar": "foo"}
        )

    def test_data_override(self):
        db, _ = set_up_database(
            "pgtest",
            "postgres",
            "pgtest_data",
            {"data": {"foo": "bar", "bar": "nope"}},
            "test/integration/config",
        )

        self.assertEqual(db._db_template_data, {"foo": "bar", "bar": "nope"})
