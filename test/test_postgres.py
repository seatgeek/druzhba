import unittest

from druzhba.db import ConnectionParams
from druzhba.postgres import PostgreSQLTableConfig


class PostgresTest(unittest.TestCase):
    def table_config(self):
        return PostgreSQLTableConfig(
            "db",
            ConnectionParams("name", "host", "port", "user", "password"),
            "table",
            "schema",
            "source",
            index_column="id",
        )

    def test_init(self):
        table = self.table_config()
        self.assertIsNotNone(table)
