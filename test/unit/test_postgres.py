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
            "index_schema",
            "index_table",
            index_column="id",
        )

    def test_init(self):
        table = self.table_config()
        self.assertIsNotNone(table)

    def test_additional_parameters(self):
        more_params_table_config = PostgreSQLTableConfig(
            "db",
            ConnectionParams(
                "name",
                "host",
                "port",
                "user",
                "password",
                {"connect_timeout": "60", "sslmode": "disable"},
            ),
            "table",
            "schema",
            "source",
            "index_schema",
            "index_table",
            index_column="id",
        )
        self.assertIsNotNone(more_params_table_config)
        self.assertEqual(
            more_params_table_config.connection_vars,
            {
                "database": "name",
                "host": "host",
                "port": "port",
                "user": "user",
                "password": "password",
                "connect_timeout": "60",
                "sslmode": "disable",
            },
        )
