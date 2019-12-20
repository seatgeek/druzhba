import os
from collections import namedtuple
from urllib.parse import urlparse

import psycopg2
import pymssql
import pymysql

from druzhba.mssql import MSSQLTableConfig
from druzhba.mysql import MySQLTableConfig
from druzhba.postgres import PostgreSQLTableConfig

ConnectionParams = namedtuple(
    "ConnectionParams", ["name", "host", "port", "user", "password"]
)


class DatabaseConfig(object):
    """Defines the configuration of a database.

    Should contain everything necessary to connect to a database.

    If neither 'connection_string' or 'connection_string_env' is given,
    connection parameters will be retrieved from the envar:

        '{DATABASE_ALIAS}_DATABASE_URL'

    Parameters
    ----------
    database_alias : str
        Colloquial name of the source database. Should match associated
        config files & env vars
    database_type : {'mysql', 'postgres'}
        Determines the engine of the source database
    connection_string : str, optional
        database connection string
    connection_string_env : str, optional
        name of database connection environment variable.
    object_schema_name : str, optional
        mssql object schema name
    db_template_data : dict, optional
        parameters to interpolate into SQL queries, if this is
        a SQL-defined table.

    Attributes
    ----------
    database_alias : str
        set by database_alias parameter
    database_type : str
        set by database_type parameter
    db_errors
        python namespace for appropriate DBAPI errors
    """

    def __init__(
        self,
        database_alias,
        database_type,
        connection_string=None,
        connection_string_env=None,
        object_schema_name=None,
        db_template_data=None,
    ):
        self.database_alias = database_alias
        self.database_type = database_type

        self._connection_string = connection_string
        self._connection_string_env = connection_string_env
        self._object_schema_name = object_schema_name
        self._db_template_data = db_template_data

        if self.database_type == "mysql":
            self._table_conf_cls = MySQLTableConfig
            self.db_errors = pymysql.err
        elif self.database_type == "postgres":
            self._table_conf_cls = PostgreSQLTableConfig
            self.db_errors = psycopg2
        elif self.database_type == "mssql":
            self._table_conf_cls = MSSQLTableConfig
            self.db_errors = pymssql
        else:
            msg = "Unknown database type {}".format(self.database_type)
            raise ValueError(msg)

    def get_table_config(self, table_params, index_schema, index_table):
        return self._table_conf_cls(
            self.database_alias,
            self.get_connection_params(),
            db_template_data=self._db_template_data,
            index_schema=index_schema,
            index_table=index_table,
            **table_params
        )

    def get_connection_params(self):
        if self._connection_string:
            db_url = self._connection_string
        else:
            if self._connection_string_env:
                env_path = self._connection_string_env
            else:
                env_path = self.database_alias.upper() + "_DATABASE_URL"
            db_url = os.getenv(env_path)
            if db_url is None:
                raise RuntimeError(
                    "Environment variable {} was not set".format(env_path)
                )

        parsed = urlparse(db_url)

        return ConnectionParams(
            name=self._object_schema_name or parsed.path.lstrip("/"),
            host=parsed.hostname,
            port=parsed.port,
            user=parsed.username,
            password=parsed.password,
        )
