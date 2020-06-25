from contextlib import closing

import psycopg2
import psycopg2.extensions
import psycopg2.extras

from druzhba.config import CONFIG_DIR
from druzhba.table import TableConfig, load_query

psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

ENCODING = "UTF8"


class PostgreSQLTableConfig(TableConfig):
    """TableConfig Subclass for PostgreSQL tables. This class will have all
    methods that are specific to Postgres, and handle the extracting and
    transforming of the data.

    Attributes
    ----------
    see TableConfig
    """

    database_type = "postgresql"
    avro_type_map = {
        "string": {
            "xml",
            "char",
            "text",
            "bytea",
            "name",
            "json",
            "jsonb",
            "varchar",
            "timestamp",
            "date",
            "time",
            "timestampz",
            "citext"
        },
        "int": {},  # prefer long to int
        "long": {"int2", "int4", "oid", "int8", "serial8"},
        "double": {"float4", "float8"},
        "boolean": {"bool"},
        "decimal": {"decimal", "numeric", "money"},
    }
    _pg_types = None

    def __init__(self, *args, **kwargs):
        super(PostgreSQLTableConfig, self).__init__(*args, **kwargs)
        type_map_defaults = {
            "text": "varchar(max)",
            "citext": "varchar(max)",
            "jsonb": "varchar(max)",
            "json": "varchar(max)",
            "array": "varchar(max)",
            "uuid": "char(36)",
        }

        type_map_defaults.update(self.type_map)
        self.type_map = type_map_defaults

    @property
    def connection_vars(self):
        return {
            "database": self.db_name,
            "host": self.db_host,
            "port": self.db_port,
            "user": self.db_user,
            "password": self.db_password,
        }

    @property
    def pg_types(self):
        if self._pg_types is None:
            with closing(psycopg2.connect(**self.connection_vars)) as conn:
                conn.set_client_encoding(ENCODING)
                with closing(
                    conn.cursor(
                        cursor_factory=psycopg2.extras.DictCursor, name="druzhba"
                    )
                ) as cursor:
                    cursor.execute("select oid, typname from pg_type")
                    self._pg_types = dict(
                        [(t["oid"], t["typname"]) for t in cursor.fetchall()]
                    )
        return self._pg_types

    def _get_column_is_nullable(self):
        is_nullable_query = """

        SELECT column_name, (is_nullable = 'YES') AS is_nullable
        FROM information_schema.columns
        WHERE table_name = '{0}';

        """.format(
            self.source_table_name
        )

        return {
            row["column_name"]: row["is_nullable"]
            for row in self.query(is_nullable_query)
        }

    def _load_new_index_value(self):
        query = 'SELECT MAX("{}") AS index_value FROM "{}";'.format(
            self.index_column, self.source_table_name
        )
        return self.query_fetchone(query)["index_value"]

    def get_sql_description(self, sql):
        if self.query_file is None:
            column_is_nullable = self._get_column_is_nullable()
        else:
            column_is_nullable = {}

        with closing(psycopg2.connect(**self.connection_vars)) as conn:
            conn.set_client_encoding(ENCODING)
            with closing(
                conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            ) as cursor:
                cursor.execute(sql.rstrip("; \n") + " LIMIT 1")
                return (
                    (
                        col.name,
                        self.type_map.get(
                            self.pg_types[col.type_code], self.pg_types[col.type_code],
                        ),
                        None,
                        col.internal_size,
                        col.precision,
                        col.scale,
                        column_is_nullable.get(col.name),
                    )
                    for col in cursor.description
                )

    def query_to_redshift_create_table(self, sql, table_name):
        if self.schema_file:
            create_table = load_query(self.schema_file, CONFIG_DIR).rstrip("; \n\t")
            create_table += self.create_table_keys()
            return create_table
        else:
            if self.query_file is not None:
                self.logger.warning(
                    (
                        "Cannot obtain `null_ok` attribute for columns in postgres "
                        'source necessary to create target table "%s", assuming '
                        "that all columns should be NOT NULL. Please manually "
                        "rebuild the target table if some columns should be "
                        "nullable."
                    ),
                    table_name,
                )

            desc = self.get_sql_description(sql)
            create_table = """CREATE TABLE "{}"."{}" (\n    """.format(
                self.destination_schema_name, table_name
            )
            field_strs = []
            for (name, type_code, _, internal_size, precision, scale, null_ok,) in desc:
                size_str = "({}".format(precision) if precision else ""
                size_str += ",{}".format(scale) if scale else ""
                size_str += ")" if size_str else ""
                if self.type_map.get(type_code, type_code) == "varchar":
                    length = internal_size if internal_size > 0 else "max"
                    size_str = "({})".format(length)

                red_type = "{}{}".format(
                    self.type_map.get(type_code, type_code), size_str
                )
                field_strs.append(
                    '"{name}" {type}{null_ok}'.format(
                        name=name,
                        type=red_type,
                        null_ok="" if null_ok else " NOT NULL",
                    )
                )

            create_table += "\n  , ".join(field_strs)
            create_table += "\n)\n"
            create_table += self.create_table_keys()
            return create_table

    def query(self, sql):
        """Handles the opening and closing of connections for querying the
        source database

        Parameters
        ----------
        sql : str
            preformatted SQL query

        Returns
        -------
        list of dicts (generator)
            Returns the full database result. If a query returns no
            results, this returns an empty list.
        """

        self.logger.debug("Running query: %s", sql)

        cursor_name = "{}_{}".format(self.source_table_name, self.date_key)
        with closing(psycopg2.connect(**self.connection_vars)) as conn:
            conn.set_client_encoding(ENCODING)

            with conn.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor, name=cursor_name
            ) as cursor:
                cursor.execute(sql)
                for dict_row in cursor:
                    yield dict_row

    def _get_query_sql(self):
        if self.query_file:
            return self.get_query_from_file()

        if not self.pks:
            self.pks = [
                c["attname"]
                for c in self.query(
                    """
                SELECT a.attname
                FROM pg_index i
                    JOIN pg_attribute a ON a.attrelid = i.indrelid
                                        AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = '{}'::regclass
                    AND i.indisprimary;
                """.format(
                        self.source_table_name
                    )
                )
            ]

        columns = [
            '"{}"'.format(c["column_name"])
            for c in self.query(
                """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = CURRENT_SCHEMA
                AND "table_name"= '{}'
                AND column_name NOT IN ('{}')
            ORDER BY ordinal_position
            """.format(
                    self.source_table_name, "','".join(self.columns_to_drop)
                )
            )
        ]

        return 'SELECT\n    {}\nFROM "{}"'.format(
            "\n  , ".join(columns), self.source_table_name
        )
