from contextlib import closing

import psycopg2
import psycopg2.extensions
import psycopg2.extras
import logging

from druzhba.config import CONFIG_DIR
from druzhba.table import TableConfig, load_query

psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

logger = logging.getLogger("druzhba.main")

ENCODING = "UTF8"
MAX_DECIMAL_PRECISION = 38
MAX_DECIMAL_SCALE = 37


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
            "citext",
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
            "money": "decimal(19,2)",
        }

        type_map_defaults.update(self.type_map)
        self.type_map = type_map_defaults
        logger.info(self.type_map)

    @property
    def connection_vars(self):
        parameters = {
            "database": self.db_name,
            "host": self.db_host,
            "port": self.db_port,
            "user": self.db_user,
            "password": self.db_password,
        }

        if self.db_additional_parameters:
            parameters.update(self.db_additional_parameters)

        return parameters

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

    def _get_table_attributes(self):
        query = """

        SELECT pg_catalog.obj_description('{0}'::regclass, 'pg_class') AS comment;

        """.format(
            self.source_table_name
        )
        rows = [row for row in self.query(query)]
        assert (
            len(rows) == 1
        ), "Expected one row to be returned when retrieving table attributes."
        return {"comment": rows[0]["comment"]}

    def _get_column_attributes(self):
        query = """

        SELECT
            column_name
          , (is_nullable = 'YES') AS is_nullable
          , pg_catalog.col_description('{0}'::regclass, ordinal_position::INT) AS comment
        FROM information_schema.columns
        WHERE table_name = '{0}';

        """.format(
            self.source_table_name
        )

        return {
            row["column_name"]: {
                "is_nullable": row["is_nullable"],
                "comment": row["comment"],
            }
            for row in self.query(query)
        }

    def _load_new_index_value(self):
        query = 'SELECT MAX("{}") AS index_value FROM "{}";'.format(
            self.index_column, self.source_table_name
        )
        return self.query_fetchone(query)["index_value"]

    def get_sql_description(self, sql):
        if self.query_file is None:
            table_attributes = self._get_table_attributes()
            column_attributes = self._get_column_attributes()
        else:
            table_attributes = {}
            column_attributes = {}

        with closing(psycopg2.connect(**self.connection_vars)) as conn:
            conn.set_client_encoding(ENCODING)
            with closing(
                conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            ) as cursor:
                cursor.execute(sql.rstrip("; \n") + " LIMIT 1")
                return (
                    table_attributes,
                    (
                        (
                            col.name,
                            self.type_map.get(
                                self.pg_types[col.type_code],
                                self.pg_types[col.type_code],
                            ),
                            None,
                            col.internal_size,
                            col.precision,
                            col.scale,
                            column_attributes.get(col.name, {}).get("is_nullable"),
                            column_attributes.get(col.name, {}).get("comment"),
                        )
                        for col in cursor.description
                    ),
                )

    def query_to_redshift_create_table(self, sql, table_name):
        if self.schema_file:
            create_table = load_query(self.schema_file, CONFIG_DIR).rstrip("; \n\t")
            create_table += self.create_table_keys()
            create_table += ";\n"
            # TODO: add support for table and column comments in yaml config file.
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

            table_attributes, columns = self.get_sql_description(sql)
            create_table = """CREATE TABLE "{}"."{}" (\n    """.format(
                self.destination_schema_name, table_name
            )
            field_strs, comments = [], []
            table_comment = table_attributes.get("comment")
            if self.include_comments and table_comment is not None:
                comments.append(
                    """COMMENT ON TABLE "{}"."{}" IS '{}'""".format(
                        self.destination_schema_name,
                        table_name,
                        table_comment.replace(
                            "'", "''"
                        ),  # escape single quotes in the creation statement
                    )
                )
            for (
                name,
                type_code,
                _,
                internal_size,
                precision,
                scale,
                null_ok,
                comment,
            ) in columns:
                size_str = self._get_column_size(type_code, internal_size, precision, scale)

                red_type = self._format_red_type(self.type_map.get(type_code, type_code), size_str, name)
                logger.info("type for column with name %s is %s with size %s from type map default %s", name, red_type, size_str, self.type_map.get(type_code, type_code))

                field_strs.append(
                    '"{name}" {type}{null_ok}'.format(
                        name=name,
                        type=red_type,
                        null_ok="" if null_ok else " NOT NULL",
                    )
                )
                if self.include_comments and comment is not None:
                    comments.append(
                        """COMMENT ON COLUMN "{}"."{}"."{}" IS '{}';""".format(
                            self.destination_schema_name,
                            table_name,
                            name,
                            comment.replace(
                                "'", "''"
                            ),  # escape single quotes in the creation statement
                        )
                    )

            create_table += "\n  , ".join(field_strs)
            create_table += "\n)\n"
            create_table += self.create_table_keys()
            create_table += ";\n"
            create_table += ";\n".join(comments)
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

    def _get_column_size(self, column_type, internal_size, precision, scale):
        if self.type_map.get(column_type, column_type).lower() in ('decimal', 'numeric'):
            precision = min(int(precision), MAX_DECIMAL_PRECISION)
            scale = min(int(scale), MAX_DECIMAL_SCALE)

        size_str = "({}".format(precision) if precision else ""
        size_str += ",{}".format(scale) if scale else ""
        size_str += ")" if size_str else ""
        if self.type_map.get(column_type, column_type) == "varchar":
            length = internal_size if internal_size > 0 else "max"
            size_str = "({})".format(length)

        return size_str

    def _format_red_type(self, type_name, size_str, name):
        if name in self.type_map:
            return self.type_map[name]
        else:
            return "{}{}".format(
                    type_name, size_str
                )

    def _format_column_query(self, column_name, data_type):
        # PostgreSQL's MONEY type is a bit strange. It's an 8-byte fixed fractional precision value
        # with the precision controlled by the value of lc_monetary (a locale setting).
        # Redshift doesn't support MONEY and treats it as a string (expecting the $123.45 format),
        # but it's also safe and lossless to cast to decimal/numeric in order to keep the value as
        # a number, which is the approach we take here. The default precision and scale as defined
        # in default_type_map above reflect the range of values in the en_US locale but can be
        # overridden using the usual type_map mechanism.
        # See: https://www.postgresql.org/docs/10/datatype-money.html
        if data_type.lower() == 'money':
            return '"{}"::{}'.format(column_name, self.type_map['money'])

        # In the typical case we simply query the column by name, quoting for good measure.
        return '"{}"'.format(column_name)

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
                    JOIN pg_attribute a
                        ON a.attrelid = i.indrelid
                        AND a.attnum::TEXT = ANY(STRING_TO_ARRAY(TEXTIN(INT2VECTOROUT(i.indkey)), ' '))
                WHERE i.indrelid = '{}'::REGCLASS
                    AND i.indisprimary;
                """.format(
                        self.source_table_name
                    )
                )
            ]

        columns = [
            self._format_column_query(c['column_name'], c['data_type'])
            for c in self.query(
                """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = CURRENT_SCHEMA()
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
