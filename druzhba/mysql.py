import datetime
import re
from contextlib import closing

import pymysql.cursors
from pymysql import FIELD_TYPE as ft

from druzhba.config import CONFIG_DIR
from druzhba.table import TableConfig, load_query


class MysqlTypes:
    mysql_type_to_name = {
        ft.TINY: "TINYINT",
        ft.SHORT: "SMALLINT",
        ft.INT24: "MEDIUMINT",
        ft.LONG: "INT",
        ft.LONGLONG: "BIGINT",
        ft.FLOAT: "FLOAT",
        ft.DOUBLE: "DOUBLE",
        ft.NEWDECIMAL: "DECIMAL",
        ft.TIME: "TIME",
        ft.DATE: "DATE",
        ft.DATETIME: "DATETIME",
        ft.TIMESTAMP: "TIMESTAMP",
        ft.STRING: "CHAR",
        ft.VAR_STRING: "VARCHAR",
        ft.TINY_BLOB: "TINYBLOB",
        ft.BLOB: "BLOB",
        ft.MEDIUM_BLOB: "MEDIUMBLOB",
        ft.LONG_BLOB: "LONGBLOB",
        ft.BIT: "BIT",
        ft.JSON: "BLOB",
    }
    numeric_types_always_promote = ["tinyint", "mediumint", "float", "double"]
    numeric_type_promotions = {
        "tinyint": "smallint",
        "mediumint": "integer",
        "smallint": "integer",
        "int": "bigint",
        "bigint": "numeric(65, 0)",
        "float": "real",
        "double": "double precision",
    }
    fixed_types = ["decimal", "numeric"]
    date_and_time_types = {
        "date": "date",
        "time": "varchar(40)",
        "datetime": "timestamp",
        "timestamp": "timestamp",
        "year": "integer",
    }
    string_types = ["char", "varchar"]
    cmax = 65535


class MySQLTableConfig(TableConfig):
    """TableConfig Subclass for MySQL tables. This class will have all
    methods that are specific to MySQL, and handle the extracting and
    transforming of the data.

    Attributes
    ----------
    (see TableConfig)
    connection_kwargs : dict
        Dict created from environment variables that can be passed
        into MySQLdb or as an environment
    """

    database_type = "mysql"
    avro_type_map = {
        "string": pymysql.connections.TEXT_TYPES.union(
            {
                ft.DATE,
                ft.TIME,
                ft.TIMESTAMP,
                ft.JSON,
                ft.DATETIME,
                ft.ENUM,
                ft.YEAR,
                ft.NULL,
            }
        ),
        "int": {},  # prefer long to int
        "long": {ft.SHORT, ft.INT24, ft.TINY, ft.LONG, ft.LONGLONG},
        "double": {ft.DOUBLE, ft.FLOAT},
        "boolean": set(),
        "decimal": {ft.DECIMAL, ft.NEWDECIMAL},
    }

    @staticmethod
    def get_non_null_datetime_converters():
        """
        Build a substitute for mysql DT conversions which
        can handle "zero values" like '0000-00-00' - these
        are normally are converted to python None as they're
        invalid Datetimes, even if the column is NOT NULL.

        Note: converters are not called at all if a literal
        None is received - it short circuits. See:
        pymysql.connections.MySQLResult._read_row_from_packet

        Returns
        -------
        a dictionary of substitute timestamp conversions
        to pass to the `connect` call.

        """

        def convert_or_default(f, default):
            def _converter(x):
                out = f(x)
                # As of v0.8 pymysql casts "broken" datetimes to strings instead of None
                if out is not None and not isinstance(out, str):
                    return out
                else:
                    return default

            return _converter

        return {
            ft.DATE: convert_or_default(
                f=pymysql.converters.convert_date, default=datetime.date.min
            ),
            ft.DATETIME: convert_or_default(
                f=pymysql.converters.convert_datetime, default=datetime.datetime.min,
            ),
            ft.TIMESTAMP: convert_or_default(
                f=pymysql.converters.convert_mysql_timestamp,
                default=datetime.datetime.min,
            ),
        }

    @property
    def connection_vars(self):
        return {
            "host": self.db_host,
            "user": self.db_user,
            "password": self.db_password,
            "port": self.db_port,
            "db": self.db_name,
            "charset": "utf8",
        }

    def get_sql_description(self, sql):
        with closing(pymysql.connect(**self.connection_vars)) as conn:
            with closing(conn.cursor(pymysql.cursors.SSDictCursor)) as cursor:
                cursor.execute(sql + " LIMIT 1")
                return cursor.description

    def _mysql_to_redshift_type(self, input_type):
        # Note other non-implemented MySQL 8 types:
        #  - Spatial Data Types
        #  - JSON data type
        inp = input_type.lower()

        # Note: for enums, the type name is the same as the column name
        if inp in self.type_map:
            return self.type_map[inp]

        unsigned = "unsigned" in inp
        m = re.search(r"\((\d+)\)", inp)
        if m is not None:
            prec = int(m.group(1))
        else:
            prec = None

        inp = inp.replace("integer", "int")
        type_name = inp.split("(")[0].rstrip()

        # Numeric Types
        if type_name in MysqlTypes.numeric_types_always_promote:
            return MysqlTypes.numeric_type_promotions[type_name]
        elif type_name in MysqlTypes.numeric_type_promotions:
            if not unsigned:
                if type_name == "int":
                    return "integer"
                else:
                    return type_name
            else:
                return MysqlTypes.numeric_type_promotions[type_name]
        elif type_name in MysqlTypes.fixed_types:
            return inp.replace("unsigned", "").rstrip()
        elif type_name == "bit":
            if prec == 1:
                return "boolean"
            else:
                return "varchar({})".format(prec)

        # Date and Time Types
        # don't need precision or scale
        if type_name in MysqlTypes.date_and_time_types.keys():
            return MysqlTypes.date_and_time_types[type_name]

        # String Types
        if type_name in MysqlTypes.string_types:
            out_prec = min(MysqlTypes.cmax, 4 * prec)
            return "varchar({})".format(out_prec)

        # Sensible Defaults
        return "varchar({})".format(MysqlTypes.cmax)

    def _load_new_index_value(self):
        query = "SELECT MAX(`{}`) AS index_value FROM `{}`;".format(
            self.index_column, self.source_table_name
        )
        return self.query_fetchone(query)["index_value"]

    def query_to_redshift_create_table(self, sql, table_name):
        if self.schema_file:
            query = load_query(self.schema_file, CONFIG_DIR)
            create_table = query.rstrip("; \n")
            create_table += self.create_table_keys()
            return create_table
        else:
            desc = self.get_sql_description(sql)
            create_table = """CREATE TABLE "{}"."{}" (\n    """.format(
                self.destination_schema_name, table_name
            )
            field_strs = []
            for (name, type_code, _, _, precision, scale, null_ok,) in desc:
                # Note: mysql overreports this number by up to 4 places, which
                # should't cause problems
                size_str = "({}".format(precision) if precision else ""
                size_str += ",{}".format(scale) if scale else ""
                size_str += ")" if size_str else ""

                sql_type = "{}{}".format(
                    MysqlTypes.mysql_type_to_name[type_code], size_str
                )
                red_type = self._mysql_to_redshift_type(sql_type)
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
        source MySQL database

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

        converters = pymysql.converters.conversions
        if self.not_null_date:
            converters = converters.copy()
            converters.update(self.get_non_null_datetime_converters())

        with closing(pymysql.connect(conv=converters, **self.connection_vars)) as conn:
            with closing(conn.cursor(pymysql.cursors.SSDictCursor)) as cursor:
                cursor.execute(sql)
                for dict_row in cursor:
                    yield dict_row

    def _get_query_sql(self):
        if self.query_file:
            return self.get_query_from_file()

        if not self.pks:
            self.pks = [
                c["Column_name"]
                for c in self.query(
                    """
                SHOW KEYS FROM {} WHERE Key_name = 'PRIMARY'
                """.format(
                        self.source_table_name
                    )
                )
            ]

        cols = [
            c["column_name"]
            for c in self.query(
                """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
                AND table_name='{}'
                AND column_name NOT IN ('{}')
            ORDER BY ordinal_position
            """.format(
                    self.source_table_name, "','".join(self.columns_to_drop)
                )
            )
        ]

        return "SELECT\n    `{}`\nFROM `{}`".format(
            "`\n  , `".join(cols), self.source_table_name
        )
