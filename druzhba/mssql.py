from contextlib import closing

import pymssql

from druzhba.config import CONFIG_DIR
from druzhba.table import TableConfig, load_query


class MSSQLTableConfig(TableConfig):
    """TableConfig Subclass for MSSQL tables. This class will have all
    methods that are specific to MSSQL, and handle the extracting and
    transforming of the data.

    Attributes
    ----------
    see TableConfig
    """

    database_type = "mssql"
    avro_type_map = {
        # Lint escapes here are because pylint cant introspect what appears to be
        # Java code in pymssql module
        "string": {
            pymssql.STRING.value,  # pylint: disable=no-member
            pymssql.DATETIME.value,  # pylint: disable=no-member
            pymssql.BINARY.value,  # pylint: disable=no-member
        },
        "int": {},  # prefer long to int
        "long": {pymssql.NUMBER.value},  # pylint: disable=no-member
        "double": {},
        "boolean": {},
        "decimal": {pymssql.DECIMAL.value},  # pylint: disable=no-member
    }

    def _load_new_index_value(self):
        raise NotImplementedError(
            "auto generated index queries not yet supported for MSSQL"
        )

    @property
    def connection_vars(self):
        return {
            "server": self.db_host,
            "user": self.db_user,
            "password": self.db_password,
            "database": self.db_name,
            "port": self.db_port,
            "login_timeout": 10,
            "charset": "UTF-8",  # default, but be explicit
        }

    def get_sql_description(self, sql):
        with closing(pymssql.connect(**self.connection_vars)) as conn:
            with closing(conn.cursor(as_dict=True)) as cursor:
                cursor.execute(sql)
                return ((col[0], col[1]) for col in cursor.description)

    def query_to_redshift_create_table(self, sql, table_name):
        if self.schema_file:
            query = load_query(self.schema_file, CONFIG_DIR)
            create_table = query.rstrip("; \n")
            create_table += self.create_table_keys()
            return create_table
        else:
            msg = "auto generated queries not yet supported for MSSQL"
            raise NotImplementedError(msg)

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
        with closing(pymssql.connect(**self.connection_vars)) as conn:
            with closing(conn.cursor(as_dict=True)) as cursor:
                cursor.execute(sql)
                for dict_row in cursor:
                    yield dict_row

    def _get_query_sql(self):
        if self.query_file:
            return self.get_query_from_file()
        else:
            msg = "auto generated queries not yet supported for MSSQL"
            raise NotImplementedError(msg)
