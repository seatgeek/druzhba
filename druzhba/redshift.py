import logging
from contextlib import contextmanager

import psycopg2

from druzhba.config import RedshiftConfig

logger = logging.getLogger("druzhba.redshift")


class Redshift(object):
    """Mixin for Redshift connection configs"""

    def __init__(self, destination_config):
        self.config = destination_config

    @property
    def iam_copy_role(self):
        return self.config.iam_copy_role

    @property
    def s3_config(self):
        return self.config.s3_config

    @contextmanager
    def connection(self):
        redshift_kwargs = self.config.connection_params

        if self.config.redshift_cert_path:
            redshift_kwargs.update(
                {"sslmode": "verify-ca", "sslrootcert": self.config.redshift_cert_path}
            )

        connection = psycopg2.connect(**redshift_kwargs)
        connection.set_client_encoding("utf-8")
        connection.autocommit = True
        try:
            yield connection
        finally:
            connection.close()

    @contextmanager
    def cursor(self, cursor_factory=None):
        with self.connection() as connection:
            cursor = connection.cursor(cursor_factory=cursor_factory)
            try:
                yield cursor
            finally:
                cursor.close()


_redshift = None


def get_redshift():
    return _redshift


def init_redshift(destination_config):
    # TODO: Replace with singleton pattern
    global _redshift  # pylint: disable=global-statement
    _redshift = Redshift(RedshiftConfig(destination_config))
    return _redshift


def generate_copy_query(table_to_copy, copy_target_url, iam_copy_role, manifest_mode):
    query = """
        COPY "{table_to_copy}" FROM '{s3_path}'
        CREDENTIALS 'aws_iam_role={iam_copy_role}'
        {manifest}
        FORMAT AS AVRO 'auto'
        EXPLICIT_IDS ACCEPTINVCHARS TRUNCATECOLUMNS
        COMPUPDATE OFF STATUPDATE OFF;
        """.format(
        table_to_copy=table_to_copy,
        s3_path=copy_target_url,
        iam_copy_role=iam_copy_role,
        manifest="MANIFEST" if manifest_mode else "",
    )
    return query


def generate_rename_query(current_table_name, renamed_table_name):
    return 'ALTER TABLE "{current_table_name}" RENAME TO "{renamed_table_name}";'.format(
        current_table_name=current_table_name, renamed_table_name=renamed_table_name,
    )


def generate_count_query(table):
    return "SELECT COUNT(*) FROM {};".format(table)


def generate_insert_all_query(table_to_select_from, table_to_insert_into):
    return 'INSERT INTO "{table_to_insert_into}" SELECT * FROM "{table_to_select_from}";'.format(
        table_to_select_from=table_to_select_from,
        table_to_insert_into=table_to_insert_into,
    )


def generate_create_table_like_query(new_table_name, table_to_copy):
    return 'CREATE TABLE "{}" (LIKE "{}");'.format(new_table_name, table_to_copy)


def generate_drop_query(table):
    return 'DROP TABLE "{}";'.format(table)


def generate_drop_exists_query(table):
    return 'DROP TABLE IF EXISTS "{}";'.format(table)


def generate_lock_query(table):
    return 'LOCK TABLE "{}";'.format(table)


def create_index_table(index_schema, index_table):
    logger.info(
        "Checking for existence of index table %s.%s", index_schema, index_table
    )
    with get_redshift().cursor() as cur:
        cur.execute(
            """SELECT COUNT(*) = 1
               FROM pg_catalog.pg_class c
               JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = %s
                 AND c.relname = %s
                 AND c.relkind = 'r'    -- only tables
            """,
            (index_schema, index_table),
        )
        # TODO: check format of this table that it's correct maybe?
        if not cur.fetchone()[0]:
            logger.warning(
                "Index table %s.%s does not exist, creating", index_schema, index_table
            )
            cur.execute(
                f"""
                CREATE TABLE {index_schema}.{index_table} (
                    datastore_name VARCHAR(127) NOT NULL,
                    database_name  VARCHAR(127) NOT NULL,
                    table_name     VARCHAR(127) NOT NULL,
                    index_value    VARCHAR(256) NOT NULL,
                    created_ts     TIMESTAMP DEFAULT getdate()
                )
                DISTSTYLE even
                SORTKEY(created_ts)
                ;
            """
            )
            logger.info("Index table %s.%s created", index_schema, index_table)
