from contextlib import contextmanager

import psycopg2

from druzhba.config import RedshiftConfig


class RedshiftConfigMixin(RedshiftConfig):
    """Mixin for Redshift connection configs"""

    @contextmanager
    def connection(self, sslmode='verify-ca'):
        connection = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
            sslmode=sslmode,
            sslrootcert=self.redshift_cert_path
        )
        connection.set_client_encoding('utf-8')
        connection.autocommit = True
        try:
            yield connection
        finally:
            connection.close()

    @contextmanager
    def cursor(self, sslmode='verify-ca', cursor_factory=None):
        with self.connection(sslmode) as connection:
            cursor = connection.cursor(cursor_factory=cursor_factory)
            try:
                yield cursor
            finally:
                cursor.close()


def generate_copy_query(table_to_copy, copy_target_url, iam_copy_role, manifest_mode):
    query = """
        COPY "{table_to_copy}" FROM '{s3_path}'
        CREDENTIALS 'aws_iam_role={iam_copy_role}'
        {manifest}
        FORMAT AS AVRO 'auto'
        EXPLICIT_IDS ACCEPTINVCHARS TRUNCATECOLUMNS
        COMPUPDATE OFF STATUPDATE OFF;
        """.format(table_to_copy=table_to_copy,
                   s3_path=copy_target_url,
                   iam_copy_role=iam_copy_role,
                   manifest='MANIFEST' if manifest_mode else '')
    return query


def generate_rename_query(current_table_name, renamed_table_name):
    return 'ALTER TABLE "{current_table_name}" RENAME TO "{renamed_table_name}";'.format(
        current_table_name=current_table_name,
        renamed_table_name=renamed_table_name
    )


def generate_count_query(table):
    return "SELECT COUNT(*) FROM {};".format(table)


def generate_insert_all_query(table_to_select_from, table_to_insert_into):
    return 'INSERT INTO "{table_to_insert_into}" SELECT * FROM "{table_to_select_from}";'.format(
                table_to_select_from=table_to_select_from,
                table_to_insert_into=table_to_insert_into
            )


def generate_create_table_like_query(new_table_name, table_to_copy):
    return 'CREATE TABLE "{}" (LIKE "{}");'.format(new_table_name, table_to_copy)


def generate_drop_query(table):
    return 'DROP TABLE "{}";'.format(table)


def generate_drop_exists_query(table):
    return 'DROP TABLE IF EXISTS "{}";'.format(table)


def generate_lock_query(table):
    return 'LOCK TABLE "{}";'.format(table)
