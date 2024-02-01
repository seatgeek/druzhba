import json
import logging
import re
import unittest
from datetime import datetime
from io import BytesIO

import fastavro
from mock import ANY, MagicMock, Mock, PropertyMock, call, patch

from druzhba.config import RedshiftConfig
from druzhba.db import ConnectionParams
from druzhba.redshift import Redshift, get_redshift
from druzhba.table import (
    ConfigurationError,
    InvalidSchemaError,
    MigrationError,
    Permissions,
    TableConfig,
    TableStateError,
)


class IgnoreWhitespace(str):
    """ Wrap a string so it compares equal to other strings
    except for repeated whitespace characters. """

    r = re.compile(r"(\s+)")

    def __eq__(self, other):
        self_clean = self.r.sub(" ", self)
        other_clean = self.r.sub(" ", other)
        return self_clean == other_clean


class MockBytesIO(object):
    def __init__(self, b):
        self.b = b

    def __enter__(self, *args):
        return self.b

    def __exit__(self, *args):
        pass


mock_conn = ConnectionParams("name", "host", "port", "user", "password")


class EchoDict(object):
    def __getitem__(self, item):
        return item

    def __contains__(self, item):
        return True


class MockS3Config(object):
    bucket = "my-bucket"
    prefix = "my_prefix"


class MockRedshiftConfig(object):
    s3_config = MockS3Config()
    iam_copy_role = "iam_copy_role"
    connection_config = EchoDict()
    redshift_cert_path = "/my/cert/path"


class TableTest(unittest.TestCase):
    def test_validate_config(self):
        # Full refresh should pass
        table_config = {
            "database_alias": "alias",
            "destination_table_name": "table",
            "destination_schema_name": "schema",
            "source_table_name": "source",
            "full_refresh": True,
        }
        # Shouldn't raise anything
        TableConfig.validate_yaml_configuration(table_config)

        # With index column should pass
        table_config = {
            "database_alias": "alias",
            "destination_table_name": "table",
            "destination_schema_name": "schema",
            "source_table_name": "source",
            "index_column": "id",
        }
        TableConfig.validate_yaml_configuration(table_config)

        # With index SQL should pass
        table_config = {
            "database_alias": "alias",
            "destination_table_name": "table",
            "destination_schema_name": "schema",
            "source_table_name": "source",
            "index_sql": "coalesce(id_1, id2)",
        }
        TableConfig.validate_yaml_configuration(table_config)

        # No index and not full refresh should fail.
        with self.assertRaises(ConfigurationError):
            table_config = {
                "database_alias": "alias",
                "destination_table_name": "table",
                "destination_schema_name": "schema",
                "source_table_name": "source",
            }
            TableConfig.validate_yaml_configuration(table_config)

        # Append only without incremental column should fail
        with self.assertRaises(ConfigurationError):
            table_config = {
                "database_alias": "alias",
                "destination_table_name": "table",
                "destination_schema_name": "schema",
                "source_table_name": "source",
                "append_only": True,
            }
            TableConfig.validate_yaml_configuration(table_config)

        # Full refresh and index column should fail
        with self.assertRaises(ConfigurationError):
            table_config = {
                "database_alias": "alias",
                "destination_table_name": "table",
                "destination_schema_name": "schema",
                "source_table_name": "source",
                "full_refresh": True,
                "index_column": "id",
            }
            TableConfig.validate_yaml_configuration(table_config)

    def test_where_clause(self):
        # no index column
        table_config = TableConfig(
            "alias",
            mock_conn,
            "table",
            "schema",
            "source",
            "index_schema",
            "index_table",
            full_refresh=True,
        )
        self.assertEqual(table_config.where_clause(), "")

        # no saved index
        with patch(
            "druzhba.table.TableConfig.new_index_value", new_callable=PropertyMock
        ) as m:
            m.return_value = None
            table_config = TableConfig(
                "alias",
                mock_conn,
                "table",
                "schema",
                "source",
                "index_schema",
                "index_table",
                index_column="id",
            )
            self.assertEqual(table_config.where_clause(), "")

        # new index only
        with patch(
            "druzhba.table.TableConfig.old_index_value", new_callable=PropertyMock
        ) as oiv:
            oiv.return_value = None
            with patch(
                "druzhba.table.TableConfig.new_index_value", new_callable=PropertyMock
            ) as niv:
                niv.return_value = 42
                table_config = TableConfig(
                    "alias",
                    mock_conn,
                    "table",
                    "schema",
                    "source",
                    "index_schema",
                    "index_table",
                    index_column="id",
                )
                self.assertEqual(table_config.where_clause(), "\nWHERE id <= '42'")

        # new and old index
        with patch(
            "druzhba.table.TableConfig.old_index_value", new_callable=PropertyMock
        ) as oiv:
            oiv.return_value = 13
            with patch(
                "druzhba.table.TableConfig.new_index_value", new_callable=PropertyMock
            ) as niv:
                niv.return_value = 42
                table_config = TableConfig(
                    "alias",
                    mock_conn,
                    "table",
                    "schema",
                    "source",
                    "index_schema",
                    "index_table",
                    index_column="id",
                )
                self.assertEqual(
                    table_config.where_clause(), "\nWHERE id > '13' AND id <= '42'"
                )
        
        with patch(
            "druzhba.table.TableConfig.old_index_value", new_callable=PropertyMock
        ) as oiv:
            oiv.return_value = 13
            with patch(
                "druzhba.table.TableConfig.new_index_value", new_callable=PropertyMock
            ) as niv:
                niv.return_value = 42
                with patch(
                    "druzhba.table.TableConfig.lookback_index_value", new_callable=PropertyMock
                ) as liv:
                    liv.return_value = 10
                    table_config = TableConfig(
                        "alias",
                        mock_conn,
                        "table",
                        "schema",
                        "source",
                        "index_schema",
                        "index_table",
                        index_column="id",
                    )
                    self.assertEqual(
                        table_config.where_clause(), "\nWHERE id > '10' AND id <= '42'"
                    )

    def test_full_refresh(self):
        with patch(
            "druzhba.table.TableConfig._load_old_index_value", new_callable=Mock
        ) as loiv:
            loiv.return_value = 13
            # no index column
            table_config = TableConfig(
                "alias",
                mock_conn,
                "table",
                "schema",
                "source",
                "index_schema",
                "index_table",
                index_column="id",
                full_refresh=True,
            )
            self.assertEqual(table_config.old_index_value, None)


class TestTableIndexLogic(unittest.TestCase):
    logging.disable(logging.CRITICAL)

    class MockTable(TableConfig):
        def __init__(self, oiv, niv, append_only=False, full_refresh=False):
            super(TestTableIndexLogic.MockTable, self).__init__(
                "my_db",
                mock_conn,
                "my_table",
                "my_schema",
                "org_table",
                "index_schema",
                "index_table",
            )

            self._old_index_value = oiv
            self._new_index_value = niv
            self.append_only = append_only
            self.full_refresh = full_refresh

            # mocking placeholders
            self.db_name = "my_db"
            self.logger = logging.getLogger("test_logger")

        def connection_vars(self):
            return {}

        @classmethod
        def validate_yaml_configuration(cls, _):
            pass

    def test_check_index_append_only(self):
        tt = self.MockTable("123", 124, append_only=True, full_refresh=False)
        self.assertTrue(tt._check_index_values())

    def test_check_index_full_refresh(self):
        tt = self.MockTable("123", 124, append_only=False, full_refresh=True)
        self.assertTrue(tt._check_index_values())

    def test_check_index_missing_old(self):
        tt = self.MockTable(None, "")
        self.assertTrue(tt._check_index_values())

    def test_check_index_missing_new(self):
        tt = self.MockTable("123", None)
        self.assertFalse(tt._check_index_values())

    def test_int_index_check(self):
        tt = self.MockTable("123", 126)
        self.assertTrue(tt._check_index_values())

        tt = self.MockTable("123", 121)
        self.assertFalse(tt._check_index_values())

    def test_dt_ms_index_check(self):
        dts = datetime(2018, 10, 28, 11, 6, 34)
        dtu = datetime(2018, 10, 28, 11, 6, 34, 543217)

        dss3 = "2018-10-28 11:06:33.980"
        dss4 = "2018-10-28 11:06:34.418"
        dss5 = "2018-10-28 11:06:34.632"

        tt = self.MockTable(dss3, dts)
        self.assertTrue(tt._check_index_values())

        tt = self.MockTable(dss5, dts)
        self.assertFalse(tt._check_index_values())

        tt = self.MockTable(dss4, dtu)
        self.assertTrue(tt._check_index_values())

        tt = self.MockTable(dss5, dtu)
        self.assertFalse(tt._check_index_values())

    def test_dt_us_index_check(self):
        dts = datetime(2018, 10, 28, 11, 6, 34)
        dtu = datetime(2018, 10, 28, 11, 6, 34, 543217)

        dss3 = "2018-10-28 11:06:33.980293"
        dss4 = "2018-10-28 11:06:34.418490"
        dss5 = "2018-10-28 11:06:34.632398"

        tt = self.MockTable(dss3, dts)
        self.assertTrue(tt._check_index_values())

        tt = self.MockTable(dss5, dts)
        self.assertFalse(tt._check_index_values())

        tt = self.MockTable(dss4, dtu)
        self.assertTrue(tt._check_index_values())

        tt = self.MockTable(dss5, dtu)
        self.assertFalse(tt._check_index_values())

    def test_invalid_index_check(self):
        tt = self.MockTable("abcd", 123)
        self.assertFalse(tt._check_index_values())

        # currently unable to handle strings this functionality may change
        tt = self.MockTable("abcd", "abcd")
        self.assertFalse(tt._check_index_values())

        tt = self.MockTable("abcd", set("abc"))
        self.assertFalse(tt._check_index_values())

        tt = self.MockTable("12:23:291 32/13/19", datetime(2018, 10, 28, 11, 6, 34))
        self.assertFalse(tt._check_index_values())


class TestSetLastUpdateIndex(unittest.TestCase):
    class MockTable(TableConfig):
        def __init__(self, niv):
            super(TestSetLastUpdateIndex.MockTable, self).__init__(
                "my_db",
                mock_conn,
                "my_table",
                "my_schema",
                "org_table",
                "index_schema",
                "index_table",
            )

            self._new_index_value = niv

            # mocking placeholders
            self.db_name = "my_db"
            self.logger = logging.getLogger("test_logger")

        def connection_vars(self):
            return {}

        @classmethod
        def validate_yaml_configuration(cls, _):
            pass

    def mock_cursor(self):
        m = Mock()
        cur = Mock()
        m.__enter__ = Mock(return_value=cur)
        m.__exit__ = Mock(return_value=False)
        return m, cur

    @property
    def query(self):
        return """
        INSERT INTO "index_schema"."index_table" VALUES
        (%s, %s, %s, %s)
        """

    @patch("druzhba.redshift._redshift")
    def test_missing_index(self, r):
        r.cursor = Mock()
        tt = self.MockTable(None)
        tt.set_last_updated_index()
        r.cursor.assert_not_called()

    @patch("druzhba.redshift._redshift")
    def test_int_index(self, r):
        m, c = self.mock_cursor()
        r.cursor = Mock(return_value=m)

        tt = self.MockTable(123)
        tt.set_last_updated_index()

        expected_args = ("my_db", "my_db", "org_table", "123")
        c.execute.assert_called_once_with(self.query, expected_args)

    @patch("druzhba.redshift._redshift")
    def test_dt_sec_index(self, r):
        m, c = self.mock_cursor()
        r.cursor = Mock(return_value=m)

        tt = self.MockTable(datetime(2018, 10, 27, 12, 6, 34))
        tt.set_last_updated_index()

        expected_args = ("my_db", "my_db", "org_table", "2018-10-27 12:06:34.000000")
        c.execute.assert_called_once_with(self.query, expected_args)

    @patch("druzhba.redshift._redshift")
    def test_unknown_index_type(self, r):
        m, c = self.mock_cursor()
        r.cursor = Mock(return_value=m)

        tt = self.MockTable([3, ["asdf"]])
        with self.assertRaises(TypeError):
            tt.set_last_updated_index()


class TestUnloadCopy(unittest.TestCase):
    class MockTable(TableConfig):
        def __init__(self):
            super(TestUnloadCopy.MockTable, self).__init__(
                "my_db",
                mock_conn,
                "my_table",
                "my_schema",
                "org_table",
                "index_schema",
                "index_table",
            )
            self._dw_columns = None  # overrides get_destination_table_columns below
            self._columns = None  # overrides columns property
            self._can_create = True
            self._desc_sql = None
            self._results_iter = None
            self._avro_type_map = None
            self.rebuild = False
            self.full_refresh = False
            self.s3 = Mock()
            self.key_name = "key_name"

            self.logger = logging.getLogger("test_logger")

        def get_query_sql(self):
            return "Test SQL"

        def row_generator(self):
            for row in self._results_iter:
                yield row

        def query_to_redshift_create_table(self, sql, table_name):
            if self._can_create:
                return "CREATE TABLE {} ();".format(table_name)
            else:
                raise NotImplementedError

        def get_destination_table_columns(self):
            return self._dw_columns

        def get_sql_description(self, sql):
            return {}, self._desc

        @property
        def avro_type_map(self):
            return self._avro_type_map

        def set_last_updated_index(self):
            pass

        def register_load_monitor(self, *args):
            pass

        def register_extract_monitor(self, *args):
            pass

        def set_search_path(self, *args):
            pass

        @classmethod
        def validate_yaml_configuration(cls, _):
            pass

    def mock_cursor(self):
        m = Mock()
        cur = Mock()
        m.__enter__ = Mock(return_value=cur)
        m.__exit__ = Mock(return_value=False)
        return m, cur

    @patch("druzhba.redshift._redshift")
    def test_create_redshift_table_if_dne(self, r):
        m, c = self.mock_cursor()
        r.cursor = Mock(return_value=m)

        # Can be created
        tt = self.MockTable()
        tt._dw_columns = []
        tt._columns = ["col1", "col2"]
        tt._can_create = True
        tt.check_destination_table_status()
        # Shouldn't raise, but should set this
        self.assertEqual(tt._destination_table_status, tt.DESTINATION_TABLE_DNE)

        # Can't be created
        tt2 = self.MockTable()
        tt2._dw_columns = []
        tt2._columns = ["col1", "col2"]
        tt2._can_create = False
        tt2._table_status = tt.DESTINATION_TABLE_DNE
        with self.assertRaises(MigrationError):
            tt2.check_destination_table_status()

    @patch("druzhba.redshift._redshift")
    def test_dont_create_redshift_table_if_exists(self, r):
        m, c = self.mock_cursor()
        r.cursor = Mock(return_value=m)

        tt = self.MockTable()
        tt._dw_columns = ["col1", "col2"]
        tt._columns = ["col1", "col2"]
        tt.check_destination_table_status()
        # Shouldn't raise
        self.assertEqual(tt._destination_table_status, TableConfig.DESTINATION_TABLE_OK)

        tt2 = self.MockTable()
        tt2._dw_columns = ["col1", "col2"]
        tt2._columns = ["col1", "col2", "col3"]
        tt2.rebuild = True
        tt2._can_create = True
        tt2.check_destination_table_status()
        # We should still rebuild if instructed, even if the table is correct
        self.assertEqual(
            tt2._destination_table_status, TableConfig.DESTINATION_TABLE_REBUILD
        )

    @patch("druzhba.redshift._redshift")
    def test_error_if_destination_table_incorrect(self, r):
        m, c = self.mock_cursor()
        r.cursor = Mock(return_value=m)

        tt = self.MockTable()
        tt._dw_columns = ["col1", "col2", "col3"]
        tt._columns = ["col1", "col2"]
        # Should fail if there are too many dw columns
        with self.assertRaises(InvalidSchemaError):
            tt.check_destination_table_status()

        # Attempt to rebuild and fail
        tt2 = self.MockTable()
        tt2._dw_columns = ["col1", "col2", "col3"]
        tt2._columns = ["col1", "col2"]
        tt2.rebuild = True
        tt2._can_create = False
        with self.assertRaises(MigrationError):
            tt2.check_destination_table_status()

        # Should not raise, but should set rebuild
        tt3 = self.MockTable()
        tt3._dw_columns = ["col1", "col2", "col3"]
        tt3._columns = ["col1", "col2"]
        tt3.rebuild = True
        tt3._can_create = True
        tt3.check_destination_table_status()
        self.assertEqual(tt3._destination_table_status, tt.DESTINATION_TABLE_REBUILD)

    def test_query_description_to_avro(self):
        tt = self.MockTable()
        tt._desc = [["column_1", "db_str("], ["column_2", "db_int"]]
        tt._avro_type_map = {
            "string": {"db_str"},
            "int": {"db_int"},
            "double": {},
            "long": {},
            "boolean": {},
            "decimal": {},
        }
        fields = tt.query_description_to_avro(None)

        target_fields = [
            {"name": "column_1", "type": ["null", "string"]},
            {"name": "column_2", "type": ["null", "int"]},
        ]

        self.assertListEqual(fields, target_fields)

    @patch("druzhba.redshift._redshift", new=Redshift(MockRedshiftConfig()))
    @patch("druzhba.table.BytesIO")
    def test_avro_to_s3(self, mock_io):
        tt = self.MockTable()
        tt.s3 = Mock()
        tt.key_name = None
        tt._desc = [["column_1", "db_str("], ["column_2", "db_int"]]
        tt._avro_type_map = {
            "string": {"db_str"},
            "int": {"db_int"},
            "double": {},
            "long": {},
            "boolean": {},
            "decimal": {},
        }
        tt._results_iter = [
            {"column_1": "first", "column_2": 1},
            {"column_1": "second", "column_2": 2},
            {"column_1": None, "column_2": None},
        ]

        b = BytesIO()
        mock_io.return_value = MockBytesIO(b)
        tt.avro_to_s3(
            tt.row_generator(), tt.query_description_to_avro(tt.get_query_sql())
        )
        b.seek(0)
        for record, target in zip(fastavro.reader(b), tt._results_iter):
            self.assertEqual(record, target)

    @patch("druzhba.redshift._redshift", new=Redshift(MockRedshiftConfig()))
    @patch("druzhba.table.BytesIO")
    def test_extract_full_single(self, mock_io):
        tt = self.MockTable()
        tt.date_key = "20190101T010203"
        tt.s3 = Mock()
        tt.key_name = None
        tt._desc = [["column_1", "db_str("], ["column_2", "db_int"]]
        tt._avro_type_map = {
            "string": {"db_str"},
            "int": {"db_int"},
            "double": {},
            "long": {},
            "boolean": {},
            "decimal": {},
        }
        tt._results_iter = [
            {"column_1": "first", "column_2": 1},
            {"column_1": "second", "column_2": 2},
            {"column_1": None, "column_2": None},
        ]

        tt.write_manifest_file = MagicMock()
        tt._upload_s3 = MagicMock()

        b = BytesIO()
        mock_io.return_value = MockBytesIO(b)
        tt.extract()
        b.seek(0)
        for record, target in zip(fastavro.reader(b), tt._results_iter):
            self.assertEqual(record, target)

        tt.write_manifest_file.assert_not_called()
        tt._upload_s3.assert_called_once_with(
            ANY, "my-bucket", "my_prefix/my_db.org_table.20190101T010203.avro",
        )

        self.assertEqual(tt.row_count, 3)

    @patch("druzhba.redshift._redshift", new=Redshift(MockRedshiftConfig()))
    def test_extract_full_multi(self):
        tt = self.MockTable()
        tt.date_key = "20190101T010203"
        tt.s3 = Mock()
        tt.s3_path, tt.key_name = "s3_path", None
        tt._desc = [["column_1", "db_str("], ["column_2", "db_int"]]
        tt._avro_type_map = {
            "string": {"db_str"},
            "int": {"db_int"},
            "double": {},
            "long": {},
            "boolean": {},
            "decimal": {},
        }
        tt._results_iter = [
            {"column_1": "first", "column_2": 1},
            {"column_1": "second", "column_2": 2},
            {"column_1": None, "column_2": None},
        ] * 100

        # At this size we can fit 146 records so we should end up with 3 data
        # files. If at some point we upgrade avro so the density changes it's
        # ok to tweak some of these settings.
        tt.max_file_size = 1024

        tt.write_manifest_file = MagicMock()
        tt._upload_s3 = MagicMock()

        tt.extract()

        tt.write_manifest_file.assert_called_once()

        # 3 from at three data files -- it would happen four times normally but
        # because we mocked write_manifest_file it does not get called in that
        # method
        self.assertEqual(tt._upload_s3.call_count, 3)
        self.assertEqual(tt.num_data_files, 3)

        # Require row_count set
        self.assertEqual(tt.row_count, 300)

    maxDiff = None

    @patch("druzhba.redshift._redshift", new=Redshift(MockRedshiftConfig()))
    def test_redshift_copy_create(self):
        m, c = self.mock_cursor()

        from druzhba.redshift import _redshift

        _redshift.cursor = Mock(return_value=m)

        tt = self.MockTable()
        tt.primary_key = ["pk"]
        tt.date_key = "20190101T010203"
        tt.full_refresh = True
        tt._can_create = True
        tt._destination_table_status = TableConfig.DESTINATION_TABLE_DNE
        tt.load()

        target_call_args = [
            call("CREATE TABLE my_table ();"),
            call("BEGIN TRANSACTION;"),
            call('LOCK TABLE "my_table";'),
            call('DROP TABLE IF EXISTS "my_db_my_table_staging";'),
            call('CREATE TABLE "my_db_my_table_staging" (LIKE "my_table");'),
            call(
                IgnoreWhitespace(
                    f"""
            COPY "my_db_my_table_staging" FROM 's3://my-bucket/my_prefix/my_db.org_table.20190101T010203.avro'
            CREDENTIALS 'aws_iam_role=iam_copy_role'
            FORMAT AS AVRO 'auto'
            EXPLICIT_IDS ACCEPTINVCHARS TRUNCATECOLUMNS
            COMPUPDATE OFF STATUPDATE OFF;
            """
                )
            ),
            call('DELETE FROM "my_table";'),
            call('INSERT INTO "my_table" SELECT * FROM "my_db_my_table_staging";'),
            call('DROP TABLE "my_db_my_table_staging";'),
            call("END TRANSACTION;"),
        ]

        call_args = c.execute.call_args_list
        self.assertListEqual(call_args, target_call_args)

    @patch("druzhba.redshift._redshift", new=Redshift(MockRedshiftConfig()))
    def test_redshift_copy_incremental_single(self):
        m, c = self.mock_cursor()

        from druzhba.redshift import _redshift

        _redshift.cursor = Mock(return_value=m)

        tt = self.MockTable()
        tt.date_key = "20190101T010203"
        tt.s3, tt.key_name = Mock(), None
        tt.primary_key = ["pk"]
        tt.index_column, tt.full_refresh = "index_col", False
        tt._destination_table_status = TableConfig.DESTINATION_TABLE_OK
        where_clause = (
            'USING "my_db_my_table_staging" WHERE '
            + '"my_db_my_table_staging"."pk" = "my_table"."pk"'
        )
        tt.load()

        target_call_args = [
            call("BEGIN TRANSACTION;"),
            call('LOCK TABLE "my_table";'),
            call('DROP TABLE IF EXISTS "my_db_my_table_staging";'),
            call('CREATE TABLE "my_db_my_table_staging" (LIKE "my_table");'),
            call(
                IgnoreWhitespace(
                    """
            COPY "my_db_my_table_staging" FROM 's3://my-bucket/my_prefix/my_db.org_table.20190101T010203.avro'
            CREDENTIALS 'aws_iam_role=iam_copy_role'
            FORMAT AS AVRO 'auto'
            EXPLICIT_IDS ACCEPTINVCHARS TRUNCATECOLUMNS
            COMPUPDATE OFF STATUPDATE OFF;
            """
                )
            ),
            call('DELETE FROM "my_table" ' + where_clause + ";"),
            call('INSERT INTO "my_table" SELECT * FROM "my_db_my_table_staging";'),
            call('DROP TABLE "my_db_my_table_staging";'),
            call("END TRANSACTION;"),
        ]

        call_args = c.execute.call_args_list
        self.assertListEqual(call_args, target_call_args)

    @patch("druzhba.redshift._redshift")
    def test_redshift_copy_incremental_manifest(self, r):
        m, c = self.mock_cursor()
        r.cursor = Mock(return_value=m)

        tt = self.MockTable()
        tt.date_key = "20190101T010203"
        tt.manifest_mode = True
        tt.s3 = Mock()
        tt.s3.list_objects = Mock(
            return_value=[{"ContentLength": 30}, {"ContentLength": 21},]
        )
        tt.key_name = None
        tt._destination_table_status = tt.DESTINATION_TABLE_OK
        tt.primary_key = ["pk"]
        tt.index_column, tt.full_refresh = "index_col", False
        r.iam_copy_role = "iam_copy_role"
        where_clause = (
            'USING "my_db_my_table_staging" WHERE '
            + '"my_db_my_table_staging"."pk" = "my_table"."pk"'
        )
        tt.load()

        target_call_args = [
            call("BEGIN TRANSACTION;"),
            call('LOCK TABLE "my_table";'),
            call('DROP TABLE IF EXISTS "my_db_my_table_staging";'),
            call('CREATE TABLE "my_db_my_table_staging" (LIKE "my_table");'),
            call(
                IgnoreWhitespace(
                    f"""
            COPY "my_db_my_table_staging" FROM 's3://{get_redshift().s3_config.bucket}/{get_redshift().s3_config.prefix}/my_db.org_table.20190101T010203.manifest'
            CREDENTIALS 'aws_iam_role=iam_copy_role'
            MANIFEST
            FORMAT AS AVRO 'auto'
            EXPLICIT_IDS ACCEPTINVCHARS TRUNCATECOLUMNS
            COMPUPDATE OFF STATUPDATE OFF;
            """
                )
            ),
            call('DELETE FROM "my_table" ' + where_clause + ";"),
            call('INSERT INTO "my_table" SELECT * FROM "my_db_my_table_staging";'),
            call('DROP TABLE "my_db_my_table_staging";'),
            call("END TRANSACTION;"),
        ]

        call_args = c.execute.call_args_list
        self.assertListEqual(call_args, target_call_args)

    @patch("druzhba.redshift._redshift")
    def test_redshift_copy_raises_error_without_pks(self, r):
        m, c = self.mock_cursor()
        r.cursor = Mock(return_value=m)

        tt = self.MockTable()
        tt.primary_key, tt.pks = None, None
        tt.index_column, tt.full_refresh = "index_col", False
        with self.assertRaises(InvalidSchemaError):
            tt.load()

    @patch("druzhba.redshift._redshift", new=Redshift(MockRedshiftConfig()))
    def test_redshift_copy_full_refresh(self):
        m, c = self.mock_cursor()

        from druzhba.redshift import _redshift

        _redshift.cursor = Mock(return_value=m)

        tt = self.MockTable()

        tt.date_key = "20190101T010203"
        tt.s3, tt.key_name = Mock(), None
        tt.pks, tt.index_column = None, None
        tt.full_refresh = True
        tt._destination_table_status = TableConfig.DESTINATION_TABLE_OK
        tt.load()

        target_call_args = [
            call("BEGIN TRANSACTION;"),
            call('LOCK TABLE "my_table";'),
            call('DROP TABLE IF EXISTS "my_db_my_table_staging";'),
            call('CREATE TABLE "my_db_my_table_staging" (LIKE "my_table");'),
            call(
                IgnoreWhitespace(
                    f"""
            COPY "my_db_my_table_staging" FROM 's3://{get_redshift().s3_config.bucket}/{get_redshift().s3_config.prefix}/my_db.org_table.20190101T010203.avro'
            CREDENTIALS 'aws_iam_role=iam_copy_role'
            FORMAT AS AVRO 'auto'
            EXPLICIT_IDS ACCEPTINVCHARS TRUNCATECOLUMNS
            COMPUPDATE OFF STATUPDATE OFF;
            """
                )
            ),
            call('DELETE FROM "my_table";'),
            call('INSERT INTO "my_table" SELECT * FROM "my_db_my_table_staging";'),
            call('DROP TABLE "my_db_my_table_staging";'),
            call("END TRANSACTION;"),
        ]

        call_args = c.execute.call_args_list
        self.assertListEqual(call_args, target_call_args)

    @patch("druzhba.redshift._redshift", new=Redshift(MockRedshiftConfig()))
    @patch("druzhba.table.BytesIO")
    def test_write_manifest_file_invalid(self, _):
        tt = self.MockTable()
        tt.date_key = "20190101T010203"
        tt.s3 = Mock()

        with self.assertRaises(TableStateError):
            tt.write_manifest_file()

    @patch("druzhba.redshift._redshift", new=Redshift(MockRedshiftConfig()))
    @patch("druzhba.table.BytesIO")
    def test_write_manifest_file(self, mock_io):
        expected_entries = [
            {
                "url": f"s3://my-bucket/my_prefix/my_db.org_table.20190101T010203/00000.avro",
                "mandatory": True,
            },
            {
                "url": f"s3://my-bucket/my_prefix/my_db.org_table.20190101T010203/00001.avro",
                "mandatory": True,
            },
            {
                "url": f"s3://my-bucket/my_prefix/my_db.org_table.20190101T010203/00002.avro",
                "mandatory": True,
            },
        ]

        tt = self.MockTable()
        tt.date_key = "20190101T010203"
        tt.manifest_mode = True
        tt.num_data_files = 3

        tt.s3 = Mock()
        tt._upload_s3 = MagicMock()

        b = BytesIO()
        mock_io.return_value = MockBytesIO(b)
        tt.write_manifest_file()
        b.seek(0)

        manifest = json.load(b)
        self.assertListEqual(manifest["entries"], expected_entries)

        tt._upload_s3.assert_called_once_with(
            ANY, "my-bucket", "my_prefix/my_db.org_table.20190101T010203.manifest",
        )

    def test_invalid_manifest_state(self):
        tt = self.MockTable()

        with self.assertRaises(TableStateError):
            tt.manifest_s3_data_key()

        tt.manifest_mode = True
        with self.assertRaises(TableStateError):
            tt.single_s3_data_key()

    @patch("druzhba.redshift._redshift", new=Redshift(MockRedshiftConfig()))
    def test_redshift_copy_full_refresh_with_index_col(self):
        m, c = self.mock_cursor()

        from druzhba.redshift import _redshift

        _redshift.cursor = Mock(return_value=m)

        tt = self.MockTable()
        tt.date_key = "20190101T010203"
        tt._destination_table_status = TableConfig.DESTINATION_TABLE_OK
        tt.pks, tt.index_column = None, "index_col"
        tt.full_refresh = True
        tt.load()

        target_call_args = [
            call("BEGIN TRANSACTION;"),
            call('LOCK TABLE "my_table";'),
            call('DROP TABLE IF EXISTS "my_db_my_table_staging";'),
            call('CREATE TABLE "my_db_my_table_staging" (LIKE "my_table");'),
            call(
                IgnoreWhitespace(
                    f"""
            COPY "my_db_my_table_staging" FROM 's3://{get_redshift().s3_config.bucket}/{get_redshift().s3_config.prefix}/my_db.org_table.20190101T010203.avro'
            CREDENTIALS 'aws_iam_role=iam_copy_role'
            FORMAT AS AVRO 'auto'
            EXPLICIT_IDS ACCEPTINVCHARS TRUNCATECOLUMNS
            COMPUPDATE OFF STATUPDATE OFF;
            """
                )
            ),
            call('DELETE FROM "my_table";'),
            call('INSERT INTO "my_table" SELECT * FROM "my_db_my_table_staging";'),
            call('DROP TABLE "my_db_my_table_staging";'),
            call("END TRANSACTION;"),
        ]

        call_args = c.execute.call_args_list
        self.assertListEqual(call_args, target_call_args)

    @patch("druzhba.redshift._redshift", new=Redshift(MockRedshiftConfig()))
    def test_redshift_copy_rebuild(self):
        m, c = self.mock_cursor()

        from druzhba.redshift import _redshift

        _redshift.cursor = Mock(return_value=m)

        c.fetchall = Mock(return_value=[(True, '{"group group_name=r/owner_name"}')])

        tt = self.MockTable()
        tt.date_key = "20190101T010203"
        tt.rebuild = True
        tt.primary_key = ["pk"]
        tt.full_refresh = True
        tt._can_create = True
        tt._destination_table_status = TableConfig.DESTINATION_TABLE_REBUILD
        tt.load()

        target_call_args = [
            call("BEGIN TRANSACTION;"),
            call('LOCK TABLE "my_table";'),
            call('DROP TABLE IF EXISTS "my_db_my_table_staging";'),
            call(ANY),  # big get permissions query
            call("CREATE TABLE my_db_my_table_staging ();"),
            call("GRANT SELECT ON my_db_my_table_staging TO GROUP group_name;"),
            call(
                IgnoreWhitespace(
                    f"""
            COPY "my_db_my_table_staging" FROM 's3://{get_redshift().s3_config.bucket}/{get_redshift().s3_config.prefix}/my_db.org_table.20190101T010203.avro'
            CREDENTIALS 'aws_iam_role=iam_copy_role'
            FORMAT AS AVRO 'auto'
            EXPLICIT_IDS ACCEPTINVCHARS TRUNCATECOLUMNS
            COMPUPDATE OFF STATUPDATE OFF;
            """
                )
            ),
            call('DROP TABLE "my_table";'),
            call('ALTER TABLE "my_db_my_table_staging" RENAME TO "my_table";'),
            call("SELECT COUNT(*) FROM my_table;"),
            call("END TRANSACTION;"),
        ]

        call_args = c.execute.call_args_list
        self.assertListEqual(call_args, target_call_args)


class TestPermissions(unittest.TestCase):
    def test_parse(self):
        x = "{user1=arwdRxtD/admin,user2=r/admin,user3=a*r*w*d*R*x*t*D*/admin,group group1=r/admin,group group2=r/admin}"
        output = Permissions.parse(x)
        all_grants = [Permissions.char_to_grant[c] for c in "arwdRxtD"]
        expected = [
            Permissions("user1", False, all_grants, "admin"),
            Permissions("user2", False, ["SELECT"], "admin"),
            Permissions("user3", False, all_grants, "admin"),
            Permissions("group1", True, ["SELECT"], "admin"),
            Permissions("group2", True, ["SELECT"], "admin"),
        ]
        self.assertListEqual(output, expected)

    def test_parse_public(self):
        x = "{=r/admin}"
        output = Permissions.parse(x)
        expected = [
            Permissions("PUBLIC", True, ["SELECT"], "admin"),
        ]
        self.assertListEqual(output, expected)

    def test_parse_empty(self):
        x = ""
        output = Permissions.parse(x)
        self.assertEqual(output, [])

    def test_parse_invalid(self):
        x = "something"
        output = Permissions.parse(x)
        self.assertEqual(output, None)

    def test_parse_invalid_permission(self):
        x = "{user4=xyz/admin}"
        output = Permissions.parse(x)
        self.assertIsNone(output)
