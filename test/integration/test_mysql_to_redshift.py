import os
import unittest
from urllib.parse import urlparse

import psycopg2
import pymysql

from druzhba.main import run as run_druzhba
from mock import ANY

from .utils import FakeArgs
from .utils import TimeFixtures as t


class BaseTestMysqlToRedshift(unittest.TestCase):
    """
    A test case consists of:
    - setup a source table
    - setup a specific state of a target table and its pipeline_table_index
    - run druzhba one or more times
    - check the final state of the target table
    """

    @classmethod
    def setUpClass(cls):
        # Run these as admin users?

        parsed = urlparse(os.getenv("MYSQLTEST_DATABASE_URL"))

        cls.source_conn = pymysql.connect(
            db=parsed.path.lstrip("/"),
            host=parsed.hostname,
            port=parsed.port,
            user=parsed.username,
            password=parsed.password,
            charset="utf8",
            autocommit=True,
        )

        cls.target_conn = psycopg2.connect(dsn=os.getenv("REDSHIFT_TEST_URL"))
        cls.target_conn.set_client_encoding("UTF8")
        cls.target_conn.autocommit = True

    @classmethod
    def tearDownClass(cls):
        cls.source_conn.close()
        cls.target_conn.close()


class TestBasicIncrementalPipeline(BaseTestMysqlToRedshift):
    args = FakeArgs(database="mysqltest", tables=["test_basic"], num_processes=1)

    def setUp(self):
        with self.source_conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS test_basic;")
            cur.execute(
                """
                CREATE TABLE `druzhba_test`.`test_basic` (
                  `pk` INT(11) unsigned NOT NULL AUTO_INCREMENT,
                  `updated_at` DATETIME NOT NULL,
                  `drop1` VARCHAR(15),
                  `value1` INT(11),
                  PRIMARY KEY (`pk`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
            """
            )

            cur.executemany(
                "INSERT INTO test_basic VALUES (%s, %s, %s, %s);",
                [(1, t.t0, "value", 0), (2, t.t1, "value", 1)],
            )

    def tearDown(self):
        with self.target_conn.cursor() as cur:
            cur.execute(
                """
                DROP TABLE IF EXISTS druzhba_test.test_basic;
                DROP TABLE IF EXISTS druzhba_test.pipeline_table_index;
            """
            )

    def test_run_incremental(self):
        """Runs Druzhba, inserts new data, then runs Druzhba again."""

        # First run - should create tracking table, target table, and insert values
        # TODO: check constraints are picked up correctly
        run_druzhba(self.args)

        with self.target_conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*), MAX(updated_at), MAX(value1) FROM druzhba_test.test_basic"
            )
            result = cur.fetchall()
            self.assertTupleEqual(result[0], (2, t.t1, 1))

        with self.source_conn.cursor() as cur:
            cur.execute(
                "UPDATE test_basic SET value1 = 2, updated_at = %s WHERE pk = 2",
                (t.t2,),
            )
            cur.execute(
                "INSERT INTO test_basic VALUES (%s, %s, %s, %s);", (3, t.t2, "drop", 3),
            )

        # Second run - should pick up the new row and the updated row
        run_druzhba(self.args)

        with self.target_conn.cursor() as cur:
            cur.execute("SELECT * FROM druzhba_test.test_basic ORDER BY pk")
            results = cur.fetchall()

            self.assertListEqual(results, [(1, t.t0, 0), (2, t.t2, 2), (3, t.t2, 3)])

            cur.execute(
                "SELECT * FROM druzhba_test.pipeline_table_index ORDER BY created_ts"
            )
            results = cur.fetchall()

            self.assertListEqual(
                results,
                [
                    (
                        "mysqltest",
                        "druzhba_test",
                        "test_basic",
                        t.t1.strftime("%Y-%m-%d %H:%M:%S.%f"),
                        ANY,
                    ),
                    (
                        "mysqltest",
                        "druzhba_test",
                        "test_basic",
                        t.t2.strftime("%Y-%m-%d %H:%M:%S.%f"),
                        ANY,
                    ),
                ],
            )


class TestNullDatetime(BaseTestMysqlToRedshift):
    args = FakeArgs(
        database="mysqltest", tables=["test_not_null_datetime"], num_processes=1
    )

    def setUp(self):
        with self.source_conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS test_not_null_datetime;")
            cur.execute("SET sql_mode = '';")
            cur.execute(
                """
                CREATE TABLE `druzhba_test`.`test_not_null_datetime` (
                  `pk` INT(11) unsigned NOT NULL AUTO_INCREMENT,
                  `updated_at` DATETIME NOT NULL,
                  `default_dt` TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00',
                  `no_default_dt` TIMESTAMP NOT NULL,
                  `null_dt` TIMESTAMP,
                  PRIMARY KEY (`pk`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
            """
            )

    def tearDown(self):
        with self.target_conn.cursor() as cur:
            cur.execute(
                """
                DROP TABLE IF EXISTS druzhba_test.test_not_null_datetime;
                DROP TABLE IF EXISTS druzhba_test.pipeline_table_index;
            """
            )

    def test_nullish_datetime(self):
        """
        Runs Druzhba, inserts data with a 0000-.. datetime, then runs Druzhba again.
        """

        with self.source_conn.cursor() as cur:

            cur.executemany(
                "INSERT INTO druzhba_test.test_not_null_datetime VALUES (%s, %s, %s, %s, %s);",
                [(1, t.t0, t.t1, t.t1, t.t1), (2, t.t0, "", "", ""),],
            )

        run_druzhba(self.args)

        with self.target_conn.cursor() as cur:
            cur.execute("SELECT * FROM druzhba_test.test_not_null_datetime ORDER BY pk")
            results = cur.fetchall()

            self.assertListEqual(
                results,
                [(1, t.t0, t.t1, t.t1, t.t1), (2, t.t0, t.tmin, t.tmin, t.tmin),],
            )
