import os
import unittest
from dataclasses import replace as dataclass_replace

import psycopg2
import psycopg2.extras

from druzhba.main import run as run_druzhba
from mock import ANY

from .utils import FakeArgs
from .utils import TimeFixtures as t


class BaseTestPostgresToRedshift(unittest.TestCase):
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
        cls.source_conn = psycopg2.connect(dsn=os.getenv("PGTEST_DATABASE_URL"))
        cls.source_conn.set_client_encoding("UTF8")
        cls.source_conn.autocommit = True

        cls.target_conn = psycopg2.connect(dsn=os.getenv("REDSHIFT_TEST_URL"))
        cls.target_conn.set_client_encoding("UTF8")
        cls.target_conn.autocommit = True

    @classmethod
    def tearDownClass(cls):
        cls.source_conn.close()
        cls.target_conn.close()


class TestBasicIncrementalPipeline(BaseTestPostgresToRedshift):
    args = FakeArgs(database="pgtest", tables=["test_basic"], num_processes=1)

    def setUp(self):
        with self.source_conn.cursor() as cur:
            cur.execute(
                """
            DROP TABLE IF EXISTS test_basic;
            DROP TYPE IF EXISTS enum1;
            CREATE TYPE enum1 AS ENUM ('a', 'b', 'c');
            CREATE TABLE test_basic (
                pk INT PRIMARY KEY,
                updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                drop1 VARCHAR(15),
                value1 INT,
                enum_value enum1
            );
            """
            )

            cur.executemany(
                "INSERT INTO test_basic VALUES (%s, %s, %s, %s, %s);",
                [(1, t.t0, "value", 0, "a"), (2, t.t1, "value", 1, "b")],
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
                "INSERT INTO test_basic VALUES (%s, %s, %s, %s, %s);",
                (3, t.t2, "drop", 3, "c"),
            )

        # Second run - should pick up the new row and the updated row
        run_druzhba(self.args)

        with self.target_conn.cursor() as cur:
            cur.execute("SELECT * FROM druzhba_test.test_basic ORDER BY pk")
            results = cur.fetchall()

            self.assertListEqual(
                results, [(1, t.t0, 0, "a"), (2, t.t2, 2, "b"), (3, t.t2, 3, "c")]
            )

            cur.execute(
                "SELECT * FROM druzhba_test.pipeline_table_index ORDER BY created_ts"
            )
            results = cur.fetchall()

            self.assertListEqual(
                results,
                [
                    (
                        "pgtest",
                        "druzhba_test",
                        "test_basic",
                        t.t1.strftime("%Y-%m-%d %H:%M:%S.%f"),
                        ANY,
                    ),
                    (
                        "pgtest",
                        "druzhba_test",
                        "test_basic",
                        t.t2.strftime("%Y-%m-%d %H:%M:%S.%f"),
                        ANY,
                    ),
                ],
            )

    def test_force_refresh(self):
        # First run - should create tracking table, target table, and insert values
        run_druzhba(self.args)

        with self.source_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO test_basic VALUES (%s, %s, %s, %s, %s);",
                (3, t.t2, "drop", 3, "c"),
            )
            cur.execute("DELETE FROM test_basic WHERE pk = %s", (1,))

        # Second run - should pick up delete and insert
        run_druzhba(dataclass_replace(self.args, full_refresh=True))

        with self.target_conn.cursor() as cur:
            cur.execute("SELECT * FROM druzhba_test.test_basic ORDER BY pk")
            results = cur.fetchall()

            self.assertListEqual(results, [(2, t.t1, 1, "b"), (3, t.t2, 3, "c")])

            cur.execute(
                "SELECT * FROM druzhba_test.pipeline_table_index ORDER BY created_ts"
            )
            results = cur.fetchall()

            self.assertListEqual(
                results,
                [
                    (
                        "pgtest",
                        "druzhba_test",
                        "test_basic",
                        t.t1.strftime("%Y-%m-%d %H:%M:%S.%f"),
                        ANY,
                    ),
                    (
                        "pgtest",
                        "druzhba_test",
                        "test_basic",
                        t.t2.strftime("%Y-%m-%d %H:%M:%S.%f"),
                        ANY,
                    ),
                ],
            )

        with self.source_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO test_basic VALUES (%s, %s, %s, %s, %s);",
                (1, t.t0, "value", 0, "a"),
            )
            cur.execute(
                "UPDATE test_basic SET value1 = 2, updated_at = %s WHERE pk = 2",
                (t.t3,),
            )

        # Third run: incremental updates should proceed from second run index value,
        # so re-insert of value 1 at original `updated_at` is not picked up while new update is.
        run_druzhba(self.args)

        with self.target_conn.cursor() as cur:
            cur.execute("SELECT * FROM druzhba_test.test_basic ORDER BY pk")
            results = cur.fetchall()

            self.assertListEqual(results, [(2, t.t3, 2, "b"), (3, t.t2, 3, "c")])

            cur.execute(
                "SELECT * FROM druzhba_test.pipeline_table_index ORDER BY created_ts"
            )
            results = cur.fetchall()

            self.assertListEqual(
                results,
                [
                    (
                        "pgtest",
                        "druzhba_test",
                        "test_basic",
                        t.t1.strftime("%Y-%m-%d %H:%M:%S.%f"),
                        ANY,
                    ),
                    (
                        "pgtest",
                        "druzhba_test",
                        "test_basic",
                        t.t2.strftime("%Y-%m-%d %H:%M:%S.%f"),
                        ANY,
                    ),
                    (
                        "pgtest",
                        "druzhba_test",
                        "test_basic",
                        t.t3.strftime("%Y-%m-%d %H:%M:%S.%f"),
                        ANY,
                    ),
                ],
            )

    def test_new_source_column_and_force_rebuild(self):
        # First run - should create tracking table, target table, and insert values
        run_druzhba(self.args)

        with self.source_conn.cursor() as cur:
            cur.execute(
                "ALTER TABLE test_basic ADD COLUMN new_value VARCHAR(63) DEFAULT 'default'"
            )
            cur.execute(
                "INSERT INTO test_basic VALUES (%s, %s, %s, %s, %s, %s);",
                (3, t.t2, "drop", 3, "c", "other"),
            )

        # Second run - should not fail despite the new column. Should pick up new row.
        run_druzhba(self.args)
        with self.target_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM druzhba_test.test_basic")
            self.assertEqual(cur.fetchone()[0], 3)

        # Third run -  should recreate target table with the new column and fully refresh
        # TODO: Check user permissions are maintained
        run_druzhba(dataclass_replace(self.args, rebuild=True))

        with self.target_conn.cursor() as cur:
            cur.execute("SELECT * FROM druzhba_test.test_basic ORDER BY pk")
            results = cur.fetchall()

            self.assertListEqual(
                results,
                [
                    (1, t.t0, 0, "a", "default"),
                    (2, t.t1, 1, "b", "default"),
                    (3, t.t2, 3, "c", "other"),
                ],
            )

            cur.execute(
                "SELECT * FROM druzhba_test.pipeline_table_index ORDER BY created_ts"
            )
            results = cur.fetchall()

            self.assertListEqual(
                results,
                [
                    (
                        "pgtest",
                        "druzhba_test",
                        "test_basic",
                        t.t1.strftime("%Y-%m-%d %H:%M:%S.%f"),
                        ANY,
                    ),
                    (
                        "pgtest",
                        "druzhba_test",
                        "test_basic",
                        t.t2.strftime("%Y-%m-%d %H:%M:%S.%f"),
                        ANY,
                    ),
                    (
                        "pgtest",
                        "druzhba_test",
                        "test_basic",
                        t.t2.strftime("%Y-%m-%d %H:%M:%S.%f"),
                        ANY,
                    ),
                ],
            )

    def test_skips_table_on_extra_target_column(self):
        # First run - should create tracking table, target table, and insert values
        run_druzhba(self.args)

        with self.source_conn.cursor() as cur:
            cur.execute("ALTER TABLE test_basic DROP COLUMN enum_value;")
            cur.execute(
                "INSERT INTO test_basic VALUES (%s, %s, %s, %s);", (3, t.t2, "drop", 3)
            )

        # Second run - should see a discrepancy between the source/target, skip the table,
        # and only omit an error log rather than raise an exception.
        run_druzhba(self.args)

        with self.target_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM druzhba_test.test_basic")
            self.assertEqual(cur.fetchone()[0], 2)

            cur.execute("SELECT COUNT(*) FROM druzhba_test.pipeline_table_index")
            self.assertEqual(cur.fetchone()[0], 1)
