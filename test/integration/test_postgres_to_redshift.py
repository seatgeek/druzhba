import datetime
import os
import unittest
from dataclasses import dataclass
from typing import Optional, List

import psycopg2
import psycopg2.extras
from mock import ANY

from druzhba.main import run as run_druzhba


@dataclass
class FakeArgs:
    log_level: Optional[str] = None
    database: Optional[str] = None
    tables: Optional[List[str]] = None
    num_processes: Optional[int] = None
    compile_only: Optional[bool] = None
    print_sql_only: Optional[bool] = None
    validate_only: Optional[bool] = None
    full_refresh: Optional[bool] = None
    rebuild: Optional[bool] = None


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
        cls.source_conn = psycopg2.connect(dsn=os.getenv('PGTEST_DATABASE_URL'))
        cls.source_conn.set_client_encoding('UTF8')
        cls.source_conn.autocommit = True

        cls.target_conn = psycopg2.connect(dsn=os.getenv('REDSHIFT_URL'))
        cls.target_conn.set_client_encoding('UTF8')
        cls.target_conn.autocommit = True

    @classmethod
    def tearDownClass(cls):
        cls.source_conn.close()
        cls.target_conn.close()


class TestBasicIncrementalPipeline(BaseTestPostgresToRedshift):
    args = FakeArgs(
        database='pgtest',
        tables=['test_basic'],
        num_processes=1
    )

    def setUp(self):
        with self.source_conn.cursor() as cur:
            cur.execute("""
            DROP TABLE IF EXISTS test_basic;
            DROP TYPE IF EXISTS enum1;
            CREATE TYPE enum1 AS ENUM ('a', 'b', 'c');
            CREATE TABLE test_basic (
                pk1 INT PRIMARY KEY,
                updated_at1 TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                drop1 VARCHAR(15),
                value1 INT,
                enum_value1 enum1
            );
            """)

    def tearDown(self):
        with self.target_conn.cursor() as cur:
            cur.execute("""
            DROP TABLE IF EXISTS druzhba_test.test_basic;
            DROP TABLE IF EXISTS druzhba_test.pipeline_table_index;
            """)

    def test_run_incremental(self):
        """Runs Druzhba, inserts new data, then runs Druzhba again."""

        t0 = datetime.datetime(2019, 1, 1, 0, 0, 0)
        t1 = t0 + datetime.timedelta(seconds=5)
        t2 = t0 + datetime.timedelta(seconds=10)

        with self.source_conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO test_basic VALUES (%s, %s, %s, %s, %s);",
                [(1, t0, 'value', 0, 'a'), (2, t1, 'value', 1, 'b')]
            )

        run_druzhba(self.args)

        with self.target_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*), MAX(updated_at1), MAX(value1) FROM druzhba_test.test_basic")
            result = cur.fetchall()
            self.assertTupleEqual(result[0], (2, t1, 1))

        with self.source_conn.cursor() as cur:
            cur.execute(
                "UPDATE test_basic SET value1 = 2, updated_at1 = %s WHERE pk1 = 2",
                (t2,)
            )
            cur.execute(
                "INSERT INTO test_basic VALUES (%s, %s, %s, %s, %s);",
                (3, t2, 'drop', 3, 'c')
            )

        run_druzhba(self.args)

        with self.target_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*), MAX(updated_at1), MAX(value1) FROM druzhba_test.test_basic")
            result = cur.fetchall()

            self.assertTupleEqual(result[0], (3, t2, 3))

            cur.execute("SELECT * FROM druzhba_test.test_basic ORDER BY pk1")
            results = cur.fetchall()

            self.assertListEqual(
                results,
                [
                    (1, t0, 0, 'a'),
                    (2, t2, 2, 'b'),
                    (3, t2, 3, 'c'),
                ]
            )

            cur.execute("SELECT * FROM druzhba_test.pipeline_table_index ORDER BY created_ts")
            results = cur.fetchall()

            self.assertListEqual(
                results,
                [
                    ('pgtest', 'druzhba_test', 'test_basic', t1.strftime("%Y-%m-%d %H:%M:%S.%f"), ANY),
                    ('pgtest', 'druzhba_test', 'test_basic', t2.strftime("%Y-%m-%d %H:%M:%S.%f"), ANY),
                ]
            )

    # def test_force_refresh(self):
    #     pass
    #
    # def test_force_rebuild(self):
    #     # Check that it keeps user permissions?
    #     pass
    #
    # def test_ignores_new_source_columns(self):
    #     pass
    #
    # def test_fails_new_target_columns(self):
    #     pass

