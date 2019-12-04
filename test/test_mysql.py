import unittest

from druzhba.db import ConnectionParams
from druzhba.mysql import MySQLTableConfig


class MysqlTest(unittest.TestCase):
    def table_config(self):
        return MySQLTableConfig(
            'db',
            ConnectionParams(
                'name', 'host', 'port', 'user', 'password'
            ),
            'table',
            'schema',
            'source',
            index_column='id'
        )

    def test_type_converstion(self):
        table = self.table_config()

        t = table._mysql_to_redshift_type('TINYINT(1)')
        self.assertEqual(t, 'smallint')

        t = table._mysql_to_redshift_type('BIGINT(1)')
        self.assertEqual(t, 'smallint')

        t = table._mysql_to_redshift_type('TINYINT(2)')
        self.assertEqual(t, 'smallint')

        t = table._mysql_to_redshift_type('SMALLINT(2)')
        self.assertEqual(t, 'integer')

        t = table._mysql_to_redshift_type('MEDIUMINT(3)')
        self.assertEqual(t, 'integer')

        t = table._mysql_to_redshift_type('INT(4)')
        self.assertEqual(t, 'bigint')

        t = table._mysql_to_redshift_type('BIGINT(8)')
        self.assertEqual(t, 'varchar(80)')

        t = table._mysql_to_redshift_type('FLOAT')
        self.assertEqual(t, 'real')

        t = table._mysql_to_redshift_type('DOUBLE')
        self.assertEqual(t, 'double precision')

        t = table._mysql_to_redshift_type('DECIMAL(6, 2)')
        self.assertEqual(t, 'decimal(6, 2)')

        t = table._mysql_to_redshift_type('decimal(8, 4)')
        self.assertEqual(t, 'decimal(8, 4)')

        t = table._mysql_to_redshift_type('VARCHAR(10)')
        self.assertEqual(t, 'varchar(40)')

        t = table._mysql_to_redshift_type('CHAR(10)')
        self.assertEqual(t, 'varchar(40)')

        t = table._mysql_to_redshift_type('tinytext')
        self.assertEqual(t, 'varchar(65535)')

        t = table._mysql_to_redshift_type('mediumtext')
        self.assertEqual(t, 'varchar(65535)')

        t = table._mysql_to_redshift_type('text')
        self.assertEqual(t, 'varchar(65535)')

        t = table._mysql_to_redshift_type('longtext')
        self.assertEqual(t, 'varchar(65535)')

        t = table._mysql_to_redshift_type('tinyblob')
        self.assertEqual(t, 'varchar(65535)')

        t = table._mysql_to_redshift_type('mediumblob')
        self.assertEqual(t, 'varchar(65535)')

        t = table._mysql_to_redshift_type('blob')
        self.assertEqual(t, 'varchar(65535)')

        t = table._mysql_to_redshift_type('longblob')
        self.assertEqual(t, 'varchar(65535)')

        t = table._mysql_to_redshift_type("enum('foo', 'bar')")
        self.assertEqual(t, 'varchar(65535)')

        t = table._mysql_to_redshift_type("datetime")
        self.assertEqual(t, 'timestamp')

        t = table._mysql_to_redshift_type("timestamp")
        self.assertEqual(t, 'timestamp')

        t = table._mysql_to_redshift_type("date")
        self.assertEqual(t, 'date')

        t = table._mysql_to_redshift_type("year")
        self.assertEqual(t, 'varchar(4)')
