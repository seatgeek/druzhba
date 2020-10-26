import unittest

from druzhba.db import ConnectionParams, DatabaseConfig


class DbTest(unittest.TestCase):
    def test_connection_string(self):
        config = DatabaseConfig(
            'test_db',
            'mysql',
            ('postgresql://test_user:test_password@test-db.prod:5439/'
                'test_db_name')
        )

        self.assertIsNotNone(config)

    def test_encoded_connection_string(self):
        config = DatabaseConfig(
            'test_db',
            'mysql',
            ('postgresql://test_user:test_%3Fpassword@test-db.prod:5439/'
                'test_db_name')
        )

        params = config.get_connection_params()
        self.assertIsNotNone(params)
        self.assertEqual(params.password, 'test_?password')
        self.assertEqual(params.additional, {})

    def test_unencoded_connection_string(self):
        """
        '?' in the password will cause the urlparse to fail
        """
        config = DatabaseConfig(
            'test_db',
            'mysql',
            ('postgresql://test_user:test_?password@test-db.prod:5439/'
                'test_db_name')
        )

        self.assertRaises(
            ValueError,
            config.get_connection_params
        )

    def test_additional_parameters(self):
        config = DatabaseConfig(
            'test_db',
            'mysql',
            ('postgresql://test_user:test_password@test-db.prod:5439/'
                'test_db_name?sslmode=disable&connect_timeout=60')
        )

        params = config.get_connection_params()
        self.assertIsNotNone(params)
        self.assertEqual(
            params.additional,
            {'sslmode': 'disable', 'connect_timeout': '60'}
        )


class ConnectionParamsTest(unittest.TestCase):
    def test_bad_connection_params(self):
        self.assertRaises(
            TypeError,
            ConnectionParams,
            "name", "host", "port", "user")