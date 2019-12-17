#!/usr/bin/env bash

set -u

# Connection strings used to set up databases
export PGTEST_ADMIN_DATABASE_URL=postgresql://postgres@druzhba_db_1:5432
export REDSHIFT_ADMIN_URL=postgresql://${REDSHIFT_USER}:${REDSHIFT_PASSWORD}@${REDSHIFT_HOST}:${REDSHIFT_PORT:-5439}/${REDSHIFT_DATABASE}

# Set up databases
psql ${PGTEST_ADMIN_DATABASE_URL} -c "CREATE DATABASE druzhba_test;"
psql ${PGTEST_ADMIN_DATABASE_URL} -c "CREATE USER druzhba_test_user WITH LOGIN PASSWORD 'druzhba';"
psql ${PGTEST_ADMIN_DATABASE_URL} -c "GRANT CREATE, CONNECT ON DATABASE druzhba_test TO druzhba_test_user;"

psql ${REDSHIFT_ADMIN_URL} -c "CREATE USER druzhba_test_user PASSWORD 'Test12345!';"
psql ${REDSHIFT_ADMIN_URL} -c "CREATE SCHEMA druzhba_test AUTHORIZATION druzhba_test_user;"

# Should our redshift commands use a special user?

# Connection strings used by Druzhba tests
export PGTEST_DATABASE_URL=postgresql://druzhba_test_user:druzhba@druzhba_db_1:5432/druzhba_test
export REDSHIFT_URL=postgresql://druzhba_test_user:Test12345!@${REDSHIFT_HOST}:${REDSHIFT_PORT:-5439}/${REDSHIFT_DATABASE}

# Integration tests
nosetests --tests test/integration/

# Cleanup
psql ${REDSHIFT_ADMIN_URL} -c "DROP SCHEMA druzhba_test CASCADE;"
psql ${REDSHIFT_ADMIN_URL} -c "DROP USER druzhba_test_user;"
psql ${PGTEST_ADMIN_DATABASE_URL} -c "DROP OWNED BY druzhba_test_user;" druzhba_test
psql ${PGTEST_ADMIN_DATABASE_URL} -c "REVOKE ALL PRIVILEGES ON DATABASE druzhba_test FROM druzhba_test_user;"
psql ${PGTEST_ADMIN_DATABASE_URL} -c "DROP DATABASE druzhba_test;"
psql ${PGTEST_ADMIN_DATABASE_URL} -c "DROP USER druzhba_test_user;"

