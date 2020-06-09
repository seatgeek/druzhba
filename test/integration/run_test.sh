#!/usr/bin/env bash

set -u

TEST_MYSQL=0
TEST_POSTGRES=0
for i in "$@" ; do
    if [[ ${i} == "mysql" ]] ; then
        TEST_MYSQL=1
    elif [[ ${i} == "postgres" ]] ; then
        TEST_POSTGRES=1
    fi
done

echo "Setting up target database"

REDSHIFT_ADMIN_URL=postgresql://${REDSHIFT_USER}:${REDSHIFT_PASSWORD}@${REDSHIFT_HOST}:${REDSHIFT_PORT:-5439}/${REDSHIFT_DATABASE}
export REDSHIFT_TEST_URL=postgresql://druzhba_test_user:Test12345@${REDSHIFT_HOST}:${REDSHIFT_PORT:-5439}/${REDSHIFT_DATABASE}
psql ${REDSHIFT_ADMIN_URL} -c "CREATE USER druzhba_test_user PASSWORD 'Test12345';"
psql ${REDSHIFT_ADMIN_URL} -c "CREATE SCHEMA druzhba_test AUTHORIZATION druzhba_test_user;"

# Set up source and target databases
if [[ ${TEST_POSTGRES} == 1 ]]; then
    echo "Setting up postgres source database"

    PGTEST_ADMIN_DATABASE_URL=postgresql://postgres:postgres_root_password@druzhba_postgres_1:5432/druzhba_test
    psql ${PGTEST_ADMIN_DATABASE_URL} -c "CREATE USER druzhba_test_user WITH LOGIN PASSWORD 'druzhba_password';"
    psql ${PGTEST_ADMIN_DATABASE_URL} -c "GRANT CREATE, CONNECT ON DATABASE druzhba_test TO druzhba_test_user;"

    echo "Setup complete, running postgres test suite"

    PGTEST_DATABASE_URL=postgresql://druzhba_test_user:druzhba_password@druzhba_postgres_1:5432/druzhba_test \
        nosetests --tests test/integration/test_postgres_to_redshift.py

    echo "Tearing down postgres source database"

    psql ${PGTEST_ADMIN_DATABASE_URL} -c "DROP OWNED BY druzhba_test_user;"
    psql ${PGTEST_ADMIN_DATABASE_URL} -c "REVOKE ALL PRIVILEGES ON DATABASE druzhba_test FROM druzhba_test_user;"
    psql ${PGTEST_ADMIN_DATABASE_URL} -c "DROP USER druzhba_test_user;"
fi

if [[ ${TEST_MYSQL} == 1 ]]; then
    echo "Setting up mysql source database"

    MYSQL_ARGS="-u root --password=mysql_root_password -h druzhba_mysql_1 -P 3306"
    mysql ${MYSQL_ARGS} -Be "CREATE DATABASE druzhba_test;"
    mysql ${MYSQL_ARGS} -Be "CREATE USER druzhba_user IDENTIFIED BY 'druzhba_password';"
    mysql ${MYSQL_ARGS} -Be "GRANT ALL ON druzhba_test.* TO druzhba_user;"
    mysql ${MYSQL_ARGS} -Be "FLUSH PRIVILEGES;"

    echo "Setup complete, running mysql test suite"

    MYSQLTEST_DATABASE_URL=mysql://druzhba_user:druzhba_password@druzhba_mysql_1:3306/druzhba_test \
        nosetests --tests test/integration/test_mysql_to_redshift.py

    echo "Tearing down mysql source database"

    mysql ${MYSQL_ARGS} -Be "DROP USER druzhba_user;"
    mysql ${MYSQL_ARGS} -Be "DROP DATABASE druzhba_test;"
fi

echo "Tearing down target database"

psql ${REDSHIFT_ADMIN_URL} -c "DROP SCHEMA druzhba_test CASCADE;"
psql ${REDSHIFT_ADMIN_URL} -c "DROP USER druzhba_test_user;"
