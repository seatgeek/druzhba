#!/bin/sh

# Adds the citext extension to database and test database
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname druzhba_test <<-EOSQL
CREATE EXTENSION IF NOT EXISTS citext;
EOSQL
