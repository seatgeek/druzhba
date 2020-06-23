#!/bin/sh

# Adds the citext extension to database and test database
"${psql[@]}" <<- 'EOSQL'
\c druzhba_test
CREATE EXTENSION IF NOT EXISTS citext;
EOSQL
