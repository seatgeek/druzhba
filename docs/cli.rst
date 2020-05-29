
CLI Help
========

.. code-block::

  usage: druzhba [-h] [-ll LOG_LEVEL] [-d DATABASE] [-t [TABLES [TABLES ...]]]
                [-np NUM_PROCESSES] [-co] [-ps] [-vo] [-f] [-r]

  Friendly DB-to-DB pipeline

  optional arguments:
    -h, --help            show this help message and exit
    -ll LOG_LEVEL, --log-level LOG_LEVEL
                          Name of a python log level eg DEBUG
    -d DATABASE, -db DATABASE, --database DATABASE
                          A single database to run. Will override a database
                          marked disabled in the db config file
    -t [TABLES [TABLES ...]], --table [TABLES [TABLES ...]], --tables [TABLES [TABLES ...]]
                          List of tables to run separated by spaces. Must be run
                          with --database
    -np NUM_PROCESSES, --num-processes NUM_PROCESSES
                          Number of parallel processes to spawn. Defaults to
                          number of CPUs (cores) available.
    -co, --compile-only   Will print generated queries to STDOUT but not execute
                          anything.
    -ps, --print-sql-only
                          Will print generated CREATE and SELECT statements to
                          STDOUT only.
    -vo, --validate-only  Will execute configuration checks only.
    -f, --full-refresh    Force a full refresh of the table. Must be run with
                          --database and --table(s).
    -r, --rebuild         Automatically recreate and full-refresh the table.
                          Must be run with --database and --table(s). Only
                          supported for tables Druzhba can build.


Specific Options
================

Database
--------

By default, Druzhba runs all databases defined in `_pipeline.yaml` (see 
[configuration.md](configuration.md)) unless they are explicitly disabled with 
`enabled: false`. These disabled source databases may still be run by calling Druzhba
with the `--database` argument.

Databases are referred to by their "alias", which does not necessarily need to match the "database name" actually provided
in the source connection string.


Testing
-------

Three testing / development options are available.

The `--validate-only` option is useful to run in CI to ensure that configuration files can be parsed correctly. The Druzhba process will show an error message return a non-zero exit status
if Druzhba detects any configuration errors. Note that this option does not connect to any
databases and therefore can only catch errors in the Druzhba configuration itself and not
issues like a misspelled table name.

The `--compile-only` option will print `SELECT` statements that would be run against the
source database. This option can be useful for developing and debugging custom SQL logic.

The `--print-sql-only` option will display all SQL statements that would be run in
would be run against the destination and source databases respectively, and can be useful
for troubleshooting issues in a pipeline.


Table Maintenence and Manual Overrides
--------------------------------------

Using `--full-refresh` as a CLI parameter overrides (defaulted to `false` if ommited) `full-refresh` table configuration field is described in `configuration.md`. If given,
all tables in the Druzhba invocation will have all existing rows deleted, and the source side query will ignore the existing
incremental index. This option is typically used with `-t` / `--table` and is useful if data in the source table has been deleted or updated in a way that will not be
picked up by an `index_column`.

`--rebuild` behaves as `full-refresh`, but additionally will transactionally delete and recreate the target table.
Druzhba also attempts to copy the permissions on the old table to the new one, but will ignore some kinds of
permissions like `with grant option`. This option is useful after a migration to the source table. `--rebuild` is
not supported for manual tables or those with a `truncate_file`.
