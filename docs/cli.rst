.. _cli-help:

Command Line Help
=================

.. include:: _gen/usage.rst

Specific options follow.

Database Options
----------------

By default, Druzhba runs all databases defined in ``_pipeline.yaml`` (see
:ref:`configuration`) unless they are explicitly disabled with ``enabled:
false``. These disabled source databases may still be run by calling Druzhba
with the ``--database`` argument.

Databases are referred to by their "alias", which does not necessarily need to
match the "database name" actually provided in the source connection string.

Additionally, Druzhba supports a ``--table`` option (alternately ``-t`` or
``--tables``). Using this option will cause druzhba to update only a single
table or whitespace delimited list of tables. The ``--table`` option will
override ``enabled: false`` from the relevant configuration file.

Testing Options
---------------

Three testing / development options are available.

The ``--validate-only`` option is useful to run in CI to ensure that
configuration files can be parsed correctly. The Druzhba process will show an
error message return a non-zero exit status if Druzhba detects any configuration
errors. Note that this option does not connect to any databases and therefore
can only catch errors in the Druzhba configuration itself and not issues like a
misspelled table name.

The ``--compile-only`` option will print ``SELECT`` statements that would be run
against the source database. This option can be useful for developing and
debugging custom SQL logic.

The ``--print-sql-only`` option will display all SQL statements that would be
run in would be run against the destination and source databases respectively,
and can be useful for troubleshooting issues in a pipeline.


Table Maintenance and Manual Overrides
--------------------------------------

Using ``--full-refresh`` as a CLI parameter overrides (defaulted to ``false`` if
omitted) ``full-refresh`` table configuration field as described in
``configuration.md``. If given, all tables in the Druzhba invocation will have
all existing rows deleted, and the source side query will ignore the existing
incremental index. This option is typically used with ``-t`` / ``--table`` and
is useful if data in the source table has been deleted or updated in a way that
will not be picked up by an `index_column`.

``--rebuild`` behaves as ``full-refresh``, but additionally will transactionally
delete and recreate the target table. Druzhba also attempts to copy the
permissions on the old table to the new one, but will ignore some kinds of
permissions like ``with grant option``. This option is useful after a migration
to the source table. ``--rebuild`` is not supported for manual tables or those
with a ``truncate_file``.


Production Options
------------------

Default logging level can be overridden with the ``--log-level`` option. Its use
supersedes the ``LOG_LEVEL`` environment variable. If neither is set then
Druzhba defaults to ``INFO``. Available levels include ``DEBUG``, ``INFO``,
``WARN``, and ``ERROR``.

While druzhba processes tables from a given source database sequentially, it will
attempt to process multiple source databases concurrently, one table from each
at a time. By default, Druzhba's parallelism limit will be equal to the number
of CPU cores detected. Use ``-np`` or ``--num-processes`` to override that
default. Note that the number of concurrent processes will never exceed the
number of source databases.
