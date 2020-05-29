Configuration
=============

Druzhba pipelines are defined in YAML files, located in the directory given by the `DRUZHBA_CONFIG_DIR`, laid out like:

.. code-block::

  /<DRUZHBA_CONFIG_DIR>
    - _pipeline.yaml  # defines tables in this pipeline
    - pgtest.yaml
    - mysqltest.yaml
    - etc

Pipeline Configuration
----------------------

An example can be found in the tests at ``/test/integration/config/``.

The top level ``_pipeline.yaml`` defines details about the pipeline as a whole as well as connection
info for the target database. We use a modified parser for YAML extended with support for
interpolation like ``${REDSHIFT_URL}`` which will inject the value of the ``REDSHIFT_URL`` environment
variable.

Besides the target configuration fields, ``_pipeline.yaml`` also includes a list of source tables

.. code-block:: yaml

  connection: <see below>
  sources:
    - alias: pgtest
      type: postgres
    - alias: mysqltest
      type: mysql
      enabled: false

These aliases point to other files in the same directory, e.g. ``pgtest.yaml``.

For an entry in ``sources``:

- ``alias``: the "name" of a single source databsae
- ``type``: indicates which database driver Druzhba should use. ``postgres``, ``mysql``, or ``mssql``.
- ``enabled``: if false, indicates that a Druzhba run without a ``--database`` argument 
  should not include this database. It may still be requested explicitly by passing its alias 
  to ``--database``.

Supported fields for the connection to the target database (currently only Redshift):

- ``connection`` -- options of the target database connection.

  - ``url`` -- if provided, will be parsed into a user/password/host/port/database instead of the seperate items below
  - ``user``
  - ``password``
  - ``host``
  - ``port``
  - ``database``

- ``index``: options for the tracking table in the target database

  - ``schema``
  - ``table``

- ``s3``: used to write data files to S3 and the ``COPY`` into Redshift

  - ``bucket``
  - ``prefix``

- ``iam_copy_role``: IAM role used in the copy operation. Only IAM authorization is supported.
- ``redshift_cert_path``: path to an SSL cert file (optional)

Database Configuration
----------------------

``<db_alias>.yaml`` defines the configuration for a specific source database.

Other top-level keys in this file are:

- ``tables``: a list of tables to pull. Usually the majority of this file. See below.
- ``data``: an arbitrary dictionary of keys and value which will be formatted into SQL queries under the key `db`, for manual pipelines.

  - (SQL Server only) Within this object, the key `object_schema_name` is also used in the database connection.

- ``connection_string``: an explicit connection URI like `protocol://user:pass@host:post/database_name` in most cases.
- ``connection_string_env``: an alternate environment variable for this database's `connection_string`.

If neither `connection_string` or `connection_string_env` is provided, the environment variable `<DB_ALIAS>_DATABASE_URL` is assumed.

Table Configuration
-------------------

The YAML file has several configurable settings for each table.

 - ``source_name``: table name in source database. Required even if `query_file` is used.

Options configuring the creation of the target table for automatic tables.

 - ``destination_name``: desired table name in target database
 - ``destination_schema``: schema in target database. *This schema must already exist*. 
 - ``distribution_key``: Optional. A single column to be used as the table `distkey`. It should be unique or mostly unique. Good examples are primary key `id`s and high-resolution timestamps. You can read more about distkeys [here](http://docs.aws.amazon.com/redshift/latest/dg/c_Distribution_examples.html). NOTE: If no `distribution_key` is specified, the first Primary Key will be selected by default.
 - ``sort_keys``: Optional. Zero or more columns that define the sortkey. Read more about sortkeys [here](http://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-sort-key.html).

Options pertaining to the table's incremental update behavior:

 - ``index_column``: Optional. The column to use for determining which rows are dumped from the source DB. Ideally this is a timestamp (`updated_at` if rows can be edited, `created_at` if the table is append-only) or a sequential numeric ID. In any case, this will go into a where clause like `WHERE col > n AND col <= m`. Please note that if there is no index on this column in the source database, this could affect performance. `n` is pulled from `"public"."pipeline_table_index"`, and `m` is pulled from the source database (`SELECT MAX(index_column) FROM source_table;`) before data export. NOTE: If no `index_column` is specified, the entire table will be dumped (refreshed) on each run of the pipeline.
 - ``index_sql``: Optional (alternative to `index_column`). A SQL expression defining the index column, which should return a single row with column called `index_value`. Jinja is supported.
 - ``primary_key``: Optional. Column name or list of column names to specify as primary keys, if they cannot be inferred from the source table. When loading data, the `primary_key` will be used to replace existing rows instead of inserting new ones. Required for incremental updates based on a `query_file`.
 - ``full_refresh``: Optional. Deletes the entire table prior to loading extracted data. Not compatible with `index_column`, `index_sql`, or `append_only`.
 - ``append_only``: Optional. Simplifies load side by skipping deletes entirely. Requires `index_column` or `index_sql`. Incompatible with `full_refresh`.

Options defining a "manual" table rather than "managed" one.

 - ``query_file``: Optional.  Local path to a file containing valid query SQL. If this is provided, only this query will be used to pull from the source database, no SQL will be auto generated and no datatypes will be inferred.
 - ``schema_file``: Optional. Local path to a file containing valid CREATED TABLE SQL.  If this is provided, only this query will be used to create the destination table in the Data Warehouse.  Without this, the table schema will be generated from the contents of the query.
   Not compatible with the `--rebuild` command line argument.

Column-specific configuration:

 - ``columns_to_drop``: Optional. This is a list of columns that exist in the source table that should *not* exist in the warehoused table. This is the only transformation that currently happens to data.
 - ``type_map``: Optional. Overrides type conversion from the source DB to redshift. This is especially useful for types not supported in Redshift like Enums and Arrays. Ex:

.. code-block:: yaml

  type_map:
    - your_column_name: smallint
    - other_column_name: varchar(70)

Other configuration options:

 - ``truncate_file``: Optional. If using a `query_file` (below), this is required to define deletes from the destination table for a `--full-refresh`. This option is useful
   for many-to-one pipelines, to only delete records in the target that come from the current source. Not compatible with the `--rebuild` command line argument.
 - ``not_null_date``: Optional. If the source table has a `NOT NULL` constraint specified on date/datetime/timestamp columns that do, in fact, have NULL values (or equivalent, a la `0000-00-00`), this option can be used to convert these to `datetime.datetime.min` instead.
 - ``data``: Optional. Object which will be formatted into Jinja templates under the key `table`.


Templating
----------

Custom SQL files can use jinja2 templating. Three variables are defined:

 - ``db`` gets data from the ``data`` block of the database yaml file
 - ``table`` gets data from the ``data`` block of the table configuration
 - ``run`` contains automatically set run metadata with fields:

   - ``destination_schema_name``
   - ``destination_table_name``
   - ``db_name``
   - ``source_table_name``
   - ``old_index_value``
   - ``new_index_value``

In particular ``run.old_index_value`` and ``run.new_index_value`` are useful for building custom incremental update logic.
