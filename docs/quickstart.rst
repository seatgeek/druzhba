Tutorial
========

In this tutorial we'll work through the basic steps of setting up a minimal
Druzhba pipeline.

Installing Druzhba
------------------

Install locally in a Python3 virtual environment or wherever you like:

.. code-block:: bash

  pip install druzhba

Or clone the source code and install from there

.. code-block:: bash

  git clone git@github.com:seatgeek/druzhba.git
  pip install -e .


Define your pipeline
--------------------

A Druzhba pipeline is defined by a directory of YAML_ configuration files.
At run time, Druzhba will read these files and a special tracking table in
the destination database to determine what data to extract from source databases

.. _YAML: https://yaml.org/

As minimal example we're going to configure Druzhba to transfer the contents
of a single table in a PostgreSQL database to our data warehouse. We'll start by
creating a directory to hold our pipeline configuration.

Using your favorite text editor, create a file ``pipeline/_pipeline.yaml``:

.. code-block:: yaml

  ---
  connection:
    host: testserver.123456789012.us-east-1.redshift.amazonaws.com
    port: 5439
    database: testserver
    user: ${REDSHIFT_USER}
    password: ${REDSHIFT_PASSWORD}
  index:
    schema: druzhba_raw
    table: pipeline_index
  s3:
    bucket: my-bucket
    prefix: druzhba/
  iam_copy_role: arn:aws:iam::123456789012:role/RedshiftCopyUnload
  sources:
    - alias: demodb
      type: postgres


This file defines a *pipeline*. A pipeline definition consists of a destination
database connection (currently only `Amazon Redshift`_ is supported), an
optional index table definition, a mandatory S3_ location that the Druzhba
process will have read/write access to (temporary files will be written here
before calling ``COPY`` on the Redshift instance), an `IAM copy role`_, and
a list of source databases to pull. Each source has a unique ``alias`` and 
a ``type``, which must be one of ``postgres``, ``mysql``, or ``mssql``.

Druzhba supports limited templating of YAML configuration files and for example
the ``user`` field will be populated by the value of the ``REDSHIFT_USER``
environment variable.

Replace ``host``, ``database``, and ``iam_copy_role`` with appropriate values
for the Redshift instance you'll be using in this test.

.. _`Amazon Redshift`: https://aws.amazon.com/redshift/
.. _S3: https://aws.amazon.com/s3/
.. _`IAM copy role`: https://docs.aws.amazon.com/redshift/latest/mgmt/copy-unload-iam-role.html


Next we will create a file for each source database in our pipeline -- in this
case, one. Similarly to above this configuration will define a connection to the
source database, but will also contain a list of tables to copy from that source
database -- again, only one in this example.

Create a file ``pipeline/demodb.yaml``:

.. code-block:: yaml

  ---
  connection_string: postgresql://postgres:docker@localhost:54320/postgres
  tables:
    - source_table_name: starter_table
      destination_table_name: starter_table
      destination_schema_name: druzhba_raw
      index_column: updated_at
      primary_key:
        - id

For each table we define a the table in the source database to use, the schema
and table to create in the target database, and two special columns that we
usually want on every table in the pipeline. The index column is a column on the
source table that is only increasing -- generally an auto-incrementing
identifier for append-only tables, or an updated timestamp or row version for
update-in-place tables. The primary key is one or more columns used to uniquely
identify a row. Updated rows where the primary key already exists in the
destination table will result in updates rather than inserts.

See configuration_ for more on the configuration files,
and |example-link| for more examples.

.. _configuration: :doc:`configuration.rst`

Configuring a Source Database
-----------------------------

For this tutorial, we'll need a PostgreSQL instance. We'll walk through the steps
to set up a local PostgreSQL instance running in a Docker container below, but
if you already have a database running feel, free to skip the Docker instructions
and change connection strings below appropriately.

If you do not already have Docker installed, see: https://docs.docker.com/get-docker/

.. code-block:: bash

  # Run a local postgres instance in Docker on 54320
  docker run -d --name pglocal -e POSTGRES_PASSWORD=docker -v my_dbdata:/var/lib/postgresql/data -p 54320:5432 postgres:11

  # Run psql in the container
  docker exec -it pglocal psql -Upostgres

This will launch a psql process within the container.

.. code-block:: postgresql

  -- Inside PSQL:
  CREATE TABLE starter_table (
      id SERIAL PRIMARY KEY,
      data VARCHAR(255),
      created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
  );

  INSERT INTO starter_table (data)
  VALUES ('my first record'), ('my second record');

  SELECT * FROM starter_table;
   id |       data       |        created_at         |        updated_at
  ----+------------------+---------------------------+---------------------------
    1 | my first record  | 2020-05-26 11:29:52.25809 | 2020-05-26 11:29:52.25809
    2 | my second record | 2020-05-26 11:29:52.25809 | 2020-05-26 11:29:52.25809


Connect to your Redshift instance somehow and:

.. code-block:: postgresql

  CREATE USER druzhba_test PASSWORD 'Druzhba123';
  CREATE SCHEMA druzhba_raw;
  GRANT ALL ON SCHEMA druzhba_raw TO druzhba_test;

Set up your environment:

.. code-block:: bash

  export DRUZHBA_CONFIG_DIR=pipeline
  export REDSHIFT_USER=druzhba_test
  export REDSHIFT_PASSWORD=Druzhba123
  # ... set all the other envars from .env.test.sample for Redshift, AWS, S3...

Invoke Druzhba
--------------

Once configuration is set up for your database, run Druzhba with:

.. code-block:: bash

  druzhba -d demodb -t starter_table

... your data is now in Redshift! Subsequent invocations will incrementally pull updated rows
from the source table. Of course, this is just the beginning of your pipeline.
See [docs/cli.md](docs/cli.md) for more on the command line interface.


Usage Considerations
--------------------

Index_column filters should be fast
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Druzhba pulls incrementally according to the value of the `index_column` given in a table's 
configuration, and then inserts-or-replaces new or updated rows according to an optional
`primary_key`. On the first run (or if `--rebuild` is given) Druzhba will create the target table.
After that, it will use a SQL filter on `index_column` to only pull newly updated rows.

Consequently, queries against ``index_column`` need to be fast! Usually, unless a table is
``append_only``, an ``updated_at`` timestamp column is used to for `index_column` - it is usually
necessary to create a *database index*  (unfortunate name collision!) on this column to make these
pulls faster, which will slow down writes a little bit.


State
^^^^^

Druzhba currently tracks pipeline state by the _source_ database, database_alias, and table. Consequently, it supports
many-to-one pipelines from e.g. multiple copies of the same source database to a single shared target table.
But it does not support one-to-many pipelines, because it could not distinguish the state of the different pipelines.
SQL-based pipelines currently need to define a `source_table_name` which is used to track their state.


Manual vs Managed
^^^^^^^^^^^^^^^^^

A specific target table may be:

- "managed", meaning Druzhba handles the creation of the target table
  (inferred from datatypes on the source table) and the generation of
  the source-side query.
- "manual" - SQL queries are provided to read from the source (not
  necessarily from one table) and to create the target table (rather
  than inferring its schema from the source table).

Manual table creation is not supported for SQL Server.
