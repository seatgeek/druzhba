.. _quickstart:

Tutorial
========

In this tutorial we'll work through the basic steps of setting up a minimal
Druzhba pipeline.

To run this example you will need a source database to move data from and a,
target Redshift_ database to move data to, and an S3_ location to use as a
staging area for temporary files. If you do not already have those set up, or if
you run into any permissions errors, see:
:ref:`Demo Environment Setup <demosetup>`.

.. _Redshift: https://aws.amazon.com/redshift/
.. _S3: https://aws.amazon.com/s3/

Installing Druzhba
------------------

Install locally in a Python3 virtual environment or wherever you like:

.. code-block:: bash

  pip install druzhba

Or clone the source code and install from there

.. code-block:: bash

  git clone git@github.com:seatgeek/druzhba.git
  pip install -e .

.. _define-pipeline:

Define Your Pipeline
--------------------

A Druzhba pipeline is defined by a directory of YAML_ configuration files. At
run time, Druzhba will read these files and a special tracking table in the
destination database to determine what data to extract from source databases

.. _YAML: https://yaml.org/

As minimal example we're going to configure Druzhba to transfer the contents of
a single table in a PostgreSQL database to our data warehouse. We'll start by
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
before calling ``COPY`` on the Redshift instance), an `IAM copy role`_, and a
list of source databases to pull. Each source has a unique ``alias`` and a
``type``, which must be one of ``postgres``, ``mysql``, or ``mssql``.

Druzhba supports limited templating of YAML configuration files and to allow
injection of environment variables into the configuration. For example, the
``user`` field will be populated by the value of the ``REDSHIFT_USER``
environment variable.

Replace ``host``, ``database``, and ``iam_copy_role`` with appropriate values
for the Redshift instance you'll be using in this test.

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

For each table we define the table in the source database to use, the schema
and table to create in the target database, and two special columns that we
usually want on every table in the pipeline. The index column is a column on the
source table that is only increasing -- generally an auto-incrementing
identifier for append-only tables, or an updated timestamp or row version for
update-in-place tables. The primary key field is mandatory and used by Druzhba to uniquely
identify a row. Druzhba will ignore any primary key defined in the source database.
Updated rows where the primary key already exists in the
destination table will result in updates rather than inserts.

See :ref:`configuration` for more on the configuration files, and |example-link|
for more examples.

Set up your environment
^^^^^^^^^^^^^^^^^^^^^^^

Now we are ready to finish configuring our environment. We'll need to make sure
we have appropriate AWS credentials available to Druzhba, through the default
provider chain. Then we need to create environment variables to hold our
destination database credentials that our config file was set up to read.
Finally we set the ``DRUZHBA_CONFIG_DIR`` variable to point at the configuration
we want to run.

.. code-block:: bash

  export DRUZHBA_CONFIG_DIR=pipeline
  export REDSHIFT_USER=druzhba_test
  export REDSHIFT_PASSWORD=Druzhba123

Invoke Druzhba
--------------

Extract and load ``starter_table`` from ``demodb`` with:

.. code-block:: bash

  druzhba -d demodb -t starter_table

Your data is now in Redshift! Subsequent invocations will incrementally pull
updated rows from the source table. Of course, this is just the beginning of
your pipeline.

Note that you could also just run the command ``druzhba`` with no arguments to
run the entire pipeline. See :ref:`CLI Help <cli-help>` for more on the command
line interface.

Next Steps
----------

That's it! you should now have a working Druzhba pipeline. Next consider reading
the :ref:`configuration guide <configuration>`.
