.. image:: docs/resources/SG_Druzhba_Logo-Large.jpg
  :width: 600
  :alt: Druzhba
  :align: center

.. begin-lede

Druzhba is a friendly framework for building data pipelines. It efficiently
copies data from your production/transactional databases to your data warehouse.

A Druzhba pipeline connects one or more source databases to a target database.
It *pulls* data incrementally from each configured source table and writes to a
target table (which is automatically created in most cases), tracking
incremental state and history in the target database. Druzhba may also be
configured to pull using custom SQL, which supports Jinja templating of pipeline
metadata.

In a typical deployment, Druzhba serves the extract and load steps of an ELT
pipeline, although it is capable of limited in-flight transformations through
custom extract SQL.

Druzhba currently fully supports PostgreSQL and Mysql 5.5-5.7, and provides
partial support for Microsoft SQL Server as source databases. Druzhba supports
AWS Redshift as a target.

.. end-of-lede

Minimal Example
---------------

We'll set up a pipeline to extract a single table from an example PostgreSQL
instance that we'll call "testsource" and write to an existing Redshift database
that we'll call "testdest".

.. TODO: change the link below to point to hosted docs once they're hosted

See `quick start <docs/quickstart.rst>`_ for a more complete example.

Install locally in a Python3 environment:

.. code-block:: bash

  pip install druzhba

Druzhba's behavior is defined by a set of YAML_ configuration files +
environment variables for database connections. As minimal example, create a
directory `/pipeline` and a file `pipeline/_pipeline.yaml` that configures the
pipeline:

.. _YAML: https://yaml.org/

.. code-block:: yaml

  ---
  connection:
    host: ${REDSHIFT_HOST}
    port: 5439
    database: ${REDSHIFT_DATABASE}
    user: ${REDSHIFT_USER}
    password: ${REDSHIFT_PASSWORD}
  index:
    schema: druzhba_raw
    table: pipeline_index
  s3:
    bucket: ${S3_BUCKET}
    prefix: ${S3_PREFIX}
  iam_copy_role: ${IAM_COPY_ROLE}
  sources:
    - alias: testsource
      type: postgres

The ``_pipeline.yaml`` file defines the connection to the destination database
(via environment variables), the location of Druzhba's internal tracking table,
working S3 location for temporary files, the IAM copy role, and a single source
database called "testsource".

Create a file ``pipeline/testsource.yaml`` representing the source database:

.. code-block:: yaml

  ---
  connection_string: postgresql://user:password@host:5432/testdest
  tables:
    - source_table_name: your_table
      destination_table_name: your_table
      destination_schema_name: druzhba_raw
      index_column: updated_at
      primary_key:
        - id

The ``testsource.yaml`` file defines the connection to the testsource database
(note: see documentation for more secure ways of supplying connection
credentials) and a single table to copy over. The contents of your_table in the
source database will be copied to your_table in the `druzhba_raw` schema of the
target database. New rows will be identified by the value of their `id` column
and existing rows will be replaced if their `updated_at` column is greater than
on the previous iteration. 

Then, you'll need to set some environment variables corresponding to the
template fields in the configuration file above.

Once your configuration and environment are ready, load into Redshift:

.. code-block:: bash

  druzhba --database testsource --table your_table

Typically Druzhba's CLI would be run on a Cron schedule. Many deployments place
the configuration files in source control and use some form of CI for
deployment.

Druzhba may also be imported and used as a Python library, for example
to wrap pipeline execution with your own error handling.

Documentation
-------------

Please see documentation_ for more complete configuration examples and
descriptions of the various options to configure your data pipeline.

.. _documentation: https://github.com/seatgeek/druzhba/blob/master/docs/configuration.rst

Contributing
------------

Druzhba is an ongoing project. Feel free to open feature request issues or PRs.

PRs should be unit-tested, and will require an integration test passes to merge.

.. TODO: fix the link below once we have hosting correct 

See the docs_ for instructions on setting up a
Docker-Compose-based test environment.

.. _docs: https://github.com/seatgeek/druzhba/blob/sphinx-reorg/docs/contributing.rst

License
-------

This project is licensed under the terms of the 
`MIT license <https://github.com/seatgeek/druzhba/blob/master/LICENSE>`_.
