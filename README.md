# Druzhba

Druzhba is a friendly tool for moving data around.

A Druzhba pipeline connects one or more source databases to a target database. It _pulls_ data incrementally
from each configured source table and writes to a target table (which is automatically created in most cases),
tracking incremental state and history in the target database. Druzhba may also be configured to pull from a SQL
query, which supports Jinja templating of some pipeline metadata.

In a typical deployment Druzhba serves the extract and load steps of an ELT pipeline, although it is capable of limited in-flight transformations through custom extract SQL.

Druzhba is tested against Postgres, Mysql 5.X, and (partially) MSQL Server as sources, and Amazon Redshift as a target.

## Minimal Example

We'll set up a pipeline to extract a single table from an example
Postgres instance that we'll call "testsource" and write to an existing Redshift database
that we'll call "testdest".

See [docs/README.md](docs/README.md) for a more complete example.

Install locally in a Python3 environment:
```
pip install druzhba
```

Druzhba's behavior is defined by a set of [YAML](https://yaml.org/) configuration files +
environment variables for database connections. As minimal example,
create a directory `/pipeline` and a file `pipeline/_pipeline.yaml`
that configures the pipeline:

```
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
```

The `_pipeline.yaml` file defines the connection to the destination database
(via environment variables), the location of Druzhba's internal tracking table,
working S3 location for temporary files, the IAM copy role, and a single
source database called "testsource".

Create a file `pipeline/testsource.yaml` representing the source database:

```
---
connection_string: postgresql://user:password@host:5432/testdest
tables:
  - source_table_name: your_table
    destination_table_name: your_table
    destination_schema_name: druzhba_raw
    index_column: updated_at
    primary_key:
      - id
```

The `testsource.yaml` file defines the connection to the testsource database 
(note: see documentation for more secure ways of supplying connection credentials) 
and a single table to copy over. The contents of your_table in the source database
will be copied to your_table in the druzhba_raw schema of the target database.
New rows will be identified by the value of their `id` column and existing rows
will be replaced if their `updated_at` column is greater than on the previous
iteration. 

Then, you'll need to set some environment variables corresponding to
the template fields in the configuration file above.

Once your configuration and environment are ready, load into Redshift:
```
druzhba --database testsource --table your_table
```

Typically Druzhba's CLI would be run on a Cron schedule, while its
configuration files would be updated via Github pull requests.

Druzhba may also be imported and used as a Python library, for example
to wrap pipeline execution with your own error handling.

## Documentation

Please see [docs/](docs/) for more complete configuration examples and descriptions of the various
options to configure your data pipeline.

## Contributing

Druzhba is an ongoing project. Feel free to open feature request issues or PRs.

PRs should be unit-tested, and will require an integration test passes to merge.

See the [docs/README.md](docs) for instructions on setting up a Docker-Compose-based test environment.

## License

This project is licensed under the terms of the MIT license.
