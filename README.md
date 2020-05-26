# Druzhba

Druzhba is a friendly tool for moving data around.

A Druzhba pipeline connects one or more source databases to a target database. It _pulls_ data incrementally
from each configured source table and writes to a target table (which is automatically created in most cases),
tracking incremental state and history in the target database. Druzhba may also be configured to pull from a SQL
query, which supports Jinja templating of some pipeline metadata.

Druzhba is tested against Postgres, Mysql 5.X, and (partially) SQL Server as sources, and Amazon Redshift as a target.

## Getting started

We'll set up a pipeline to extract a single table from a local
Postgres instance and write to an existing Redshift database.

See [docs/README.md](docs/README.md) for a more complete example.

Install locally in a Python3 environment:
```
pip install druzhba
```

Druzhba's behavior is defined by a set of `.yaml` configuration files +
environment variables for database connections. As minimal example,
create a directory `/pipeline` and a file `pipeline/_pipeline.yaml`
representing the target database:

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
  - alias: pg
    type: postgres
```

Create a file `pipeline/pg.yaml` representing the source database:

```
---
connection_string: postgresql://user:password@host:5432/database_name
tables:
  - source_table_name: your_table
    destination_table_name: your_table
    destination_schema_name: druzhba_raw
    index_column: updated_at
    primary_key:
      - id
```

Then, you'll need to set some environment variables corresponding to
the template fields in the configuration file above.

Once your configuration and environment are ready, load into Redshift:
```
druzhba --database pg --table your_table
```

Typically Druzhba's CLI would be run on a Cron schedule, while its
configuration files would be updated via Github pull requests.

Druzhba may also be imported and used as a Python library, for example
to wrap pipeline execution with your own error handling.

## Documentation

[docs/](docs/)

## Contributing

Druzhba is still an immature project, but feel free to open feature request issues or PRs.

PRs should be unit-tested, and will require an integration test passes to merge.

See the [docs/README.md](docs) for instructions on setting up a Docker-Compose-based test environment.

## License

This project is licensed under the terms of the MIT license.
