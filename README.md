# Druzhba

Druzhba is a friendly tool for moving data around.

A Druzhba pipeline connects one or more source databases to a target database. It _pulls_ data incrementally
from each configured source table and writes to a target table (which is automatically created in most cases),
tracking incremental state and history in the target database. Druzhba may also be configured to pull from a SQL
query, which supports Jinja templating of some pipeline metadata.

Druzhba is tested against Postgres, Mysql 5.X, and (partially) SQL Server as sources, and Amazon Redshift as a target.

## Usage

Install locally:
```
pip install -e .
```

TODO: pypi instructions

Druzhba's behavior is defined by a directory of YAML configuration files.
See [docs/configuration.md](docs/configuration.md) for details, or [test/integration/config/](test/integration/config/) for an example.

A specific target table may be "managed", meaning Druzhba handles the creation of the target table (inferred from
datatypes on the source table) and the generation of the source-side query. Or it may be "manual" - using specific
SQL queries to read from the source (not necessarily from one table) and create the target table (since the datatypes
of the source query cannot be inferred the same way.) Automatic table creation is not supported for SQL Server.

Once configuration is set up for your database, run Druzhba with:
```
druzhba -d <database> -t <tables>
```

See [docs/cli.md](docs/cli.md) for more on the command line interface.

### Considerations

1. Druzhba pulls incrementally according to the value of the `index_column` given in a table's configuration, and then
inserts-or-replaces new or updated rows according to an optional `primary_key`. On the first run (or if `--rebuild` or
 `--full-refresh` options are given Druzhba will create the target table. After that, it will use a SQL filter on `index_column`
to only pull newly updated rows.
As a result, queries against `index_column` need to be fast! Usually, unless a table is `append_only`, an `updated_at` timestamp column
is used to for `index_column` - it is usually necessary to create a _database index_  (unfortunate name collision!) on this column to make
these pulls faster, which will slow down writes a little bit.


2. Druzhba tracks pipeline state by the _source_ database, database_alias, and table. Consequently, it supports
many-to-one pipelines from e.g. multiple copies of the same source database to a single shared target table.
But it does not support one-to-many pipelines, because it could not distinguish the state of the different pipelines.
SQL-based pipelines currently need to define a `source_table_name` which is used to track their state.


## Contributing

### Development

To run unit tests locally:

```
python setup.py test
```

To use Docker for testing, create a `.env.test` file based on `env.test.sample` with
any environment variable overrides you wish to assign (can be empty).

To run unit tests in Docker:
```
docker-compose run test
```


To run integration tests of a Postgres->Redshift pipeline in Docker, you'll need
to add Redshift credentials to your environment or your `.env.test` file. This makes use
of a test schema in an existing Redshift database, and will fail if the schema name already exists.
Then,run:
```
source .env.test.sample
source .env.test  # For whatever overrides you need

docker-compose up -d postgres mysql
docker-compose build test

docker-compose run test bash test/integration/run_test.sh mysql postgres
```

TODO: Use tox, test multiple versions of Python/Postgres, add Mysql8, add MSSQL.

### Releasing

See [docs/release.md](docs/release.md)
