# Druzhba

Soon to be open-source.


To install locally:

```
pip install -e .
```


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

docker-compose up -d db
docker-compose run test bash test/integration/run_test.sh
```

TODO: Use tox, test multiple versions of Python/Postgres, add Mysql/MSSQL.
