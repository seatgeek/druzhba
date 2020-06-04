Contributing
============

*This document contains information related to developing the Druzhba
application, specifically how to run unit and integration tests and the release
procedure for publishing a new version of Druzha. It will be of minimal use to
the average end user.*

Pull Requests
-------------

Unsolicited pull requests may be submitted against the ``master`` branch. Please
run Isort and Black (See: ``.pre-commit.sh``) prior to opening your pull request
and ensure that unit tests pass. Maintainers will run the integration test suite
if appropriate.

Needed Features
---------------

This is a list of most requested Druzhba features:

- Complete SQL Server support (currently partial support)
- Support for multiple output database types

Testing
-------

Unit Tests
^^^^^^^^^^

To run unit tests locally:

.. code-block:: bash

  python setup.py test

To use Docker for testing, create a `.env.test` file based on `env.test.sample`
with any environment variable overrides you wish to assign (can be empty).

To run unit tests in Docker:

.. code-block:: bash

  docker-compose run test


Integration Tests
^^^^^^^^^^^^^^^^^

To run integration tests of full pipelines in Docker, you'll need to add
Redshift credentials to your environment or your `.env.test` file. This makes
use of a test schema in an existing Redshift database, and for safety will fail
if the schema name already exists.

Then run:

.. code-block:: bash

  source .env.test.sample
  source .env.test  # For whatever overrides you need

  docker-compose up -d postgres mysql
  docker-compose build test

  docker-compose run test bash test/integration/run_test.sh mysql postgres
