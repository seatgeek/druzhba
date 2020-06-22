import argparse
import copy
import datetime
import logging
import sys
import time
import traceback
from multiprocessing import cpu_count
from multiprocessing.dummy import Pool

import psycopg2
import yaml
from botocore.vendored.requests.exceptions import SSLError

from druzhba.config import CONFIG_DIR, load_destination_config
from druzhba.db import DatabaseConfig
from druzhba.monitoring import DefaultMonitoringProvider, configure_logging
from druzhba.redshift import create_index_table, init_redshift
from druzhba.table import (
    ConfigurationError,
    InvalidSchemaError,
    MigrationError,
    TableConfig,
)

logger = logging.getLogger("druzhba.main")
monitor = DefaultMonitoringProvider()


COMPILE_ONLY = None
PRINT_SQL_ONLY = None
VALIDATE_ONLY = None


def process_database(
    index_schema,
    index_table,
    db_alias,
    db_type,
    only_table_names,
    full_refresh=None,
    rebuild=None,
):
    logger.info("Beginning database %s", db_alias)
    try:
        with monitor.wrap("run-time", database=db_alias):
            _process_database(
                index_schema,
                index_table,
                db_alias,
                db_type,
                only_table_names,
                full_refresh,
                rebuild,
            )
        logger.info("Done with database %s", db_alias)
    except Exception as e:
        logger.exception("Fatal error in database %s, aborting", db_alias)
        raise e


def _process_database(
    index_schema,
    index_table,
    db_alias,
    db_type,
    only_table_names,
    full_refresh=None,
    rebuild=None,
):
    with open("{}/{}.yaml".format(CONFIG_DIR, db_alias), "r") as f:
        dbconfig = yaml.safe_load(f)
        db = DatabaseConfig(
            db_alias,
            db_type,
            connection_string=dbconfig.get("connection_string"),
            connection_string_env=dbconfig.get("connection_string_env"),
            object_schema_name=dbconfig.get("data", {}).get("object_schema_name"),
            db_template_data=dbconfig.get("data", {}),
        )

    tables_yaml = dbconfig["tables"]

    logger.info(
        "%s tables ready to import for database %s",
        len(tables_yaml),
        db.database_alias,
    )

    invalids = []
    retries_remaining = 5

    for table_yaml in tables_yaml:
        if not only_table_names and not table_yaml.get("enabled", True):
            continue

        source_table_name = table_yaml["source_table_name"]
        if only_table_names and source_table_name not in only_table_names:
            continue

        try:
            TableConfig.validate_yaml_configuration(table_yaml)
            logger.info("Validated: %s / %s", db.database_alias, source_table_name)
        except ConfigurationError as e:
            logger.error(str(e))
            invalids.append(source_table_name)
            continue

        if VALIDATE_ONLY:
            continue

        table_params = copy.deepcopy(table_yaml)
        if rebuild:
            table_params["rebuild"] = True
            table_params["full_refresh"] = True
        elif full_refresh:
            table_params["full_refresh"] = True
        table = db.get_table_config(
            table_params, index_schema=index_schema, index_table=index_table
        )
        table.validate_runtime_configuration()

        if COMPILE_ONLY:
            print("---------------------------------------------")
            print(table.get_query_sql())
            print("---------------------------------------------\n\n\n")
            continue

        if PRINT_SQL_ONLY:
            select_query = table.get_query_sql()
            # Create statement introspects the source DB for a schema
            create_statement = table.query_to_redshift_create_table(
                select_query, table.destination_table_name
            )

            print("---------------------------------------------")
            print(create_statement)
            print("---------------------------------------------")
            print(select_query)
            print("---------------------------------------------\n\n\n")
            continue

        advance_to_next_table = False
        while not advance_to_next_table and retries_remaining > 0:
            try:
                with monitor.wrap("create-redshift-table", database=db_alias):
                    table.check_destination_table_status()

                with monitor.wrap(
                    "extract-table", database=db_alias, table=table.source_table_name
                ):
                    table.extract()

                with monitor.wrap(
                    "load-table", database=db_alias, table=table.source_table_name
                ):
                    table.load()

                advance_to_next_table = True

            except (InvalidSchemaError, MigrationError):
                logger.exception(
                    "Error preparing target table %s.%s",
                    table.destination_schema_name, table.destination_table_name
                )
                advance_to_next_table = True

            except (
                ValueError,
                db.db_errors.InternalError,
                db.db_errors.IntegrityError,
                db.db_errors.ProgrammingError,
                psycopg2.InternalError,
                psycopg2.IntegrityError,
                psycopg2.ProgrammingError,
                psycopg2.extensions.TransactionRollbackError,
                psycopg2.errors.FeatureNotSupported,  # pylint: disable=no-member
            ) as e:
                logger.warning(
                    "Unexpected error processing %s table %s: ```%s\n\n%s```",
                    table.database_alias,
                    table.source_table_name,
                    e,
                    "".join(traceback.format_exc()),
                )
                logger.info("Continuing")
                advance_to_next_table = True

            except (
                SSLError,
                db.db_errors.OperationalError,
                db.db_errors.DatabaseError,
            ) as e:
                retries_remaining -= 1
                if retries_remaining > 0:
                    logger.info(
                        "Disconnected while processing %s table %s with error... Retrying.",
                        table.database_alias,
                        table.source_table_name,
                    )
                    logger.info(e)
                    monitor.record_error("disconnect-error", db_alias=db_alias)
                    time.sleep((5.0 - retries_remaining) ** 2)
                else:
                    logger.error(
                        "Error processing %s table %s and out of retries: ```%s\n\n%s```",
                        table.database_alias,
                        table.source_table_name,
                        e,
                        "".join(traceback.format_exc()),
                    )
                    raise

            except (psycopg2.extensions.QueryCanceledError, Exception) as e:
                logger.error(
                    "Unexpected error processing %s table %s",
                    table.database_alias,
                    table.source_table_name,
                )
                raise

            logger.info(
                "Done with %s table %s", table.database_alias, table.source_table_name,
            )

    if len(invalids) > 0:
        raise RuntimeError(
            "Had invalid table configurations in {}: \n{}".format(
                db.database_alias, ",".join(invalids)
            )
        )


@monitor.timer("full-run-time")
def run(args):
    # pylint: disable=global-statement
    if args.tables and not args.database:
        msg = "--tables argument is not valid without --database argument"
        raise ValueError(msg)
    if args.full_refresh and not args.tables:
        msg = "--full-refresh argument is not valid without --table(s) argument"
        raise ValueError(msg)

    logger.info(
        "Detected %s CPUs available with %s threads requested. Using %s.",
        cpu_count(),
        args.num_processes or "unspecified",
        args.num_processes or cpu_count(),
    )

    global COMPILE_ONLY
    COMPILE_ONLY = args.compile_only

    global PRINT_SQL_ONLY
    PRINT_SQL_ONLY = args.print_sql_only

    global VALIDATE_ONLY
    VALIDATE_ONLY = args.validate_only

    destination_config, missing_vars = load_destination_config(CONFIG_DIR)
    if not COMPILE_ONLY and not PRINT_SQL_ONLY and not VALIDATE_ONLY:
        if missing_vars:
            logger.error(
                "Could not find required environment variable(s): %s",
                ", ".join(missing_vars),
            )
            sys.exit(1)

    index_schema = destination_config["index"]["schema"]
    index_table = destination_config["index"]["table"]

    init_redshift(destination_config)

    # Create the index table if it doesn't exist
    if not COMPILE_ONLY and not PRINT_SQL_ONLY and not VALIDATE_ONLY:
        create_index_table(index_schema, index_table)

    if args.database:
        dbs = [
            (
                index_schema,
                index_table,
                db["alias"],
                db["type"],
                args.tables,
                args.full_refresh,
                args.rebuild,
            )
            for db in destination_config["sources"]
            if db["alias"] == args.database
        ]
        if not dbs:
            msg = "Database {} not recognized in _databases.yml".format(args.database)
            raise ValueError(msg)
    else:
        dbs = [
            (
                index_schema,
                index_table,
                db["alias"],
                db["type"],
                args.tables,
                None,
                None,
            )
            for db in destination_config["sources"]
            if db.get("enabled", True)
        ]

    if args.num_processes == 1:
        for db in dbs:
            process_database(*db)
    else:
        # Preload _strptime to avoid a threading bug in cpython
        # See: https://mail.python.org/pipermail/python-list/2015-October/697689.html
        _ = datetime.datetime.strptime("2018-01-01 01:02:03", "%Y-%m-%d %H:%M:%S")
        with Pool(args.num_processes) as pool:
            results = pool.map_async(lambda db: process_database(*db), dbs)

            results.wait()
            if not results.successful():
                # Don't need to relog on failure, the process already logged
                sys.exit(2)

    if args.validate_only:
        logger.info("Validation complete")


def _get_parser():

    parser = argparse.ArgumentParser(description="Friendly data pipeline framework")
    parser.add_argument(
        "-ll", "--log-level", help="Name of a python log level eg DEBUG"
    )
    parser.add_argument(
        "-d",
        "-db",
        "--database",
        help="A single database to run."
        "\nWill override a database marked disabled in the db config file",
    )
    parser.add_argument(
        "-t",
        "--table",
        "--tables",
        help="List of tables to run separated by spaces. Must be run"
        " with --database",
        nargs="*",
        dest="tables",
    )
    parser.add_argument(
        "-np",
        "--num-processes",
        help="Number of parallel processes to spawn."
        "\nDefaults to number of CPUs (cores) available.",
        type=int,
    )
    parser.add_argument(
        "-co",
        "--compile-only",
        action="store_true",
        help="Will print generated queries to STDOUT but not execute anything.",
    )
    parser.add_argument(
        "-ps",
        "--print-sql-only",
        action="store_true",
        help="Will print generated CREATE and SELECT statements to STDOUT only.",
    )
    parser.add_argument(
        "-vo",
        "--validate-only",
        action="store_true",
        help="Will execute configuration checks only.",
    )
    parser.add_argument(
        "-f",
        "--full-refresh",
        help="Force a full refresh of the table. "
        "Must be run with --database and --table(s). ",
        action="store_true",
    )
    parser.add_argument(
        "-r",
        "--rebuild",
        help="Automatically recreate and full-refresh the table. "
        "Must be run with --database and --table(s). Only "
        "supported for tables Druzhba can build.",
        action="store_true",
    )
    return parser


def main():
    args = _get_parser().parse_args()

    configure_logging(args)
    logger.info("Running druzhba")

    run(args)

    logger.info("Shutting down")


if __name__ == "__main__":
    main()
