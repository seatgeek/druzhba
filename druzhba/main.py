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
from botocore.vendored.requests.exceptions import SSLError

from druzhba.config import CONFIG_DIR, load_config_file, load_destination_config
from druzhba.db import DatabaseConfig
from druzhba.monitoring import DefaultMonitoringProvider, configure_logging
from druzhba.redshift import (
    create_extract_monitor_table,
    create_index_table,
    create_load_monitor_table,
    init_redshift,
)
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


def process_table(
    table_yaml,
    index_schema,
    index_table,
    db: DatabaseConfig,
    db_alias,
    full_refresh=None,
    rebuild=None,
    monitor_tables_config=None,
):
    source_table_name = table_yaml["source_table_name"]

    try:
        TableConfig.validate_yaml_configuration(table_yaml)
        logger.info("Validated: %s / %s", db.database_alias, source_table_name)
    except ConfigurationError as e:
        logger.error(str(e))
        return source_table_name

    if VALIDATE_ONLY:
        return

    table_params = copy.deepcopy(table_yaml)
    if rebuild:
        table_params["rebuild"] = True
        table_params["full_refresh"] = True
    elif full_refresh:
        table_params["full_refresh"] = True
    table = db.get_table_config(
        table_params,
        index_schema=index_schema,
        index_table=index_table,
        monitor_tables_config=monitor_tables_config,
    )
    table.validate_runtime_configuration()

    if COMPILE_ONLY:
        print("---------------------------------------------")
        print(table.get_query_sql())
        print("---------------------------------------------\n\n\n")
        return

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
        return

    retries_remaining = 5
    table_complete = False
    while not table_complete and retries_remaining > 0:
        try:
            with monitor.wrap("create-redshift-table", db_alias=db_alias):
                table.check_destination_table_status()

            with monitor.wrap(
                "extract-table", db_alias=db_alias, table=table.source_table_name
            ):
                table.extract()

            with monitor.wrap(
                "load-table", db_alias=db_alias, table=table.source_table_name
            ):
                table.load()

            table_complete = True

        except (InvalidSchemaError, MigrationError):
            logger.exception(
                "Error preparing target table %s.%s",
                table.destination_schema_name,
                table.destination_table_name,
            )
            table_complete = True

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
            table_complete = True

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
            "Done with %s table %s",
            table.database_alias,
            table.source_table_name,
        )


def process_database(
    index_schema,
    index_table,
    db_alias,
    db_type,
    db_config_name,
    db_config_override,
    only_table_names,
    full_refresh=None,
    rebuild=None,
    monitor_tables_config=None,
    num_processes=cpu_count(),
    parallelize="database",
):
    logger.info("Beginning database %s", db_alias)
    try:
        with monitor.wrap("run-time", db_alias=db_alias):
            _process_database(
                index_schema,
                index_table,
                db_alias,
                db_type,
                db_config_name,
                db_config_override,
                only_table_names,
                full_refresh,
                rebuild,
                monitor_tables_config,
                num_processes,
                parallelize,
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
    db_config_name,
    db_config_override,
    only_table_names,
    full_refresh=None,
    rebuild=None,
    monitor_tables_config=None,
    num_processes=cpu_count(),
    parallelize="database",
):
    db, dbconfig = set_up_database(
        db_alias,
        db_type,
        db_config_name,
        db_config_override,
        CONFIG_DIR,
    )

    tables_yaml = dbconfig["tables"]

    logger.info(
        "%s tables ready to import for database %s",
        len(tables_yaml),
        db.database_alias,
    )

    invalids = []

    tables_to_process = []
    for table_yaml in tables_yaml:
        if not only_table_names and not table_yaml.get("enabled", True):
            continue

        source_table_name = table_yaml["source_table_name"]
        if only_table_names and source_table_name not in only_table_names:
            continue

        tables_to_process.append(table_yaml)

    if num_processes == 1 or parallelize == "database":
        invalids = [
            process_table(
                table_yaml,
                index_schema,
                index_table,
                db,
                db_alias,
                full_refresh,
                rebuild,
                monitor_tables_config,
            )
            for table_yaml in tables_to_process
        ]
    else:
        # Preload _strptime to avoid a threading bug in cpython
        # See: https://mail.python.org/pipermail/python-list/2015-October/697689.html
        _ = datetime.datetime.strptime("2018-01-01 01:02:03", "%Y-%m-%d %H:%M:%S")
        with Pool(num_processes) as pool:
            invalids = pool.map_async(
                lambda table_yaml: process_table(
                    table_yaml,
                    index_schema,
                    index_table,
                    db,
                    db_alias,
                    full_refresh,
                    rebuild,
                    monitor_tables_config,
                ),
                tables_to_process,
            )

            invalids.wait()
            if not invalids.successful():
                # Don't need to relog on failure, the process already logged
                sys.exit(2)

    if len(invalids) > 0:
        raise RuntimeError(
            "Had invalid table configurations in {}: \n{}".format(
                db.database_alias, ",".join(invalids)
            )
        )


def set_up_database(
    db_alias,
    db_type,
    db_config_name,
    db_config_override,
    config_dir,
):
    if db_config_name is None:
        db_config_name = db_alias

    if db_config_override is None:
        db_config_override = {}

    dbconfig, missing_vars = load_config_file(
        "{}/{}.yaml".format(config_dir, db_config_name)
    )
    _handle_missing_vars(missing_vars)

    # Combine pipeline data dict with dbconfig data dict
    db_template_data = dbconfig.get("data", {})
    db_template_data.update(db_config_override.get("data", {}))

    db = DatabaseConfig(
        db_alias,
        db_type,
        connection_string=db_config_override.get(
            "connection_string", dbconfig.get("connection_string")
        ),
        connection_string_env=db_config_override.get(
            "connection_string_env", dbconfig.get("connection_string_env")
        ),
        object_schema_name=db_template_data.get("object_schema_name"),
        db_template_data=db_template_data,
    )

    return db, dbconfig


def _handle_missing_vars(missing_vars):
    if not COMPILE_ONLY and not PRINT_SQL_ONLY and not VALIDATE_ONLY:
        if missing_vars:
            logger.error(
                "Could not find required environment variable(s): %s",
                ", ".join(missing_vars),
            )
            sys.exit(1)


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
    if not args.num_processes:
        args.num_processes = cpu_count()

    global COMPILE_ONLY
    COMPILE_ONLY = args.compile_only

    global PRINT_SQL_ONLY
    PRINT_SQL_ONLY = args.print_sql_only

    global VALIDATE_ONLY
    VALIDATE_ONLY = args.validate_only

    destination_config, missing_vars = load_destination_config(CONFIG_DIR)
    _handle_missing_vars(missing_vars)

    index_schema = destination_config["index"]["schema"]
    index_table = destination_config["index"]["table"]

    init_redshift(destination_config)

    # The monitor tables an optional configuration. If not set, Druzhba will not populate monitor data.
    monitor_tables_config = destination_config.get("monitor_tables")

    # Create the index and monitor (if configured) tables if they don't already exist.
    if not COMPILE_ONLY and not PRINT_SQL_ONLY and not VALIDATE_ONLY:
        create_index_table(index_schema, index_table)
        if monitor_tables_config is not None:
            monitor_schema = monitor_tables_config["schema"]
            extract_monitor_table = monitor_tables_config["extract_monitor_table"]
            load_monitor_table = monitor_tables_config["load_monitor_table"]
            create_extract_monitor_table(monitor_schema, extract_monitor_table)
            create_load_monitor_table(monitor_schema, load_monitor_table)

    if args.database:
        dbs = [
            (
                index_schema,
                index_table,
                db["alias"],
                db["type"],
                db.get("config_name"),
                db.get("config"),
                args.tables,
                args.full_refresh,
                args.rebuild,
                monitor_tables_config,
                args.num_processes,
                args.parallelize,
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
                db.get("config_name"),
                db.get("config"),
                args.tables,
                None,
                None,
                monitor_tables_config,
                args.num_processes,
                args.parallelize,
            )
            for db in destination_config["sources"]
            if db.get("enabled", True)
        ]

    if args.num_processes == 1 or args.parallelize == "table":
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
        "-p",
        "--parallelize",
        help="What level to apply paralell processing. Either 'database' or 'table'."
        "\n'database' will spawn multiple procesess per database configured."
        "\n'table' will spawn multiple procesess per table configured within a database."
        "\nDefaults to 'database'",
        type=str,
        choices=["table", "database"],
        default="database",
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
