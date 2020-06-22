import datetime
import json
import logging
import os
import time
from collections import namedtuple
from io import BytesIO

from boto3.s3.transfer import TransferConfig
from boto3.session import Session
from jinja2 import Environment, FileSystemLoader, StrictUndefined, select_autoescape

from druzhba.avro import write_avro_file
from druzhba.config import CONFIG_DIR
from druzhba.redshift import (
    generate_copy_query,
    generate_count_query,
    generate_create_table_like_query,
    generate_drop_exists_query,
    generate_drop_query,
    generate_insert_all_query,
    generate_lock_query,
    generate_rename_query,
    get_redshift,
)


def load_query(query, query_dir):
    with open(os.path.join(query_dir, query)) as f:
        query_file_contents = f.read()
    return query_file_contents


def _s3_url(bucket, key):
    return "s3://{}/{}".format(bucket, key)


class ConfigurationError(Exception):
    def __init__(self, msg, table):
        message = "Conflicting configuration for table {}: {}".format(table, msg)
        super(ConfigurationError, self).__init__(message)


class InvalidSchemaError(Exception):
    pass


class TableStateError(Exception):
    pass


class MigrationError(Exception):
    pass


class Permissions(namedtuple("Permissions", ["name", "is_group", "grants", "owner"])):
    all_str = "arwdRxt"
    all_grants = "ALL PRIVILEGES"
    char_to_grant = {
        "r": "SELECT",  # read
        "w": "UPDATE",  # write
        "a": "INSERT",  # append
        "d": "DELETE",
        "D": "TRUNCATE",
        "x": "REFERENCES",
        "t": "TRIGGER",
        "R": "RULE",  # not documented, apparently
    }

    @classmethod
    def parse(cls, raw_permissions):
        """

        Parameters
        ----------
        raw_permissions : str
            Like {user_name=arwdRxt/owner_name,"group group_name=r/owner_name"}

        Returns
        -------
        List of Permissions, or None if they could not be parsed
        """
        if not raw_permissions:
            return []
        elif raw_permissions[0] != "{" or raw_permissions[-1] != "}":
            return None
        else:
            output = []
            for p in raw_permissions.strip("{}").split(","):
                user, permission = p.strip('"').split("=")
                levels, owner = permission.split("/")
                if user == "":
                    name, is_group = "PUBLIC", True
                elif user.startswith("group "):
                    name, is_group = user[6:], True
                else:
                    name, is_group = user, False

                # A following * represents WITH GRANT OPTION - ignore
                levels_stripped = levels.replace("*", "")
                if levels_stripped == cls.all_str:
                    grants = [cls.all_grants]
                else:
                    grants = [cls.char_to_grant[c] for c in levels_stripped]

                output.append(cls(name, is_group, grants, owner))
            return output


class TableConfig(object):
    """Base class for a specific table. This class will have all methods
    that are engine agnostic--methods that only act with the host server
    or the data warehouse. All methods that interact with a specific engine
    should raise a NotImplementedError here and be overwritten with a
    subclass.

    Parameters
    ----------
    database_alias : str
        Config file name of the database
    db_connection_params : db.ConnectionParams
        DB connection parameters derived from a parsed connection string
    destination_table_name : str
        The name of the table where data should be loaded in the data
        warehouse
    destination_schema_name : str
        The name of the schema where data should be loaded in the data
        warehouse. Note : this should be "public" unless there's a good
        reason to segregate data.
    source_table_name : str
        name of the table in the source database. If a query_file is provided,
        this is purely for logging and monitoring
    query_file : str, optional
        path of a fully qualified SQL file that contains all the logic
        needed to extract a table.
    columns_to_drop : list of str, optional
        Defined by the YAML file, a list of columns that should not be
        imported into the data warehouse
    distribution_key : str, optional
        destination column name to use as the Redshift distkey
    sort_keys : list of str, optional
        destination column names to use as the Redshift sortkeys
    append_only : bool, optional
        Indicates that rows should only be inserted to this table and
        never updated or deleted. If True, primary_key has no effect.
        Default: False
    primary_key : str or list(str), optional
        Columns used to match records when updating the destination table. If
        not provided, primary keys are inferred from the source table. Has no
        effect if append_only is True
        Default: None
    index_column : str, optional
        Column used to identify new or updated rows in the source table.
        Persisted in the index table.
    index_sql : str, optional
        Custom SQL to be run against the source DB to find the current
        max index. Standard templating for the table applies and Druzhba expects
        the index to be returned in a column called `index_value`. Overrides
        index_column.
    truncate_file : str, optional
        Path to a fully qualified SQL file that contains logic to truncate
        the table when full-refreshing. Required to pass --full-refresh if a
        table defined is defined by a query_file.
    full_refresh : boolean, optional
        flag that forces a full deletion of a table prior to loading new data,
        rather than deleting only on matched PKs.  Setting True will conflict
        with index_column.
    rebuild : boolean, optional
        flag to rebuild the table completely. Implies full_refresh.
        Incompatible with query_file and certain conditions per-database-driver.
    type_map : dict, optional
        override type conversion from the source DB to redshift. This is
        set by YAML, and is an empty list of no configuration is provided.
        ex: {
               'tinyint(1)': 'smallint',
               'char(35)': 'varchar(70)',
               'bigint(20) unsigned': 'bigint'
            }

    Attributes
    ----------
    columns : list of str
        All columns read from the source table
    foreign_keys : list of str
        generated from the `create table` syntax, a list of foreign key
        relationships to create for a table after all tables are created
    comments : list of str
        generated from the `create table` syntax, a list of COMMENT ON
        commands to add comments to table columns
    pks : list of str
        generated from the `create table` syntax, a list of source column
        names that define the PK
    key_name : str
        Once the dump has happened, the file is uploaded to s3 at this
        key location
    old_index_value : str
        Used in the `where` clause, this is the most recent index value for
        a given table currently in the data warehouse
    new_index_value : str
        Used in the `where` clause, this is the max index value for a given
        table currently in the source database.
    data : dict
        Read from table definition in the yaml file to supply data to the Jinja
        templating
    db_template_data : dict
        Read from db definition in the yaml file to supply data to the Jinja
        templating

    Notes
    -----
    All parameters are also set as attributes
    """

    DESTINATION_TABLE_OK = "ok"
    DESTINATION_TABLE_REBUILD = "rebuild"
    DESTINATION_TABLE_DNE = "non-existent"
    DESTINATION_TABLE_INCORRECT = "incorrect"

    max_file_size = 100 * 1024 ** 2

    def __init__(
        self,
        database_alias,
        db_connection_params,
        destination_table_name,
        destination_schema_name,
        source_table_name,
        index_schema,
        index_table,
        query_file=None,
        distribution_key=None,
        sort_keys=None,
        index_column=None,
        index_sql=None,
        truncate_file=None,
        columns_to_drop=None,
        type_map=None,
        primary_key=None,
        not_null_date=False,
        full_refresh=False,
        rebuild=False,
        schema_file=None,
        data=None,
        append_only=False,
        db_template_data=None,
    ):
        self.database_alias = database_alias
        self.db_host = db_connection_params.host
        self.db_port = db_connection_params.port
        self.db_name = db_connection_params.name
        self.db_user = db_connection_params.user
        self.db_password = db_connection_params.password
        self._columns = None
        self.columns_to_drop = columns_to_drop or []
        self.destination_table_name = destination_table_name
        self.destination_schema_name = destination_schema_name
        self.query_file = query_file
        self.schema_file = schema_file
        self.distribution_key = distribution_key
        self.sort_keys = sort_keys
        self.source_table_name = source_table_name
        self.index_column = index_column
        self.index_sql = index_sql
        self.truncate_file = truncate_file
        self.append_only = append_only
        self.full_refresh = full_refresh
        self.rebuild = rebuild
        self.type_map = self._clean_type_map(type_map)
        self.primary_key = (
            [primary_key] if isinstance(primary_key, str) else primary_key
        )
        self.not_null_date = not_null_date
        self.foreign_keys = []
        self.comments = []
        self.pks = []
        self._old_index_value = "notset"
        self._new_index_value = "notset"
        self._destination_table_status = None
        self.table_template_data = data
        self.db_template_data = db_template_data
        self.index_schema = index_schema
        self.index_table = index_table

        self.date_key = datetime.datetime.strftime(
            datetime.datetime.utcnow(), "%Y%m%dT%H%M%S"
        )

        self.row_count = None
        self.upload_size = 0

        self.starttime = None
        self.endtime = None
        self.rows_inserted = None
        self.rows_deleted = None

        self.num_data_files = 0
        self.manifest_mode = False

        self.logger = logging.getLogger(f"druzhba.{database_alias}.{source_table_name}")
        self.s3 = Session().client("s3")

    @classmethod
    def _clean_type_map(cls, type_map):
        if not type_map:
            return {}
        for k, v in type_map.items():
            type_map[k.lower()] = v
        return type_map

    @classmethod
    def validate_yaml_configuration(cls, yaml_config):
        """
        Validate YAML configuration. Note that this can differ slightly from runtime
        config, since full_refresh may be forced even when it would fail these checks.
        """

        table = yaml_config["source_table_name"]
        index_column = yaml_config.get("index_column")
        index_sql = yaml_config.get("index_sql")
        append_only = yaml_config.get("append_only")
        full_refresh = yaml_config.get("full_refresh")
        primary_key = yaml_config.get("primary_key")
        query_file = yaml_config.get("query_file")
        schema_file = yaml_config.get("schema_file")

        has_incremental_index = index_column or index_sql

        if not has_incremental_index and append_only:
            raise ConfigurationError("Append_only without incremental index", table)
        if full_refresh and append_only:
            raise ConfigurationError("Append_only with full_refresh", table)
        elif not has_incremental_index and not full_refresh:
            raise ConfigurationError(
                "Incremental update with no specified index", table
            )
        elif index_column and full_refresh:
            raise ConfigurationError("Full refresh with index_column", table)
        elif index_sql and full_refresh:
            raise ConfigurationError("Full refresh with index_sql", table)
        elif index_sql and index_column:
            raise ConfigurationError("index_sql and index_column", table)
        elif query_file and not primary_key and not append_only and not full_refresh:
            raise ConfigurationError(
                "incremental query_file without primary_key", table
            )
        elif query_file and not os.path.isfile(os.path.join(CONFIG_DIR, query_file)):
            raise ConfigurationError("nonexistent query_file", table)
        elif schema_file and not os.path.isfile(os.path.join(CONFIG_DIR, schema_file)):
            raise ConfigurationError("nonexistent schema_file", table)

    def validate_runtime_configuration(self):
        """
        Validate this instance's configuration state, which can differ from yaml
        configurations allowed by validate_yaml_configuration and runs after
        connecting to the database.
        """
        if self.rebuild and self.truncate_file:
            msg = (
                "Cannot rebuild a table with a truncate_file "
                "because it would not be correct to drop the table."
            )
            raise ConfigurationError(msg, self.source_table_name)
        elif self.rebuild and self.schema_file:
            msg = (
                "Cannot rebuild a table with a schema file, need "
                "support for passing in the table name to create."
            )
            raise ConfigurationError(msg, self.source_table_name)

    @property
    def s3_key_prefix(self):
        return "{}/{}.{}.{}".format(
            get_redshift().s3_config.prefix,
            self.database_alias,
            self.source_table_name,
            self.date_key,
        )

    def single_s3_data_key(self):
        """Returns the S3 path to upload a single avro file to"""
        if self.manifest_mode:
            raise TableStateError(
                "Attempted to treat a manifest upload as a single file"
            )

        return "{}.avro".format(self.s3_key_prefix)

    def manifest_s3_data_key(self):
        if not self.manifest_mode:
            raise TableStateError(
                "Attempted to treat a single file upload as a manifest"
            )

        return "{}.manifest".format(self.s3_key_prefix)

    def numbered_s3_data_key(self, file_num):
        return "{}/{:05d}.avro".format(self.s3_key_prefix, file_num)

    def next_s3_data_file_key(self):
        if self.manifest_mode:
            return self.numbered_s3_data_key(self.num_data_files)
        else:
            return self.single_s3_data_key()

    @property
    def copy_target_key(self):
        if self.manifest_mode:
            return self.manifest_s3_data_key()
        else:
            return self.single_s3_data_key()

    @property
    def copy_target_url(self):
        return _s3_url(get_redshift().s3_config.bucket, self.copy_target_key)

    def data_file_keys(self):
        if self.manifest_mode:
            for fn in range(self.num_data_files):
                yield self.numbered_s3_data_key(fn)
        else:
            yield self.single_s3_data_key()

    @property
    def connection_vars(self):
        raise NotImplementedError

    @property
    def avro_type_map(self):
        raise NotImplementedError

    def get_query_from_file(self):
        env = Environment(
            loader=FileSystemLoader(os.path.join(CONFIG_DIR)),
            autoescape=select_autoescape(["sql"]),
            undefined=StrictUndefined,
        )
        template = env.get_template(self.query_file)
        return template.render(
            db=self.db_template_data,
            table=self.table_template_data,
            run=self.run_template_data,
        )

    def get_sql_description(self, sql):
        raise NotImplementedError

    def get_query_sql(self):
        if self.query_file:
            return self.get_query_from_file()
        else:
            return self._get_query_sql() + self.where_clause()

    def _get_query_sql(self):
        raise NotImplementedError

    @property
    def columns(self):
        if not self._columns:
            self._columns = [
                row[0] for row in self.get_sql_description(self.get_query_sql())
            ]
        return self._columns

    def query(self, sql):
        raise NotImplementedError

    def query_fetchone(self, sql):
        results = self.query(sql)
        return next(results)

    def _check_index_values(self):
        """Returns true if new index is greater than old index and defined"""
        if self.full_refresh:
            if self.index_column or self.index_sql:
                msg = (
                    "Index was found, but %s was forced. "
                    "Old index value will be ignored, but new "
                    "index value will be recorded."
                )
                self.logger.info(msg, "rebuild" if self.rebuild else "full-refresh")
            return True

        if self.append_only:
            return True

        # If there is no previous index, we're fine
        if self.old_index_value is None:
            return True

        # There's an old index but can't load a new value.
        if self.new_index_value is None and self.old_index_value is not None:
            msg = "Index expected but not found. Last value was %s. Dumping full table"
            self.logger.warning(msg, self.old_index_value)
            return False

        try:
            # old_index_value comes in as a unicode new_index_value as the sql
            # type
            if isinstance(self.new_index_value, int):
                is_inverted = int(self.old_index_value) > self.new_index_value
            elif isinstance(self.new_index_value, datetime.datetime):
                old_index_dt = datetime.datetime.strptime(
                    self.old_index_value, "%Y-%m-%d %H:%M:%S.%f"
                )
                is_inverted = old_index_dt > self.new_index_value
            else:
                self.logger.warning(
                    "Unknown type %s for index %s",
                    type(self.new_index_value),
                    self.old_index_value,
                )
                return False
        except (ValueError, TypeError) as ex:
            self.logger.warning("Could not check index: %s", str(ex))
            return False

        if is_inverted:
            self.logger.warning(
                "Index value has decreased for table %s.%s. "
                "May need to do full refresh",
                self.db_name,
                self.source_table_name,
            )
        return not is_inverted

    def row_generator(self):
        sql = self.get_query_sql()
        self._check_index_values()
        self.logger.info("Extracting %s table %s", self.db_name, self.source_table_name)
        self.logger.debug("Running SQL: %s", sql)
        return self.query(sql)

    @property
    def run_template_data(self):
        return {
            "destination_schema_name": self.destination_schema_name,
            "destination_table_name": self.destination_table_name,
            "db_name": self.db_name,
            "source_table_name": self.source_table_name,
            "index_column": self.index_column,
            "new_index_value": self.new_index_value,
            "old_index_value": self.old_index_value,
        }

    def _load_old_index_value(self):
        """Sets and gets the index_value property, retrieved from Redshift

        Returns
        -------
        index_value : variable
            Since index_value can vary from table to table, this can be many
            different types. Most common will be a datetime or int, but
            could also be a date or string. Returns None if no previous index
            value found
        """

        query = f"""
        SELECT index_value
          FROM "{self.index_schema}"."{self.index_table}"
         WHERE datastore_name = %s
           AND database_name = %s
           AND table_name = %s
        ORDER BY created_ts DESC
        LIMIT 1;
        """
        self.logger.debug("Querying Redshift for last updated index")
        with get_redshift().cursor() as cur:
            cur.execute(
                query, (self.database_alias, self.db_name, self.source_table_name)
            )
            index_value = cur.fetchone()

        if index_value is None:
            self.logger.info(
                "No index found. Dumping entire table: %s.", self.source_table_name
            )
            return index_value
        else:
            self.logger.info("Index found: %s", index_value[0])
            return index_value[0]

    @property
    def old_index_value(self):
        # we use 'notset' rather than None because None is a valid output
        if self._old_index_value is "notset":
            self._old_index_value = self._load_old_index_value()
        return self._old_index_value

    def _load_new_index_value(self):
        # Abstract to support DB-specific quoting
        raise NotImplementedError

    @property
    def new_index_value(self):
        # we use 'notset' rather than None because None is a valid output
        if self._new_index_value is "notset":
            if self.index_sql:
                env = Environment(
                    autoescape=select_autoescape(["sql"]), undefined=StrictUndefined
                )
                template = env.from_string(self.index_sql)
                query = template.render(
                    db=self.db_template_data,
                    table=self.table_template_data,
                    run=self.run_template_data,
                )
                self._new_index_value = self.query_fetchone(query)["index_value"]

            elif self.index_column:
                self._new_index_value = self._load_new_index_value()
            else:
                self._new_index_value = None
        if self.query_file and self.index_sql and self._new_index_value is None:
            # Handles a special case where the index_sql query returns no rows
            # but the custom sql file is expecting both old and new index values
            return 0
        return self._new_index_value

    def where_clause(self):
        """Method for filtering get_data, if tables are able to be
        sliced on some index value.

        Returns
        -------
        str
            valid SQL featuring just the WHERE clause
        """
        where_clause = "\nWHERE "
        if not self.index_column or self.full_refresh:
            # If no index_column, there is no where clause. The whole
            # source table is dumped.
            return ""

        if self.new_index_value is None:
            # Either the table is empty or the index_column is all NULL
            return ""

        if self.old_index_value:
            # This should always happen except on the initial load
            where_clause += "{} > '{}' AND ".format(
                self.index_column, self.old_index_value
            )

        where_clause += "{} <= '{}'".format(self.index_column, self.new_index_value)
        return where_clause

    def get_destination_table_columns(self):
        query = """
        SELECT "column"
          FROM pg_table_def
         WHERE schemaname = %s
           AND tablename = %s;
        """

        with get_redshift().cursor() as cur:
            self.set_search_path(cur)
            cur.execute(
                query, (self.destination_schema_name, self.destination_table_name)
            )
            results = cur.fetchall()

        return [x[0] for x in results]

    def get_destination_table_status(self):
        """Queries the data warehouse to determine if the desired
        destination table exists and if so, if it matches the expected
        configuration.

        Returns
        -------
        str
           Representing our plan for what to do with the destination table
           Includes:
            - DESTINATION_TABLE_DNE -> build it if possible
            - DESTINATION_TABLE_REBUILD -> rebuild it
            - DESTINATION_TABLE_OK -> leave it
            - DESTINATION_TABLE_INCORRECT -> error
        """
        dw_columns = set(self.get_destination_table_columns())
        source_columns = set(self.columns)
        expected = source_columns - set(self.columns_to_drop)
        unexpected_dw_columns = dw_columns - expected
        unexpected_source_columns = expected - dw_columns

        if len(dw_columns) == 0:
            self.logger.info("Destination table does not exist.")
            return self.DESTINATION_TABLE_DNE
        elif self.rebuild:
            # We're rebuilding it so we don't care if it's right,
            # so exit before we log any errors
            self.logger.info("Attempting to rebuild destination table.")
            return self.DESTINATION_TABLE_REBUILD
        elif dw_columns == expected:
            return self.DESTINATION_TABLE_OK
        elif len(unexpected_dw_columns) > 0:
            msg = (
                "Columns exist in the warehouse table that are not in "
                "the source: `%s`"
            )
            self.logger.warning(msg, "`, `".join(unexpected_dw_columns))
            return self.DESTINATION_TABLE_INCORRECT
        elif len(unexpected_source_columns) > 0:
            msg = (
                "Columns exist in the source table that are not in the "
                + "warehouse. Skipping column(s): `%s`"
            )
            self.logger.warning(msg, "`, `".join(unexpected_source_columns))

            # Copy from avro will just ignore the extra columns so we can proceed
            return self.DESTINATION_TABLE_OK
        else:
            raise RuntimeError("Unhandled case in get_destination_table_status")

    def query_description_to_avro(self, sql):
        desc = self.get_sql_description(sql)
        fields = []

        for col_desc in desc:
            col_name = col_desc[0]
            schema = {"name": col_name}
            try:
                col_type = col_desc[1].split("(")[0]
            except AttributeError:
                col_type = col_desc[1]
            if col_type in self.avro_type_map["string"]:
                schema["type"] = ["null", "string"]
            elif col_type in self.avro_type_map["int"]:
                schema["type"] = ["null", "int"]
            elif col_type in self.avro_type_map["double"]:
                schema["type"] = ["null", "double"]
            elif col_type in self.avro_type_map["long"]:
                schema["type"] = ["null", "long"]
            elif col_type in self.avro_type_map["boolean"]:
                schema["type"] = ["null", "boolean"]
            elif col_type in self.avro_type_map["decimal"]:
                # fastavro now supports decimal types, but Redshift does not
                schema["type"] = ["null", "string"]
            else:
                self.logger.warning(
                    "unmatched data type for column %s in %s table %s",
                    col_desc[0],
                    self.db_name,
                    self.source_table_name,
                )
                schema["type"] = ["null", "string"]

            fields.append(schema)
        return fields

    def set_last_updated_index(self):
        """Adds a new index to the pipeline_table_index table for updated
        tables
        """
        if self.new_index_value is None:
            return

        query = f"""
        INSERT INTO "{self.index_schema}"."{self.index_table}" VALUES
        (%s, %s, %s, %s)
        """

        if isinstance(self.new_index_value, int):
            new_index_value = str(self.new_index_value)
        elif isinstance(self.new_index_value, datetime.datetime):
            new_index_value = self.new_index_value.strftime("%Y-%m-%d %H:%M:%S.%f")
        else:
            msg = "Don't know how to handle index {} of type {}".format(
                self.new_index_value, str(type(self.new_index_value))
            )
            raise TypeError(msg)

        self.logger.info("Updating index table")
        with get_redshift().cursor() as cur:
            args = (
                self.database_alias,
                self.db_name,
                self.source_table_name,
                new_index_value,
            )
            self.logger.debug(cur.mogrify(query, args))
            cur.execute(query, args)

    def create_table_keys(self, distkey=None, sortkeys=None):
        output = ""
        distkey = distkey or self.distribution_key
        if distkey:
            output += "distkey({})\n".format(distkey)

        sortkeys = sortkeys or self.sort_keys
        if sortkeys:
            output += "compound " if len(sortkeys) > 1 else ""
            output += "sortkey({})\n".format(",".join(sortkeys))
        return output

    def query_to_redshift_create_table(self, sql, table_name):
        raise NotImplementedError

    def check_destination_table_status(self):
        """Get the source table schema, convert it to Redshift compatibility,
        and ensure the table exists as expected in the data warehouse.

        Sets self._destination_table_status if we can proceed, or raises
        if not.

        Raises
        ------
        InvalidSchemaError
            Raised when the target table has columns not recognized in the source
            table, and we're not rebuilding the table. (Unrecognized source
            columns will log but still proceed)
        MigrationError
            Raised when the target table needs to be create or rebuilt but
            cannot be done automatically.
        """
        self.logger.info("Getting CREATE TABLE command")

        self._destination_table_status = self.get_destination_table_status()

        if self._destination_table_status in (
            self.DESTINATION_TABLE_DNE,
            self.DESTINATION_TABLE_REBUILD,
        ):
            self.logger.info("Verifying that the table can be created.")
            try:
                # Only called to see if it raises, the actual table
                # will be created later
                self.query_to_redshift_create_table(
                    self.get_query_sql(), self.destination_table_name
                )
            except NotImplementedError:
                raise MigrationError("Automatic table creation was not implemented for "
                                     "this database, manual migration needed.")
        elif self._destination_table_status == self.DESTINATION_TABLE_INCORRECT:
            raise InvalidSchemaError(
                "Extra columns exist in redshift table. Migration needed"
            )

    def register_extract_monitor(self, starttime, endtime):
        """Adds an entry into the extract monitor for a given extract task

        Parameters
        ----------
        starttime : datetime.datetime
            datetime object generated at the beginning of the data
            extraction
        endtime : datetime.datetime
            datetime object generated at the end of the data extraction
        """

        query = """
        INSERT INTO "public"."table_extract_detail" VALUES (
            %(task_id)s, %(class_name)s, %(task_date_params)s,
            %(task_other_params)s, %(start_dt)s, %(end_dt)s, %(run_time_sec)s,
            %(manifest_path)s, %(data_path)s, %(output_exists)s, %(row_count)s,
            %(upload_size)s, %(exception)s
        );
        """
        args = {
            "task_id": "{}(alias={}, database={}, table={})".format(
                self.__class__.__name__,
                self.database_alias,
                self.db_name,
                self.source_table_name,
            ),
            "class_name": self.__class__.__name__,
            "task_date_params": None,
            "task_other_params": None,
            "start_dt": starttime.replace(microsecond=0),
            "end_dt": endtime.replace(microsecond=0),
            "run_time_sec": (endtime - starttime).total_seconds(),
            "manifest_path": self.copy_target_url,
            "data_path": "s3://{}/{}".format(
                get_redshift().s3_config.bucket, self.s3_key_prefix
            ),
            "output_exists": self.row_count > 0,
            "row_count": self.row_count,
            "upload_size": self.upload_size,
            "exception": None,
        }

        self.logger.info("Inserting record into table_extract_detail")
        with get_redshift().cursor() as cur:
            cur.execute(query, args)

    def register_load_monitor(self):
        """Adds an entry into the load monitor for a given load task

        Parameters
        ----------
        starttime : datetime
            datetime object generated at the beginning of the load
        endtime : datetime
            datetime object generated at the end of the load
        rows_inserted : int
            Total number of rows generated by the Redshift COPY command
        rows_deleted : int
            Count of rows deleted by primary key in the destination table
        load_size : int
            Size in bytes of the file in S3 used in the Redshift COPY
            command
        """

        query = """
            INSERT INTO "public"."table_load_detail" VALUES (
                %(task_id)s, %(class_name)s, %(task_date_params)s, 
                %(task_other_params)s, %(target_table)s, %(start_dt)s,
                %(end_dt)s, %(run_time_sec)s, %(extract_task_update_id)s,
                %(data_path)s, %(manifest_cleaned)s, %(rows_inserted)s,
                %(rows_deleted)s, %(load_size)s, %(exception)s
            );
        """
        task_id = "{}(alias={}, database={}, table={})".format(
            self.__class__.__name__,
            self.database_alias,
            self.db_name,
            self.source_table_name,
        )
        target_table = "{}.{}".format(
            self.destination_schema_name, self.destination_table_name
        )
        args = {
            "task_id": task_id,
            "class_name": self.__class__.__name__,
            "task_date_params": None,
            "task_other_params": None,
            "target_table": target_table,
            "start_dt": self.starttime.replace(microsecond=0),
            "end_dt": self.endtime.replace(microsecond=0),
            "run_time_sec": (self.endtime - self.starttime).total_seconds(),
            "extract_task_update_id": task_id,
            "data_path": self.copy_target_url,
            "manifest_cleaned": False,
            "rows_inserted": self.rows_inserted,
            "rows_deleted": self.rows_deleted,
            "load_size": self.upload_size,
            "exception": None,
        }

        self.logger.info("Inserting record into table_load_detail")
        with get_redshift().cursor() as cur:
            cur.execute(query, args)

    def extract(self):
        """Serializes full db result set and uploads to s3

        The data will be uploaded either as a single file or as a set of files
        with a manifest
        """
        # TODO: Do we not currently execute the extract monitor?
        # starttime = datetime.datetime.utcnow()

        results_schema = self.query_description_to_avro(self.get_query_sql())
        results_iter = self.row_generator()

        done = False
        while not done:
            done = self.avro_to_s3(results_iter, results_schema)

        if self.num_data_files == 0:
            self.logger.info(
                "No data extracted; not uploading to s3 for %s table %s",
                self.db_name,
                self.source_table_name,
            )

        if self.manifest_mode:
            self.write_manifest_file()

        # endtime = datetime.datetime.utcnow()
        # self.register_extract_monitor(starttime, endtime)

    def avro_to_s3(self, results_iter, results_schema):
        """Attempts to serialize a result set to an AVRO file

        returns true if it complete writes the entire result_iter and false
        if there were records remaining when it hit the maximum file size.
        """
        with BytesIO() as f:
            complete, row_count = write_avro_file(
                f,
                results_iter,
                results_schema,
                self.destination_table_name,
                self.max_file_size,
            )

            if self.row_count is None:
                self.row_count = row_count
            else:
                self.row_count += row_count

            self.upload_size += f.tell()

            if not complete:
                self.manifest_mode = True

            if row_count > 0:
                self._upload_s3(
                    f, get_redshift().s3_config.bucket, self.next_s3_data_file_key()
                )
                self.num_data_files += 1

        return complete

    def write_manifest_file(self):
        if not self.manifest_mode:
            raise TableStateError("Cannot write manifest when not in manifest mode")

        entries = [
            {"url": _s3_url(get_redshift().s3_config.bucket, key), "mandatory": True}
            for key in self.data_file_keys()
        ]
        manifest = {"entries": entries}

        with BytesIO() as f:
            f.write(json.dumps(manifest).encode())
            self._upload_s3(
                f, get_redshift().s3_config.bucket, self.manifest_s3_data_key()
            )

    def _upload_s3(self, f, bucket, key):
        """
        Upload a file to this table's s3 key.

        Parameters
        ----------
        f : an open file handle.
        s3_path : string indicating the s3 location to write to
        """

        MB = 1024 ** 2
        s3_config = TransferConfig(multipart_threshold=10 * MB)
        f.seek(0)

        self.logger.info("Writing s3 file %s", _s3_url(bucket, key))

        retries = 3
        retries_remaining = retries
        while retries_remaining > 0:
            try:
                self.s3.upload_fileobj(f, bucket, key, Config=s3_config)
                self.logger.info("Wrote s3 file %s", _s3_url(bucket, key))
                return
            except KeyError:
                # retry on intermittent credential error
                retries_remaining -= 1
                if retries_remaining > 0:
                    time.sleep(3 * (retries - retries_remaining) ** 2)
                else:
                    raise

    def set_search_path(self, cursor):
        """This sets the search_path for a Redshift session.
        The default search_path is "'$user', public"; this replaces it with
        just the destination_schema_name.

        Parameters
        ----------
        cursor : Redshift cursor
            A cursor is passed in rather than generated because we need to
            modify the existing cursor and not create a new one
        """
        query = "SET search_path TO {};".format(self.destination_schema_name)
        self.logger.debug(query)
        cursor.execute(query)

    def get_delete_sql(self):
        if self.full_refresh:
            if self._destination_table_status == self.DESTINATION_TABLE_REBUILD:
                # We'll just drop it
                return ""
            if self.truncate_file:
                env = Environment(
                    loader=FileSystemLoader(os.path.join("datacfg")),
                    autoescape=select_autoescape(["sql"]),
                    undefined=StrictUndefined,
                )
                template = env.get_template(self.truncate_file)
                return template.render(
                    db=self.db_template_data,
                    table=self.table_template_data,
                    run=self.run_template_data,
                )
            else:
                return 'DELETE FROM "{}";'.format(self.destination_table_name)
        elif not self.append_only:
            if self.primary_key:
                # override from db yaml file
                pks = self.primary_key
            else:
                # pk column discovered from the source table
                pks = self.pks

            if not pks and self.index_column:
                raise InvalidSchemaError(
                    "Specifying an index column without primary key would "
                    "result in all records in the existing table being "
                    "deleted. If this is the desired behavior, run with "
                    "--full-refresh instead. If not, check if primary keys"
                    "can be inferred from the upstream database."
                )

            constraints = [
                '"{0}"."{2}" = "{1}"."{2}"'.format(
                    self.staging_table_name, self.destination_table_name, pk
                )
                for pk in pks
            ]
            constraint_string = " AND ".join(constraints)
            return 'DELETE FROM "{}" USING "{}" WHERE {};'.format(
                self.destination_table_name, self.staging_table_name, constraint_string,
            )
        else:
            # Should only land here when append_only
            # in which case we're not deleting
            return None

    def get_grant_sql(self, cursor):
        """ Get SQL statements to restore permissions
        to the staging table after a rebuild. """

        get_permissions_sql = """
        SELECT
            use.usename = CURRENT_USER          AS "owned"
            , c.relacl                          AS "permissions"
        FROM pg_class c
            LEFT JOIN pg_namespace nsp ON c.relnamespace = nsp.oid
            LEFT JOIN pg_user use ON c.relowner = use.usesysid
        WHERE
            c.relkind = 'r'
            AND nsp.nspname = '{schema}'
            AND c.relname = '{table}'
        """.format(
            schema=self.destination_schema_name, table=self.destination_table_name
        )
        cursor.execute(get_permissions_sql)
        permissions_result = cursor.fetchall()

        if len(permissions_result) == 0:
            self.logger.info("No existing permissions found for %s.%s",
                             self.destination_schema_name, self.destination_table_name)
            return None
        elif len(permissions_result) > 1:
            raise MigrationError("Got multiple permissions rows for table")

        is_owner, permissions_str = permissions_result[0]
        if not is_owner:
            raise MigrationError("Can't rebuild target table because it has another owner")

        self.logger.info(
            "Got existing permissions for table to add to %s: %s",
            self.staging_table_name,
            permissions_str,
        )
        permissions = Permissions.parse(permissions_str)
        if permissions is None:
            raise MigrationError(
                "Couldn't parse permissions {} to rebuild target table".format(permissions_str)
            )

        grant_template = "GRANT {grant} ON {table} TO {group}{name};"
        grant_sqls = [
            grant_template.format(
                grant=g,
                table=self.staging_table_name,
                # Should we not restore users?
                group="GROUP " if p.is_group else "",
                name=p.name,
            )
            for p in permissions
            for g in p.grants
        ]
        return "\n".join(grant_sqls)

    @property
    def staging_table_name(self):
        return "{}_{}_staging".format(self.database_alias, self.destination_table_name)

    def load(self):
        """The Load phase of the pipeline. Takes a file in S3 and issues a
        Redshift COPY command to import the data into a staging table. It
        then upserts the data by deleting rows in the destination table
        that match on PK, inserts all rows from the staging table into the
        destination table, and then deletes the staging table.
        """

        self.starttime = datetime.datetime.utcnow()

        # Initializing Data
        delete_clause = self.get_delete_sql()
        staging_table = self.staging_table_name
        destination_table = self.destination_table_name
        is_normal_load = self._destination_table_status == self.DESTINATION_TABLE_OK
        is_rebuild = self._destination_table_status == self.DESTINATION_TABLE_REBUILD
        is_dne = self._destination_table_status == self.DESTINATION_TABLE_DNE

        with get_redshift().cursor() as cur:
            self.set_search_path(cur)

            # If table does not exist, create it
            if is_dne:
                create_table = self.query_to_redshift_create_table(
                    self.get_query_sql(), self.destination_table_name
                )
                cur.execute(create_table)
            elif not is_normal_load and not is_rebuild:
                raise RuntimeError(
                    "Invalid table status in redshift_copy: {}".format(
                        self._destination_table_status
                    )
                )

            # If there is no row updates, just skip copy and return
            if self.row_count == 0:
                return

            cur.execute("BEGIN TRANSACTION;")
            # Lock the table early to avoid deadlocks in many-to-one pipelines.
            query = generate_lock_query(destination_table)
            cur.execute(query)

            query = generate_drop_exists_query(staging_table)
            cur.execute(query)

            if is_rebuild:
                # Build staging table anew and grant it appropriate permissions
                self.logger.info(
                    "Creating staging table to rebuild %s", destination_table
                )
                create_staging_table = self.query_to_redshift_create_table(
                    self.get_query_sql(), staging_table
                )
                permissions_sql = self.get_grant_sql(cur)
                cur.execute(create_staging_table)
                if permissions_sql:
                    self.logger.info(
                        "Copying permissions onto %s:\n%s",
                        staging_table,
                        permissions_sql,
                    )
                    cur.execute(permissions_sql)
            else:
                # If not rebuilding, create staging with LIKE
                self.logger.info("Creating staging table %s", staging_table)
                query = generate_create_table_like_query(
                    staging_table, destination_table
                )
                cur.execute(query)

            # Issuing Copy Command
            self.logger.info("Issuing copy command")
            query = generate_copy_query(
                staging_table,
                self.copy_target_url,
                get_redshift().iam_copy_role,
                self.manifest_mode,
            )
            self.logger.debug(query)
            cur.execute(query)

            # Row delete and count logic
            if is_rebuild or (self.append_only and not self.full_refresh):
                self.rows_deleted = 0
            else:
                cur.execute(delete_clause)
                self.rows_deleted = cur.rowcount

            # Row insert and count logic
            if is_rebuild:
                self.logger.info("Swapping staging table into %s", destination_table)
                # DNE overrides rebuild, so we can assume the table exists
                query = generate_drop_query(destination_table)
                cur.execute(query)
                query = generate_rename_query(staging_table, destination_table)
                cur.execute(query)
                query = generate_count_query(destination_table)
                cur.execute(query)
                self.rows_inserted = cur.fetchall()[0]
            else:
                query = generate_insert_all_query(staging_table, destination_table)
                cur.execute(query)
                self.rows_inserted = cur.rowcount
                query = generate_drop_query(staging_table)
                cur.execute(query)
            cur.execute("END TRANSACTION;")
            self.register_and_cleanup()

    def register_and_cleanup(self):
        # Register in index table.
        self.set_last_updated_index()

        # Register in monitor table
        # self.endtime = datetime.datetime.utcnow()
        # self.register_load_monitor()

        # Clean up S3
        for key in self.data_file_keys():
            self.s3.delete_object(Bucket=get_redshift().s3_config.bucket, Key=key)

        if self.manifest_mode:
            self.s3.delete_object(
                Bucket=get_redshift().s3_config.bucket, Key=self.manifest_s3_data_key()
            )
