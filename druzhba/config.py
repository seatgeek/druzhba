import os
import re

import yaml

CONFIG_DIR = os.getenv("DRUZHBA_CONFIG_DIR")


class ConnectionConfig(object):
    # Note: pymysq defaults port to 3306 if 0 supplied
    #       pyscopg2 defaults port to 5432 if omitted from kwargs
    #       pymssql defaults to port 1433 if not provided
    def __init__(self, config):
        self.host = config.get("host")
        self.port = config.get("port")
        self.user = config.get("user")
        self.password = config.get("password")
        self.database = config.get("database")
        self.url = config.get("url")

    @property
    def params(self):
        if self.url:
            return {"dsn": self.url}
        elif self.host and self.user:
            return {
                "host": self.host,
                "port": self.port,
                "database": self.database,
                "user": self.user,
                "password": self.password,
            }
        else:
            raise ValueError("Required connection parameters not set")


class S3Config(object):
    def __init__(self, s3_config):
        self.bucket = s3_config.get("bucket", "").replace("s3://", "")
        self.prefix = s3_config.get("prefix", "")


class RedshiftConfig(object):
    """Either a URL or a host/port/database/user may be used."""

    def __init__(self, destination_config):
        self.iam_copy_role = destination_config.get("iam_copy_role")
        self.redshift_cert_path = destination_config.get("redshift_cert_path")
        self.s3_config = S3Config(destination_config.get("s3"))
        self.connection_config = ConnectionConfig(destination_config["connection"])

    @property
    def connection_params(self):
        return self.connection_config.params


def load_destination_config(config_dir):
    return load_config_file(os.path.join(config_dir, "_pipeline.yaml"))


class SourceConfig(object):
    pass


def load_config_file(filename):
    """Loads a Druzhba config file and process env var substitution

    Druzhba uses YAML configuration files for both sources and destination
    databases. The YAML is post processed to perform environment variable
    substitution. Patterns of the format ${ENV_VAR} are substituted with the
    content of the the ENV_VAR environment variable.

    If ENV_VAR is not set in the environment then it is replaced with an empty
    string and the missing_vars flag will be set. The caller can determine if
    this state represents an error.

    Note: substitutions only occur within string values.

    :param str filename: The path to the config file to be loaded
    :returns:
        - conf (:py:class:`dict`) - The parsed config dictionary
        - missing_vars (:py:class:`set`) - Set of missing environment variables.
            Empty set indicates no errors in loading the config environment
    """
    with open(filename, "r") as f:
        config = yaml.safe_load(f)
    return _parse_config(config)


def _parse_config(config):
    """Expected to be called within load_config_file
    A description of operation can be found there"""

    def _parse_element(config_element):
        if type(config_element) == dict:
            return _parse_dict_config(config_element)
        elif type(config_element) == list:
            return _parse_list_config(config_element)
        elif type(config_element) == str:
            return _parse_string_config(config_element)
        else:
            # Do not process elements of unknown type
            return config_element, set()

    def _parse_string_config(raw_string_value):
        output_string = raw_string_value

        missing = set()
        matches = re.findall(r"\${(\w+)}", raw_string_value)

        for match in matches:
            env_val = os.getenv(match)
            if env_val is not None:
                output_string = output_string.replace(f"${{{match}}}", env_val)
            else:
                output_string = output_string.replace(f"${{{match}}}", "")
                missing.add(match)

        return output_string, missing

    def _parse_dict_config(config):
        subbed_config = {}
        missing = set()
        for key in config:
            new_val, new_errors = _parse_element(config[key])
            subbed_config[key] = new_val
            missing.update(new_errors)
        return subbed_config, missing

    def _parse_list_config(config):
        subbed_config = []
        missing = set()
        for elem in config:
            new_val, new_errors = _parse_element(elem)
            subbed_config.append(new_val)
            missing.update(new_errors)
        return subbed_config, missing

    return _parse_element(config)
