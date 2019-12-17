import os


CONFIG_DIR = os.getenv("DRUZHBA_CONFIG_DIR")


class S3Config(object):
    bucket = os.getenv("S3_BUCKET", "").replace("s3://", "")
    prefix = os.getenv("S3_PREFIX", "")


class RedshiftConfig(object):
    """Either a URL or a host/port/database/user may be used."""

    url = os.getenv("REDSHIFT_URL")

    host = os.getenv("REDSHIFT_HOST")
    port = os.getenv("REDSHIFT_PORT", 5439)
    database = os.getenv("REDSHIFT_DATABASE")
    user = os.getenv("REDSHIFT_USER")
    password = os.getenv("REDSHIFT_PASSWORD")

    iam_copy_role = os.getenv("IAM_COPY_ROLE")
    redshift_cert_path = os.getenv("REDSHIFT_CERT_PATH")
