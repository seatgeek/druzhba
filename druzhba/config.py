import logging.config
import os

import statsd

CONFIG_DIR = os.getenv('DRUZHBA_CONFIG_DIR')


statsd_client = statsd.StatsClient()


class S3Config(object):
    bucket = os.getenv('S3_BUCKET', '').replace('s3://', '')
    prefix = os.getenv('S3_PREFIX', '')


class RedshiftConfig(object):
    host = os.getenv('DW_REDSHIFT_HOST')
    port = os.getenv('DW_REDSHIFT_PORT', 5439)
    database = os.getenv('DW_REDSHIFT_DATABASE')
    user = os.getenv('DW_REDSHIFT_USER')
    password = os.getenv('DW_REDSHIFT_PASSWORD')
    iam_copy_role = os.getenv('IAM_COPY_ROLE')
    redshift_cert_path = os.getenv('REDSHIFT_CERT_PATH')


def configure_logging():
    settings = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'normal': {
                'format': "[%(asctime)s.%(msecs)03d] %(name)s [pid:%(process)s] - %(levelname)s - %(message)s",
                'datefmt': "%Y-%m-%d %H:%M:%S"
            }
        },
        'handlers': {
            'console': {
                'level': 'INFO',
                'class': 'logging.StreamHandler',
                'formatter': 'normal',
                'stream': 'ext://sys.stdout'
            }
        },
        'loggers': {
            'druzhba': {
                'level': 'INFO',
                'handlers': ['console']
            },
        }
    }
    logging.config.dictConfig(settings)
