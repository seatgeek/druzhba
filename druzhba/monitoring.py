import logging
import logging.config
import os
from contextlib import contextmanager
from enum import Enum, unique
from time import perf_counter

import sentry_sdk as sentry
import statsd
from sentry_sdk.integrations.logging import LoggingIntegration


def configure_logging(args):
    if args.log_level:
        log_level = args.log_level
    else:
        log_level = os.getenv("LOG_LEVEL", "INFO")

    settings = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "normal": {
                "format": "[%(asctime)s.%(msecs)03d] %(name)s [pid:%(process)s] - %(levelname)s - %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            }
        },
        "handlers": {
            "console": {
                "level": log_level,
                "class": "logging.StreamHandler",
                "formatter": "normal",
                "stream": "ext://sys.stdout",
            }
        },
        "loggers": {"druzhba": {"level": log_level, "handlers": ["console"]},},
    }
    logging.config.dictConfig(settings)


def init_sentry():
    dsn = os.getenv("SENTRY_DSN")

    if dsn is not None:
        sentry_logging = LoggingIntegration(
            level=logging.INFO, event_level=logging.WARNING
        )

        sentry.init(
            dsn=dsn,
            integrations=[sentry_logging],
            environment=os.getenv("SENTRY_ENVIRONMENT"),
            release=os.getenv("SENTRY_RELEASE"),
        )


class FakeStatsd(object):
    @contextmanager
    def timer(self, *args, **kwargs):
        # pylint: disable=unused-argument
        try:
            yield None
        finally:
            pass

    def timing(self, *args, **kwargs):
        pass

    def incr(self, *args, **kwargs):
        pass


def get_statsd_client():
    host = os.getenv("STATSD_HOST")
    port = os.getenv("STATSD_PORT")
    prefix = os.getenv("STATSD_PREFIX")
    if host and port:
        return statsd.StatsClient(host, port, prefix)
    else:
        return FakeStatsd()


@unique
class EventState(Enum):
    START = 1
    ERROR = 2
    COMPLETE = 3


class MonitoringProvider(object):
    """Provides real time monitoring of Druzhba

    The MonitoringProvider allows for custom monitoring of a running Druzhba
    process. The default implementation utilizes Statsd to track table and
    database extraction times and throughput.

    To implement a custom monitoring operation subclass MonitoringProvider and
    override the on_event method.
    """

    def __init__(self):
        pass

    def on_event(self, event, event_state, **kwargs):
        """Monitoring event handler

        Override to implement custom monitoring, logging, or stats collection

        At critical points within the life cycle of a Druzhba the application
        will invoke the on_event method of the current application's
        MonitoringProvider. It will be passed appropriate context based on when
        it was invoked by the application

        event: str
            The name of the event. See [events] in the docs for a list of event
            names

        event_state: EventState
            The state of the event started, completed, or errored

        db_alias: str (optional)
            The name of the database currently being processed

        table: str (optional)
            The name of the table currently being processed

        error: Exception (optional)
            The exception tha occurred within the relevant processing step. This
            exception is passed for logging or monitoring purposes and will be
            reraised within the Druzhba application after the on_event method
            is invoked.
        """

    @contextmanager
    def wrap(self, event, **kwargs):
        was_error = False

        self.on_event(event, EventState.START, **kwargs)
        started = perf_counter()

        try:
            yield None
        except Exception as ex:
            was_error = True
            et = perf_counter() - started
            kwargs.update({"et": et, "ex": ex})
            self.on_event(event, EventState.ERROR, **kwargs)
            raise
        finally:
            if not was_error:
                et = perf_counter() - started
                kwargs.update({"et": et})
                self.on_event(event, EventState.COMPLETE, **kwargs)

    def timer(self, event, **t_kwargs):
        def decorator(func):
            def wrapper(*args, **f_kwargs):
                with self.wrap(event, **t_kwargs):
                    out = func(*args, **f_kwargs)
                return out

            return wrapper

        return decorator

    def record_event(self, event, **kwargs):
        self.on_event(event, EventState.COMPLETE, **kwargs)

    def record_error(self, event, **kwargs):
        self.on_event(event, EventState.ERROR, **kwargs)


class DefaultMonitoringProvider(MonitoringProvider):
    """Implements monitoring for Druzhba application process

    The default monitoring includes a statsd integration that will, if
    STATSD_HOST and STATSD_PORT environment variables are defined, post run
    metadata to the appropriate statsd instance.  If you would like to implement
    custom monitoring and also include statsd behavior, subclass this class. If
    you would prefer to not include default statsd behavior subclass
    MonitoringProvider directly"""

    def __init__(self):
        super(DefaultMonitoringProvider).__init__()
        self.statsd_client = get_statsd_client()

    def on_event(self, event, event_state, **kwargs):
        db_alias = kwargs.get("db_alias")
        table = kwargs.get("table")
        elapsed_time = kwargs.get("et")

        if db_alias and table:
            full_event_name = f"druzhba.db.{event}.{db_alias}.{table}"
        elif db_alias:
            full_event_name = f"druzhba.db.{event}.{db_alias}"
        else:
            full_event_name = f"druzhba.db.{event}"

        if event_state == EventState.COMPLETE:
            if event in ["extract-table", "create-redshift-table", "load-table"]:
                statsd_client.incr(full_event_name)
            if event in ["run-time", "full-run-time"]:
                statsd_client.timing(full_event_name, elapsed_time)

        if event == "disconnect-error":
            statsd_client.incr(f"druzhba.db.{event}.{db_alias}")


statsd_client = get_statsd_client()
init_sentry()
