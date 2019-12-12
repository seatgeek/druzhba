from contextlib import contextmanager
from enum import Enum, unique
import os
import threading

import statsd
from time import perf_counter


class FakeStatsd(object):
    @contextmanager
    def timer(self, *args, **kwargs):
        try:
            yield None
        finally:
            pass

    def incr(self, *args, **kwargs):
        pass


def get_statsd_client():
    host = os.getenv("STATSD_HOST")
    port = os.getenv("STATSD_PORT")
    if host and port:
        return statsd.StatsClient(host, port)
    else:
        return FakeStatsd()


statsd_client = get_statsd_client()


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
            The exception tha occured within the relevant processing step. This
            exception is passed for logging or monitoring purposes and will be
            reraised within the Druzhba application after the on_event method
            is invoked.
        """
        pass

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
        if event_state == EventState.COMPLETE:
            if event in ["extract-table", "create-redshift-table", "load-table"]:
                statsd_client.incr(f"druzhba.db.{event}.{db_alias}")
            if event in ["full-run-time", "run-time"]:
                statsd_client.timing(event, 1e6 * kwargs["et"])

        if event == "disconnect-error":
            statsd_client.incr(f"druzhba.db.disconnect-error.{db_alias}")

        self.on_event(event, event_state, db_alias=db_alias, table=table)
