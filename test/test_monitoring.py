import unittest

import time

from druzhba.monitoring import MonitoringProvider, EventState


class TestMonitoringProvider(MonitoringProvider):
    def __init__(self):
        self.calls = []
        super().__init__()

    def reset(self):
        self.calls = []

    def on_event(self, event, event_state, **kwargs):
        self.calls.append((event, event_state, kwargs))


class MonitoringCallbackTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        self.monitoring = TestMonitoringProvider()
        super().__init__(*args, **kwargs)

    def setup(self):
        self.monitoring.reset()

    def test_log_event(self):
        self.monitoring.on_event("foo", EventState.COMPLETE)
        self.assertEqual(
            self.monitoring.calls[0],
            ("foo", EventState.COMPLETE, {}))

    def test_wrap_event(self):
        x = 0
        with self.monitoring.wrap("foo", db_alias="test"):
            # just do anything here
            time.sleep(.3)
            x = x + 1

        self.assertEqual(x, 1)
        self.assertEqual(len(self.monitoring.calls), 2)

        enter = self.monitoring.calls[0]
        self.assertEqual(enter[:-1], ("foo", EventState.START))
        self.assertEqual(enter[2]["db_alias"], "test")

        exit = self.monitoring.calls[1]
        self.assertEqual(exit[:-1], ("foo", EventState.COMPLETE))
        self.assertEqual(exit[2]["db_alias"], "test")
        self.assertGreaterEqual(exit[2]["et"], .250)
        self.assertLessEqual(exit[2]["et"], .350)

    def test_wrap_failure(self):
        x = 42
        with self.assertRaises(ZeroDivisionError):
            with self.monitoring.wrap("foo", db_alias="test"):
                # raise an error
                x = x / 0

        self.assertEqual(x, 42)
        self.assertEqual(len(self.monitoring.calls), 2)

        enter = self.monitoring.calls[0]
        self.assertEqual(enter[:-1], ("foo", EventState.START))
        self.assertEqual(enter[2]["db_alias"], "test")

        exit = self.monitoring.calls[1]
        self.assertEqual(exit[:-1], ("foo", EventState.ERROR))
        self.assertEqual(exit[2]["db_alias"], "test")
        self.assertIs(type(exit[2]["ex"]), ZeroDivisionError)

    def test_timer(self):
        @self.monitoring.timer("foo")
        def my_test_func(ms):
            time.sleep(ms / 1000.)
            return 42

        ret = my_test_func(300)
        self.assertEqual(ret, 42)

        self.assertEqual(len(self.monitoring.calls), 2)

        enter = self.monitoring.calls[0]
        self.assertEqual(enter, ("foo", EventState.START, {}))

        exit = self.monitoring.calls[1]
        self.assertEqual(exit[:-1], ("foo", EventState.COMPLETE))
        self.assertGreaterEqual(exit[2]["et"], .250)
        self.assertLessEqual(exit[2]["et"], .350)
