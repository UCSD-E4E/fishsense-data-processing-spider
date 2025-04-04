'''Threading interface
'''
import datetime as dt
import logging
from threading import Event, Thread

from fishsense_data_processing_spider.metrics import add_thread_to_monitor


class InstrumentedInterruptibleIntervalThread(Thread):
    """Instrumented Interruptible Interval Thread

    """
    def __init__(self,
                 group=None,
                 target=None,
                 name=None,
                 args=...,
                 kwargs=None,
                 interval: dt.timedelta = dt.timedelta(hours=1),
                 logger: logging.Logger = logging.getLogger(),
                 *,
                 daemon=None):
        # pylint: disable=too-many-arguments,too-many-positional-arguments
        super().__init__(group, target=self._run_loop, name=name, daemon=daemon)
        self._sleep_interrupt = Event()
        self._stop_event = Event()
        self._interval = interval
        self._log = logger
        self._target = target
        self._args = args
        self._kwargs = kwargs

    def start(self):
        add_thread_to_monitor(self)
        return super().start()

    def _run_loop(self):
        while not self._stop_event.is_set():
            last_run = dt.datetime.now()
            next_run = last_run + self._interval

            try:
                self._target(*self._args, **self._kwargs)
            except Exception as exc: # pylint: disable=broad-except
                self._log.exception(
                    'Thread %s failed due to %s', self.name, exc)

            self._sleep_interrupt.clear()
            time_to_sleep = (next_run - dt.datetime.now()).total_seconds()
            if time_to_sleep > 0:
                self._sleep_interrupt.wait(time_to_sleep)

    def join(self, timeout=None):
        self._sleep_interrupt.set()
        self._stop_event.set()
        return super().join(timeout)
