'''Prometheus Metrics
'''
from importlib.metadata import version
from threading import Lock, Thread
from time import sleep
from typing import Dict, Iterable, List, Literal, Optional, Sequence, Union

from prometheus_client import (REGISTRY, CollectorRegistry, Counter, Gauge,
                               Histogram, Info, Summary)

__all_gauges: Dict[str, Gauge] = {}
__gauges_lock = Lock()
__all_infos: Dict[str, Info] = {}
__infos_lock = Lock()
__all_summaries: Dict[str, Summary] = {
    'query_duration': Summary(
        name='query_duration',
        documentation='SQL Query Duration',
        labelnames=['query'],
        namespace='e4efs',
        subsystem='spider'
    )
}
__summariess_lock = Lock()
__all_histograms: Dict[str, Histogram] = {}
__histograms_lock = Lock()
__all_counters: Dict[str, Counter] = {}
__counters_lock = Lock()


def get_histogram(name: str,
                  documentation: str,
                  labelnames: Iterable[str] = (),
                  namespace: str = '',
                  subsystem: str = '',
                  unit: str = '',
                  registry: Optional[CollectorRegistry] = REGISTRY,
                  _labelvalues: Optional[Sequence[str]] = None,
                  buckets: Sequence[Union[float, str]
                                    ] = Histogram.DEFAULT_BUCKETS,
                  ) -> Histogram:
    # pylint: disable=too-many-positional-arguments,too-many-arguments,missing-function-docstring
    # Mirrors prometheus API
    with __histograms_lock:
        if name not in __all_histograms:
            __all_histograms[name] = Histogram(name,
                                               documentation,
                                               labelnames,
                                               namespace,
                                               subsystem,
                                               unit,
                                               registry,
                                               _labelvalues,
                                               buckets)
        return __all_histograms[name]


def get_summary(name: str,
                documentation: str = '',
                labelnames: Iterable[str] = (),
                namespace: str = '',
                subsystem: str = '',
                unit: str = '',
                registry: Optional[CollectorRegistry] = REGISTRY,
                _labelvalues: Optional[Sequence[str]] = None,
                ) -> Summary:
    # pylint: disable=too-many-positional-arguments,too-many-arguments,missing-function-docstring
    # Mirrors prometheus API
    with __summariess_lock:
        if name not in __all_summaries:
            __all_summaries[name] = Summary(name,
                                            documentation,
                                            labelnames,
                                            namespace,
                                            subsystem,
                                            unit,
                                            registry,
                                            _labelvalues)
        return __all_summaries[name]


def get_counter(name: str,
                documentation: str,
                labelnames: Iterable[str] = (),
                namespace: str = '',
                subsystem: str = '',
                unit: str = '',
                registry: Optional[CollectorRegistry] = REGISTRY,
                _labelvalues: Optional[Sequence[str]] = None) -> Counter:
    # pylint: disable=too-many-positional-arguments,too-many-arguments,missing-function-docstring
    # Mirrors prometheus API
    with __counters_lock:
        if name not in __all_counters:
            __all_counters[name] = Counter(
                name=name,
                documentation=documentation,
                labelnames=labelnames,
                namespace=namespace,
                subsystem=subsystem,
                unit=unit,
                registry=registry,
                _labelvalues=_labelvalues
            )
        return __all_counters[name]


def get_gauge(name: str,
              documentation: str,
              labelnames: Iterable[str] = (),
              namespace: str = '',
              subsystem: str = '',
              unit: str = '',
              registry: Optional[CollectorRegistry] = REGISTRY,
              _labelvalues: Optional[Sequence[str]] = None,
              multiprocess_mode: Literal['all', 'liveall', 'min', 'livemin', 'max',
                                         'livemax', 'sum', 'livesum', 'mostrecent',
                                         'livemostrecent'] = 'all',
              ) -> Gauge:
    # pylint: disable=too-many-positional-arguments,too-many-arguments,missing-function-docstring
    # Mirrors prometheus API
    with __gauges_lock:
        if name not in __all_gauges:
            __all_gauges[name] = Gauge(name,
                                       documentation,
                                       labelnames,
                                       namespace,
                                       subsystem,
                                       unit,
                                       registry,
                                       _labelvalues,
                                       multiprocess_mode)
        return __all_gauges[name]


def get_info(name: str,
             documentation: str,
             labelnames: Iterable[str] = (),
             namespace: str = '',
             subsystem: str = '',
             unit: str = '',
             registry: Optional[CollectorRegistry] = REGISTRY,
             _labelvalues: Optional[Sequence[str]] = None,) -> Info:
    # pylint: disable=too-many-positional-arguments,too-many-arguments,missing-function-docstring
    # Mirrors prometheus API
    with __infos_lock:
        if name not in __all_infos:
            __all_infos[name] = Info(name,
                                     documentation,
                                     labelnames,
                                     namespace,
                                     subsystem,
                                     unit,
                                     registry,
                                     _labelvalues)
        return __all_infos[name]


__threads_to_monitor: List[Thread] = []


def add_thread_to_monitor(thread: Thread) -> None:
    """Adds thread to the monitoring list

    Args:
        thread (Thread): Thread to monitor
    """
    __threads_to_monitor.append(thread)


def remove_thread_from_monitor(thread: Thread) -> None:
    """Removes thread from monitoring list

    Args:
        thread (Thread): Thread to stop monitoring
    """
    __threads_to_monitor.remove(thread)


def __system_monitor_loop():

    wd_info = get_info(
        'info', 'E4E FishSense Data Processing Spider Info',
        namespace='e4efs',
        subsystem='spider')
    wd_info.info({
        'program_version': version('fishsense_data_processing_spider')
    })

    thread_monitor = get_gauge(
        'e4efs_spider_thread_alive', 'Thread Alive Status', ['thread'])

    while True:
        for thread in __threads_to_monitor:
            thread_monitor.labels(thread=thread.name).set(thread.is_alive())
        sleep(1)


system_monitor_thread = Thread(
    target=__system_monitor_loop, name='system_metrics_monitor', daemon=True)
