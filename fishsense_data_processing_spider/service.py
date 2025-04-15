'''Main Service Entry Point
'''
import asyncio
import datetime as dt
import json
import logging
import signal
import time
from pathlib import Path
from threading import Thread
from typing import Dict, List

import psycopg
import psycopg.rows
import pytz
import tornado
from prometheus_client import start_http_server
from rpyc.utils.server import ThreadedServer
from tornado.routing import URLSpec

from fishsense_data_processing_spider.config import (PG_CONN_STR,
                                                     configure_logging,
                                                     settings)
from fishsense_data_processing_spider.data_model import DataModel
from fishsense_data_processing_spider.discovery import Crawler
from fishsense_data_processing_spider.endpoints import (
    ApiKeyAdminHandler, DebugDataHandler, DoDiscoveryHandler,
    DoLabelStudioSyncHandler, FrameMetadataHandler, HeadTailLabelHandler,
    HomePageHandler, JobStatusHandler, LaserLabelHandler, LensCalHandler,
    NewKeyHandler, NotImplementedHandler, PreprocessJpegHandler,
    PreprocessLaserJpegHandler, RawDataHandler, RetrieveBatch, VersionHandler)
from fishsense_data_processing_spider.label_studio_sync import LabelStudioSync
from fishsense_data_processing_spider.metrics import (add_thread_to_monitor,
                                                      get_gauge, get_summary,
                                                      system_monitor_thread)
from fishsense_data_processing_spider.orchestrator import Orchestrator
from fishsense_data_processing_spider.rpyc_endpoint import CliService
from fishsense_data_processing_spider.web_auth import KeyStore


def quick_con() -> psycopg.Connection:
    """Convenience quick connect

    Returns:
        psycopg.Connection: Connection
    """
    return psycopg.connect(PG_CONN_STR, row_factory=psycopg.rows.dict_row)


class Service:
    """Service class
    """
    # pylint: disable=too-few-public-methods,too-many-instance-attributes
    # Main entry point

    def __init__(self):
        data_paths = self.__validate_data_paths()
        self.__label_studio = LabelStudioSync(
            root_url=settings.web_api.root_url,
            label_studio_host=settings.label_studio.host,
            label_studio_key=settings.label_studio.api_key,
            pg_conn_str=PG_CONN_STR
        )
        self._data_model = DataModel(
            data_path_mapping=data_paths,
            pg_conn_str=PG_CONN_STR,
            max_raw_data_file_size=settings.data_model.max_load_size,
            preprocess_jpeg_path=settings.data_model.preprocess_jpg_store,
            preprocess_laser_jpeg_path=settings.data_model.preprocess_laser_jpg_store,
            debug_data_path=settings.data_model.debug_data_store
        )

        self.__crawler = Crawler(
            data_paths=list(data_paths.values()),
            conn_str=PG_CONN_STR,
            interval=settings.scraper.interval,
        )

        self.stop_event = asyncio.Event()
        signal.signal(signal.SIGTERM, lambda x, y: self.stop_event.set())
        start_time = dt.datetime.now(tz=pytz.UTC)

        self.__summary_thread = Thread(
            target=self.__summary_loop,
            name='summary_thread',
            daemon=True
        )
        add_thread_to_monitor(self.__summary_thread)

        self.__keystore = KeyStore(settings.web_api.key_store)

        self.rpyc_endpoint = ThreadedServer(
            CliService(
                key_store=self.__keystore
            ),
            port=18861
        )
        self.rpyc_thread = Thread(
            target=self.rpyc_endpoint.start,
            name='rpyc',
            daemon=True
        )
        add_thread_to_monitor(self.rpyc_thread)

        self.__job_orchestrator = Orchestrator(
            pg_conn=PG_CONN_STR,
            reaper_interval=settings.orchestrator.reaper_interval
        )

        web_routes = [
            URLSpec(
                pattern=r'/$',
                handler=HomePageHandler,
                kwargs={'start_time': start_time}
            ),
            URLSpec(
                pattern=r'/version$',
                handler=VersionHandler
            ),
            URLSpec(
                pattern=r'/api/v1/jobs/retrieve_batch$',
                handler=RetrieveBatch,
                kwargs={
                    'key_store': self.__keystore,
                    'orchestrator': self.__job_orchestrator
                }
            ),
            URLSpec(
                pattern=r'/api/v1/jobs/status$',
                handler=JobStatusHandler,
                kwargs={
                    'key_store': self.__keystore,
                    'orchestrator': self.__job_orchestrator
                }
            ),
            URLSpec(
                pattern=r'/api/v1/data/raw/(?P<checksum>[a-z0-9]+)$',
                handler=RawDataHandler,
                kwargs={
                    'key_store': self.__keystore,
                    'data_model': self._data_model
                }
            ),
            URLSpec(
                pattern=r'/api/v1/data/raw$',
                handler=NotImplementedHandler
            ),
            URLSpec(
                pattern=r'/api/v1/data/preprocess_jpeg/([a-z0-9]+)$',
                handler=PreprocessJpegHandler,
                kwargs={
                    'key_store': self.__keystore,
                    'data_model': self._data_model
                }
            ),
            URLSpec(
                pattern=r'/api/v1/data/laser_jpeg/([a-z0-9]+)$',
                handler=PreprocessLaserJpegHandler,
                kwargs={
                    'key_store': self.__keystore,
                    'data_model': self._data_model
                }
            ),
            URLSpec(
                pattern=r'/api/v1/data/laser/([a-z0-9]+)$',
                handler=LaserLabelHandler,
                kwargs={
                    'key_store': self.__keystore,
                    'data_model': self._data_model
                }
            ),
            URLSpec(
                pattern=r'/api/v1/data/head_tail/([a-z0-9]+)$',
                handler=HeadTailLabelHandler,
                kwargs={
                    'key_store': self.__keystore,
                    'data_model': self._data_model
                }
            ),
            URLSpec(
                pattern=r'/api/v1/data/depth_cal/([a-z0-9]+)$',
                handler=NotImplementedHandler
            ),
            URLSpec(
                pattern=r'/api/v1/data/lens_cal/(?P<camera_id>\d+)$',
                handler=LensCalHandler,
                kwargs={
                    'key_store': self.__keystore,
                    'data_model': self._data_model
                }
            ),
            URLSpec(
                pattern=r'/api/v1/control/discover$',
                handler=DoDiscoveryHandler,
                kwargs={
                    'crawler': self.__crawler,
                    'key_store': self.__keystore
                }
            ),
            URLSpec(
                pattern=r'/api/v1/control/label_studio_sync$',
                handler=DoLabelStudioSyncHandler,
                kwargs={
                    'crawler': self.__crawler,
                    'label_studio': self.__label_studio
                }
            ),
            URLSpec(
                pattern=r'/api/v1/debug/(?P<job_id>[a-zA-Z0-9-]+)$',
                handler=DebugDataHandler,
                kwargs={
                    'key_store': self.__keystore,
                    'orchestrator': self.__job_orchestrator,
                    'data_model': self._data_model
                }
            ),
            URLSpec(
                pattern=r'/api/v1/admin/scope$',
                handler=ApiKeyAdminHandler,
                kwargs={
                    'key_store': self.__keystore
                }
            ),
            URLSpec(
                pattern=r'/api/v1/admin/new_key$',
                handler=NewKeyHandler,
                kwargs={
                    'key_store': self.__keystore
                }
            ),
            URLSpec(
                pattern=r'/api/v1/metadata/frame/(?P<checksum>[a-z0-9]+)$',
                handler=FrameMetadataHandler,
                kwargs={
                    'key_store': self.__keystore,
                    'data_model': self._data_model
                }
            )
        ]

        self.__webapp = tornado.web.Application(web_routes)

    def __validate_data_paths(self) -> Dict[Path, Path]:
        # This isn't working!  not sure why
        # path_validators = [Validator(
        #     'scraper.data_paths',
        #     condition=lambda x: all(Path(y).is_dir() for y in x)
        # )]
        # settings.validators.register(*path_validators)
        # settings.validators.validate()
        with open(settings.scraper.data_paths, 'r', encoding='utf-8') as handle:
            data_path_mappings: List[Dict[str, str]] = json.load(handle)[
                'data_paths']
        data_paths = {Path(mapping['unc_path']): Path(mapping['mount'])
                      for mapping in data_path_mappings}
        for data_dir in data_paths.values():
            if not data_dir.is_dir():
                raise RuntimeError('Data path is not a directory!')
        return data_paths

    def __summary_loop(self):
        __log = logging.getLogger('summary')
        counts = get_gauge(
            'count',
            documentation='Counts',
            labelnames=['metric'],
            namespace='e4efs',
            subsystem='spider'
        )
        query_timer = get_summary(
            'query_duration',
            'SQL Query Duration',
            labelnames=['query'],
            namespace='e4efs',
            subsystem='spider'
        )
        while True:
            last_run = dt.datetime.now()
            next_run = last_run + settings.summary.interval
            tables = [
                'images',
                'dives',
                'canonical_dives',
                'laser_labels',
                'headtail_labels',
                'jobs'
            ]
            with psycopg.connect(PG_CONN_STR, row_factory=psycopg.rows.dict_row) as con, \
                    con.cursor() as cur:
                try:
                    for table in tables:
                        with query_timer.labels(query=f'count_{table}').time():
                            cur.execute(f'SELECT COUNT(*) FROM {table};')
                        counts.labels(metric=table).set(
                            cur.fetchone()['count'])
                    with query_timer.labels(query='count_complete_laser_labels').time():
                        cur.execute(
                            'SELECT COUNT(*) FROM laser_labels WHERE complete = TRUE;')
                    counts.labels(metric='complete_laser_labels').set(
                        cur.fetchone()['count'])
                    with query_timer.labels(query='count_complete_headtail_labels').time():
                        cur.execute(
                            'SELECT COUNT(*) FROM headtail_labels WHERE complete = TRUE;')
                    counts.labels(metric='complete_headtail_labels').set(
                        cur.fetchone()['count'])
                except Exception as exc:  # pylint: disable=broad-except
                    __log.exception('Summary thread failed due to %s', exc)
            time_to_sleep = (next_run - dt.datetime.now()).total_seconds()
            if time_to_sleep > 0:
                time.sleep(time_to_sleep)

    async def run(self):
        """Main entry point
        """
        start_http_server(9090)
        self.__webapp.listen(80)

        system_monitor_thread.start()
        self.__summary_thread.start()
        self.rpyc_thread.start()

        self.__label_studio.run()
        self.__crawler.run()
        self.__job_orchestrator.start()

        await self.stop_event.wait()

        self.__job_orchestrator.stop()
        self.__crawler.stop()
        self.__label_studio.stop()


def main():
    """Main entry point
    """
    configure_logging()
    asyncio.run(Service().run())


if __name__ == '__main__':
    main()
