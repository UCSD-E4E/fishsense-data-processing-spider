'''Main Service Entry Point
'''
import asyncio
import datetime as dt
import logging
import time
from pathlib import Path
from threading import Thread

import psycopg
import psycopg.rows
import pytz
import tornado
from prometheus_client import start_http_server
from rpyc.utils.server import ThreadedServer

from fishsense_data_processing_spider.config import (PG_CONN_STR,
                                                     configure_logging,
                                                     settings)
from fishsense_data_processing_spider.discovery import Crawler
from fishsense_data_processing_spider.endpoints import (HomePageHandler,
                                                        VersionHandler,
                                                        RetrieveBatch)
from fishsense_data_processing_spider.label_studio_sync import LabelStudioSync
from fishsense_data_processing_spider.metrics import (add_thread_to_monitor,
                                                      get_gauge, get_summary,
                                                      system_monitor_thread)
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
        self.__validate_data_paths()
        self.__label_studio = LabelStudioSync()
        self.__crawler = Crawler(
            data_paths=[
                Path(data_path)
                for data_path in settings.scraper.data_paths
            ],
            conn_str=PG_CONN_STR,
            interval=settings.scraper.interval,
        )

        self.stop_event = asyncio.Event()
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

        self.__webapp = tornado.web.Application([
            (r'/()', HomePageHandler, {'start_time': start_time}),
            (r'/version()', VersionHandler),
            (r'/retrieve_batch()', RetrieveBatch, {'key_store': self.__keystore})
        ])

    def __validate_data_paths(self):
        # This isn't working!  not sure why
        # path_validators = [Validator(
        #     'scraper.data_paths',
        #     condition=lambda x: all(Path(y).is_dir() for y in x)
        # )]
        # settings.validators.register(*path_validators)
        # settings.validators.validate()
        for data_dir in settings.scraper.data_paths:
            data_path = Path(data_dir)
            if not data_path.is_dir():
                raise RuntimeError('Data path is not a directory!')

    def __summary_loop(self):
        __log = logging.getLogger('summary')
        counts = get_gauge(
            'count',
            documentation='Counts',
            labelnames=['table'],
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
            with psycopg.connect(PG_CONN_STR, row_factory=psycopg.rows.dict_row) as con, \
                    con.cursor() as cur:
                try:
                    with query_timer.labels(query='count_images').time():
                        cur.execute('SELECT COUNT(*) FROM images;')
                    counts.labels(table='images').set(cur.fetchone()['count'])
                    with query_timer.labels(query='count_dives').time():
                        cur.execute('SELECT COUNT(*) FROM dives;')
                    counts.labels(table='dives').set(cur.fetchone()['count'])
                    with query_timer.labels(query='count_cdives').time():
                        cur.execute('SELECT COUNT(*) FROM canonical_dives;')
                    counts.labels(table='canonical_dives').set(
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

        await self.stop_event.wait()

        self.__crawler.stop()
        self.__label_studio.stop()


def main():
    """Main entry point
    """
    configure_logging()
    asyncio.run(Service().run())


if __name__ == '__main__':
    main()
