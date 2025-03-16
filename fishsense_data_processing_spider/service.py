'''Main Service Entry Point
'''
import datetime as dt
import itertools
import logging
import time
from pathlib import Path
from threading import Thread
from typing import Any, Dict, Optional, Union

import psycopg
import psycopg.rows
from prometheus_client import start_http_server

from fishsense_data_processing_spider.backend import (
    get_camera_sns, get_dive_checksum_from_query, get_file_checksum)
from fishsense_data_processing_spider.config import (PG_CONN_STR,
                                                     configure_logging,
                                                     settings)
from fishsense_data_processing_spider.metrics import (
    add_thread_to_monitor, get_counter, get_gauge, get_summary,
    remove_thread_from_monitor, system_monitor_thread)


def load_query(path: Path) -> str:
    """Loads query from path

    Args:
        path (Path): Path to query file

    Returns:
        str: Query contents
    """
    with open(path, 'r', encoding='utf-8') as handle:
        return handle.read(int(1e9))


def do_query(path: Union[Path, str], cur: psycopg.Cursor, params: Optional[Dict[str, Any]] = None):
    """Convenience function to time and execute a query

    Args:
        path (Union[Path, str]): Path to query file
        cur (psycopg.Cursor): Cursor
        params (Optional[Dict[str, Any]]): Query parameters.  Defaults to None
    """
    path = Path(path)
    query_timer = get_summary(
        'query_duration',
        'SQL Query Duration',
        labelnames=['query'],
        namespace='e4efs',
        subsystem='spider'
    )
    with query_timer.labels(query=path.stem).time():
        cur.execute(
            query=load_query(path),
            params=params
        )

class Service:
    """Service class
    """
    # pylint: disable=too-few-public-methods
    # Main entry point
    def __init__(self):
        self.__validate_data_paths()

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

    def __discover_dives(self, data_root: Path):
        __log = logging.getLogger('Dive Discovery')
        images = data_root.rglob('*.ORF', case_sensitive=False)
        query_timer = get_summary(
            'query_duration',
            'SQL Query Duration',
            labelnames=['query'],
            namespace='e4efs',
            subsystem='spider'
        )
        image_counter = get_counter(
            'images_processed',
            'Number of images processed',
            namespace='e4efs',
            subsystem='spider',
            labelnames=['phase']
        )
        images_added = get_counter(
            'images_added',
            'Number of images added',
            namespace='e4efs',
            subsystem='spider'
        )
        for image_batch in itertools.batched(images, n=128):
            # Compute checksum
            image_keys = [image.relative_to(
                data_root).as_posix() for image in image_batch]
            image_counter.labels(phase='discover_dives').inc(len(image_keys))
            with psycopg.connect(PG_CONN_STR,
                                 row_factory=psycopg.rows.dict_row) as con, \
                    con.cursor() as cur:
                # See if image already is known
                with query_timer.labels('select_image_by_path').time():
                    cur.executemany(
                        query=load_query('sql/select_image_by_path.sql'),
                        params_seq=[{
                            'path': image_key
                        } for image_key in image_keys],
                        returning=True
                    )
                results = []
                while True:
                    results.append(cur.fetchone())
                    if not cur.nextset():
                        break
                for idx, result in enumerate(results):
                    if result is not None:
                        continue
                    image = image_batch[idx]
                    image_key = image_keys[idx]
                    # do heavy lifting
                    cksum = get_file_checksum(image)
                    dive = image.parent.relative_to(data_root).as_posix()
                    # Dive is probably not known yet, add
                    with query_timer.labels('insert_dive_path').time():
                        cur.execute(
                            query=load_query('sql/insert_dive_path.sql'),
                            params={
                                'path': dive
                            }
                        )
                        con.commit()

                    with query_timer.labels('insert_image_path_dive_md5').time():
                        cur.execute(
                            query=load_query(
                                'sql/insert_image_path_dive_md5.sql'),
                            params={
                                'path': image_key,
                                'dive': dive,
                                'cksum': cksum
                            }
                        )
                        con.commit()
                    images_added.inc()

    def __process_dirs(self):
        for data_dir in settings.scraper.data_paths:
            data_path = Path(data_dir)
            self.__discover_dives(data_path)

        with psycopg.connect(PG_CONN_STR,
                             row_factory=psycopg.rows.dict_row) as con, \
                con.cursor() as cur:
            # For each dive
            do_query('sql/select_all_dives.sql', cur)
            dives = [row['path'] for row in cur.fetchall()]
            for dive_path in dives:
                do_query(
                    path='sql/select_images_in_dive.sql',
                    cur=cur,
                    params={
                        'dive': dive_path
                    }
                )
                result = cur.fetchall()
                cksum = get_dive_checksum_from_query(result)
                do_query(
                    path='sql/update_dive_cksum.sql',
                    cur=cur,
                    params={
                        'cksum': cksum,
                        'path': dive_path
                    }
                )

    def __compute_camera_sns(self, batch_size=1024):
        query_timer = get_summary(
            'query_duration',
            'SQL Query Duration',
            labelnames=['query'],
            namespace='e4efs',
            subsystem='spider'
        )
        image_counter = get_counter(
            'images_processed',
            'Number of images processed',
            namespace='e4efs',
            subsystem='spider',
            labelnames=['phase']
        )
        query_result_length = get_summary(
            'query_result_length',
            'SQL Query Result Length',
            labelnames=['query'],
            namespace='e4efs',
            subsystem='spider'
        )
        while True:
            with psycopg.connect(PG_CONN_STR, row_factory=psycopg.rows.dict_row) as con, \
                    con.cursor() as cur:
                with query_timer.labels(query='select_images_without_camerasn').time():
                    cur.execute(
                        query=load_query(
                            'sql/select_images_without_camerasn.sql'),
                        params={
                            'limit': batch_size
                        }
                    )
                results = cur.fetchall()
                if len(results) == 0:
                    return
                query_result_length.labels(
                    query='select_images_without_camerasn').observe(len(results))
                image_counter.labels(phase='camera_sns').inc(len(results))
                images = {result['cksum']: Path(
                    result['data_path']) / result['path'] for result in results}
                serial_numbers = get_camera_sns(images)

                with query_timer.labels(query=f'update_image_camerasn_x{batch_size}').time():
                    cur.executemany(
                        query=load_query('sql/update_image_camerasn.sql'),
                        params_seq=[
                            {
                                'camera_sn': camera_sn,
                                'cksum': cksum
                            }
                            for cksum, camera_sn in serial_numbers.items()
                        ]
                    )
                con.commit()

    def __summary_thread(self):
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
            with psycopg.connect(PG_CONN_STR, row_factory=psycopg.rows.dict_row) as con, \
                    con.cursor() as cur:
                last_run = dt.datetime.now()
                next_run = last_run + settings.summary.interval
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

    def run(self):
        """Main entry point
        """
        start_http_server(9090)
        system_monitor_thread.start()
        summary_thread = Thread(
            target=self.__summary_thread,
            name='summary_thread',
            daemon=True
        )
        summary_thread.start()
        add_thread_to_monitor(summary_thread)

        while True:
            last_run = dt.datetime.now()
            next_run: dt.datetime = last_run + settings.scraper.interval
            process_dir_thread = Thread(
                target=self.__process_dirs, name='process_dirs')
            add_thread_to_monitor(process_dir_thread)
            camera_sn_thread = Thread(
                target=self.__compute_camera_sns, name='camera_sns')
            add_thread_to_monitor(camera_sn_thread)

            process_dir_thread.start()
            camera_sn_thread.start()

            process_dir_thread.join()
            remove_thread_from_monitor(process_dir_thread)
            camera_sn_thread.join()

            remove_thread_from_monitor(camera_sn_thread)

            time_to_sleep = (next_run - dt.datetime.now()).total_seconds()
            if time_to_sleep > 0:
                time.sleep(time_to_sleep)

def main():
    """Main entry point
    """
    configure_logging()
    Service().run()

if __name__ == '__main__':
    main()
