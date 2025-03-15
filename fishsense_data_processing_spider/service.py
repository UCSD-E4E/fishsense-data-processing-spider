'''Main Service Entry Point
'''
import datetime as dt
import logging
import time
from pathlib import Path

import psycopg
import psycopg.rows
from prometheus_client import start_http_server

from fishsense_data_processing_spider.backend import get_file_checksum
from fishsense_data_processing_spider.config import (POSTGRES_CONNECTION_STR,
                                                     configure_logging,
                                                     settings)
from fishsense_data_processing_spider.metrics import (get_counter, get_summary,
                                                      system_monitor_thread)


def load_query(path: Path) -> str:
    """Loads query from path

    Args:
        path (Path): Path to query file

    Returns:
        str: Query contents
    """
    with open(path, 'r', encoding='utf-8') as handle:
        return handle.read(int(1e9))

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
            subsystem='spider'
        )
        images_added = get_counter(
            'images_added',
            'Number of images added',
            namespace='e4efs',
            subsystem='spider'
        )
        for image in images:
            # Compute checksum
            cksum = get_file_checksum(image)
            dive = image.parent.relative_to(data_root).as_posix()
            image_key = image.relative_to(data_root).as_posix()
            image_counter.inc()
            with psycopg.connect(POSTGRES_CONNECTION_STR,
                                 row_factory=psycopg.rows.dict_row) as con, \
                    con.cursor() as cur:
                # See if image already is known
                with query_timer.labels('select_image_by_path').time():
                    result = cur.execute(
                        query=load_query('sql/select_image_by_path.sql'),
                        params={
                            'path': image_key
                        }
                    ).fetchall()
                if len(result) == 1:
                    continue
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
                        query=load_query('sql/insert_image_path_dive_md5.sql'),
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

    def run(self):
        """Main entry point
        """
        start_http_server(9090)
        system_monitor_thread.start()
        while True:
            last_run = dt.datetime.now()
            next_run: dt.datetime = last_run + settings.scraper.interval

            try:
                self.__process_dirs()
            except Exception:  # pylint: disable=broad-except
                pass

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
