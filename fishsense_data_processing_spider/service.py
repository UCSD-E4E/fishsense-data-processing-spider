'''Main Service Entry Point
'''
import datetime as dt
import time
from pathlib import Path

import psycopg
import psycopg.rows

from fishsense_data_processing_spider.backend import get_file_checksum
from fishsense_data_processing_spider.config import (POSTGRES_CONNECTION_STR,
                                                     configure_logging,
                                                     settings)


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
        images = data_root.rglob('*.ORF', case_sensitive=False)
        for image in images:
            # Compute checksum
            cksum = get_file_checksum(image)
            dive = image.parent.relative_to(data_root).as_posix()
            with psycopg.connect(POSTGRES_CONNECTION_STR,
                                 row_factory=psycopg.rows.dict_row) as con, \
                    con.cursor() as cur:

                # Dive isn't known yet, add
                cur.execute(
                    query=load_query('sql/insert_dive_path.sql'),
                    params={
                        'path': dive
                    }
                )
                con.commit()

                cur.execute(
                    query=load_query('sql.insert_image_path_dive_md5.sql'),
                    params={
                        'path': image.relative_to(data_root).as_posix(),
                        'dive': dive,
                        'cksum': cksum
                    }
                )
                con.commit()

    def __process_dirs(self):
        for data_dir in settings.scraper.data_paths:
            data_path = Path(data_dir)
            self.__discover_dives(data_path)

    def run(self):
        """Main entry point
        """
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
