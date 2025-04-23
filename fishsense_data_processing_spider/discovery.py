'''Data Crawler
'''
import datetime as dt
import itertools
import logging
from pathlib import Path
from threading import Event, Thread
from typing import Dict, List, Optional, Union

import numpy as np
import psycopg
from psycopg.rows import dict_row

from fishsense_data_processing_spider.backend import (get_camera_sns,
                                                      get_file_checksum,
                                                      get_image_date)
from fishsense_data_processing_spider.config import get_log_path
from fishsense_data_processing_spider.metrics import (add_thread_to_monitor,
                                                      get_counter, get_gauge,
                                                      get_summary)
from fishsense_data_processing_spider.sql_utils import (do_many_query,
                                                        do_query, load_query)


class Crawler:
    """Image crawler
    """
    # pylint: disable=too-many-instance-attributes
    def __init__(self,
                 data_paths: List[Union[Path, str]],
                 conn_str: str,
                 *,
                 failed_images_path: Path = get_log_path() / 'failed_images.log',
                 multi_camera_dives_path: Path = get_log_path() / 'multiple_camera_dives.log',
                 dive_insert_path: Path = get_log_path() / 'insert_canonical_dive.sql'
                 ): # pylint: disable=too-many-arguments,
        """Creates the new crawler

        Args:
            data_paths (List[Union[Path, str]]): List of data paths
            conn_str (str): PG Connection String
            interval (dt.timedelta): Scrape interval
            failed_images_path (Path, optional): Path to failed images log. Defaults to 
                `$LOGS/failed_images.log`.
            multi_camera_dives_path (Path, optional): Path to multi camera dives log. Defaults to 
                `$LOGS/multiple_camera_dives.log`.
        """
        self.__log = logging.getLogger('Crawler')
        self.__data_paths = data_paths
        self.__conn = conn_str
        self.__failed_images_path = failed_images_path
        self.__multi_camera_dives = multi_camera_dives_path
        self.__dive_insert_path = dive_insert_path
        self.stop_event = Event()
        self.sleep_interrupt = Event()
        self.__process_thread: Optional[Thread] = None


        get_gauge(
            'new_cdives_to_add',
            'New Canonical Dives to add',
            namespace='e4efs',
            subsystem='spider'
        )

    def __image_discovery_loop(self):
        while not self.stop_event.is_set():
            # Wait until interrupt flag is set, i.e. discovery is triggered
            if not self.sleep_interrupt.wait(1):
                continue
            self.sleep_interrupt.clear()

            try:
                for data_dir in self.__data_paths:
                    data_dir = Path(data_dir)
                    self.__discover_dives(data_dir)
                    if self.stop_event.is_set():
                        return

                self.__conslidate_dives()
                if self.stop_event.is_set():
                    return

                self.__compute_camera_sns()
                if self.stop_event.is_set():
                    return

                self.__image_dates()
                if self.stop_event.is_set():
                    return

                self.__process_canonical_dives()
                if self.stop_event.is_set():
                    return
            except Exception as exc:  # pylint: disable=broad-except
                self.__log.exception('Image discovery failed due to %s', exc)


    def __conslidate_dives(self):
        with psycopg.connect(self.__conn, row_factory=dict_row) as con, con.cursor() as cur:
            # for each dive
            do_query('sql/select_all_dives.sql', cur)
            dives = [row['path'] for row in cur.fetchall()]
            for dive_path in dives:
                do_query(
                    path='sql/select_compute_dive_checksum_by_dive.sql',
                    cur=cur,
                    params={
                        'dive': dive_path
                    }
                )
                cksum = cur.fetchone()['md5']
                do_query(
                    path='sql/update_dive_cksum.sql',
                    cur=cur,
                    params={
                        'cksum': cksum,
                        'path': dive_path
                    }
                )

            # Consolidate canonical dives
            do_query(
                path='sql/select_dive_checksums.sql',
                cur=cur
            )
            checksums = [row['checksum'] for row in cur.fetchall()]
            self.__dive_insert_path.unlink(missing_ok=True)
            new_dive_count = 0
            for checksum in checksums:
                do_query(
                    path='sql/select_candidate_dive_by_checksum.sql',
                    cur=cur,
                    params={
                        'checksum': checksum
                    }
                )
                result = cur.fetchone()
                if result is None:
                    continue
                # do_query(
                #     path='sql/insert_canonical_dive.sql',
                #     cur=cur,
                #     params=result
                # )
                query = load_query('sql/insert_canonical_dive.sql')
                with open(self.__dive_insert_path, 'a', encoding='utf-8') as handle:
                    handle.write(f'{query % result}\n')
                new_dive_count += 1
            new_dive_gauge = get_gauge('new_cdives_to_add')
            new_dive_gauge.set(new_dive_count)


    def __extract_image_dates(self):
        failed_images: Dict[str, Exception] = {}
        while True:
            with psycopg.connect(self.__conn, row_factory=dict_row) as con, con.cursor() as cur:
                do_query(
                    path='sql/select_next_image_for_date.sql',
                    cur=cur,
                    params={
                        'limit': 128
                    }
                )
                results = cur.fetchall()
                get_summary('query_result_length').labels(
                    query='select_next_image_for_date').observe(len(results))
                if len(results) == 0:
                    break
                results = {row['cksum']: Path(row['img_path'])
                           for row in results}
                date_results: Dict[str, Union[dt.datetime, Exception]] = {cksum: get_image_date(
                    path) for cksum, path in results.items() if cksum not in failed_images}
                if len(date_results) == 0:
                    break
                dates = {cksum: date
                         for cksum, date in date_results.items()
                         if isinstance(date, dt.datetime)}
                failed_images.update({cksum: date
                                      for cksum, date in date_results.items()
                                      if not isinstance(date, dt.datetime)})
                get_counter('images_processed').labels(
                    phase='image_dates').inc(len(dates))
                do_many_query(
                    path='sql/update_image_date.sql',
                    cur=cur,
                    param_seq=[{
                        'date': date,
                        'cksum': cksum
                    } for cksum, date in dates.items()]
                )
                con.commit()
        return failed_images

    def __image_dates(self):
        __log = logging.getLogger('image_dates')
        failed_images = self.__extract_image_dates()

        # image dates are now in pg, coalesce per dive
        with psycopg.connect(self.__conn, row_factory=dict_row) as con, con.cursor() as cur:
            # For each dive
            do_query('sql/select_all_dives.sql', cur)
            dives = [row['path'] for row in cur.fetchall()]
            for dive_path in dives:
                do_query(
                    path='sql/query_dates_from_dive.sql',
                    cur=cur,
                    params={
                        'dive': dive_path
                    }
                )
                results = cur.fetchall()
                dates = [dt.datetime.fromisoformat(
                    row['date']) for row in results if row['date'] is not None]
                invalid_dates = (len(dates) != len(results))
                multiple_dates = len(dates) > 1
                if len(dates) == 0:
                    # no images, this is weird...
                    __log.warning('Dive %s has no images???', dive_path)
                    continue
                mean_date = dt.date.fromtimestamp(
                    np.mean([date.timestamp() for date in dates]))
                do_query(
                    path='sql/update_dive_dates.sql',
                    cur=cur,
                    params={
                        'date': mean_date.isoformat(),
                        'invalid_image': invalid_dates,
                        'multiple_date': multiple_dates,
                        'path': dive_path
                    }
                )
        # report failed images
        with open(self.__failed_images_path, 'w', encoding='utf-8') as handle:
            for cksum, exc in failed_images.items():
                handle.write(f'{cksum}: {exc}\n')

    def __discover_dives(self, data_root: Path, *, batch_size: int = 128):
        images = data_root.rglob('*.ORF', case_sensitive=False)
        images_added = get_counter('images_added')
        for image_batch in itertools.batched(images, n=batch_size):
            # Compute checksum
            image_keys = [
                image.relative_to(data_root).as_posix()
                for image in image_batch
            ]
            get_counter('images_processed').labels(
                phase='discover_dives'
            ).inc(len(image_keys))
            with psycopg.connect(self.__conn, row_factory=dict_row) as con, con.cursor() as cur:
                do_many_query(
                    path='sql/select_image_by_path.sql',
                    cur=cur,
                    param_seq=[
                        {
                            'path': image_key
                        }
                        for image_key in image_keys
                    ],
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
                    # do heavy lifting
                    cksum = get_file_checksum(image)
                    dive = image.parent.relative_to(data_root).as_posix()
                    # Dive is probably not known yet, add
                    do_query(
                        path='sql/insert_dive_path.sql',
                        cur=cur,
                        params={
                            'path': dive
                        }
                    )

                    do_query(
                        path='sql/insert_image_path_dive_md5.sql',
                        cur=cur,
                        params={
                            'path': image_keys[idx],
                            'dive': dive,
                            'cksum': cksum
                        }
                    )

                    images_added.inc()

    def __compute_camera_sns(self, batch_size=1024):
        while True:
            with psycopg.connect(self.__conn, row_factory=dict_row) as con, con.cursor() as cur:
                do_query(
                    path='sql/select_images_without_camerasn.sql',
                    cur=cur,
                    params={
                        'limit': batch_size
                    }
                )
                results = cur.fetchall()
                if len(results) == 0:
                    return
                get_summary('query_result_length').labels(
                    query='select_images_without_camerasn').observe(len(results))
                get_counter('images_processed').labels(
                    phase='camera_sns').inc(len(results))
                images = {result['cksum']: Path(
                    result['data_path']) / result['path'] for result in results}
                serial_numbers = get_camera_sns(images)

                do_many_query(
                    path='sql/update_image_camerasn.sql',
                    cur=cur,
                    param_seq=[
                        {
                            'camera_sn': camera_sn,
                            'cksum': cksum
                        }
                        for cksum, camera_sn in serial_numbers.items()
                    ]
                )

                con.commit()

    def __process_canonical_dives(self):
        with psycopg.connect(self.__conn, row_factory=dict_row) as con, con.cursor() as cur:
            do_query(
                path='sql/select_canonical_dives.sql',
                cur=cur
            )
            results = cur.fetchall()
        data_roots = list({row['data_path'] for row in results})
        dives = {Path(data_root): [Path(row['path'])
                                   for row in results
                                   if row['data_path'] == data_root]
                 for data_root in data_roots}
        multiple_camera_dives: List[Path] = []
        for dive in itertools.chain(*dives.values()):
            with psycopg.connect(self.__conn, row_factory=dict_row) as con, con.cursor() as cur:
                do_query(
                    path='sql/select_cameras_per_dive.sql',
                    cur=cur,
                    params={
                        'dive': dive.as_posix()
                    }
                )
                camera_idx = [row['idx'] for row in cur.fetchall()]
                if len(camera_idx) > 1:
                    multiple_camera_dives.append(dive)
                    continue
                if len(camera_idx) == 0:
                    # ????
                    continue
                do_query(
                    path='sql/update_cdive_camera.sql',
                    cur=cur,
                    params={
                        'camera': camera_idx[0],
                        'path': dive.as_posix()
                    }
                )
        with open(self.__multi_camera_dives, 'w', encoding='utf-8') as handle:
            for dive in multiple_camera_dives:
                handle.write(f'{dive.as_posix()}\n')

    def run(self):
        """Starts threads
        """
        self.__process_thread = Thread(
            target=self.__image_discovery_loop,
            name='Image Processing Thread'
        )
        add_thread_to_monitor(self.__process_thread)
        self.__process_thread.start()

    def stop(self):
        """Stops and joins threads
        """
        self.stop_event.set()
        self.__process_thread.join()
