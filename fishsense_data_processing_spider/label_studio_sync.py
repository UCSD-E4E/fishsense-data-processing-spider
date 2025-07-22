'''Label Studio Sync
'''
import datetime as dt
import logging
from pathlib import Path
from threading import Event, Thread

import psycopg
from label_studio_sdk.client import LabelStudio
from psycopg.rows import dict_row

from fishsense_data_processing_spider.config import get_log_path
from fishsense_data_processing_spider.metrics import add_thread_to_monitor
from fishsense_data_processing_spider.sql_utils import do_query


class LabelStudioSync:
    # pylint: disable=too-many-arguments, too-many-instance-attributes
    """Label Studio Sync thread
    """

    def __init__(self,
                 root_url: str,
                 label_studio_host: str,
                 label_studio_key: str,
                 *,
                 pg_conn_str: str,
                 interval: dt.timedelta = dt.timedelta(hours=1),
                 bad_task_links_path: Path = get_log_path() / 'bad_task_links.txt',
                 ):
        self.__log = logging.getLogger('LabelStudioSync')
        self.stop_event = Event()
        self.__bad_task_links_path = bad_task_links_path
        self.__run_thread = Thread(target=self.__sync_body, name='label_studio_sync')
        add_thread_to_monitor(self.__run_thread)
        self.sleep_interrupt = Event()

        self._root_url = root_url
        self._label_studio_host = label_studio_host
        self._label_studio_key = label_studio_key
        self._sync_interval = interval
        self._pg_conn = pg_conn_str

    def _import_headtail_tasks(self, priority: str, project_id: int):
        client = LabelStudio(
            base_url=f'https://{self._label_studio_host}',
            api_key=self._label_studio_key
        )
        with psycopg.connect(self._pg_conn, row_factory=dict_row) as con, con.cursor() as cur:
            do_query(
                path='sql/select_preprocessed_laser_images_for_labeling.sql',
                cur=cur,
                params={
                    'priority': priority
                }
            )
            image_checksums = [row['cksum'] for row in cur.fetchall()]
            urls = {
                cksum: f'{self._root_url}/api/v1/data/laser_jpeg/{cksum}'
                for cksum in image_checksums
            }

            for cksum, url in urls.items():
                new_task = client.tasks.create(
                    data={
                        'img': url
                    },
                    project=project_id
                )
                task_id = new_task.id
                do_query(
                    path='sql/insert_headtaillabels.sql',
                    cur=cur,
                    params={
                        'cksum': cksum,
                        'task_id': task_id
                    }
                )

    def _import_laser_tasks(self, priority: str, project_id: int):
        client = LabelStudio(
            base_url=f'https://{self._label_studio_host}',
            api_key=self._label_studio_key
        )
        with psycopg.connect(self._pg_conn, row_factory=dict_row) as con, con.cursor() as cur:
            do_query(
                path='sql/select_preprocessed_images_for_labeling.sql',
                cur=cur,
                params={
                    'priority': priority
                }
            )
            image_checksums = [row['cksum'] for row in cur.fetchall()]
            urls = {
                cksum: f'{self._root_url}/api/v1/data/preprocess_jpeg/{cksum}'
                for cksum in image_checksums
            }

            for cksum, url in urls.items():
                new_task = client.tasks.create(
                    data={
                        'img': url
                    },
                    project=project_id
                )
                task_id = new_task.id
                do_query(
                    path='sql/insert_laser_labels.sql',
                    cur=cur,
                    params={
                        'cksum': cksum,
                        'task_id': task_id
                    }
                )

    def __sync_body(self):
        while not self.stop_event.is_set():
            last_run = dt.datetime.now()
            next_run = last_run + self._sync_interval

            self.__log.info('Syncing projects')
            self.__bad_task_links_path.unlink(missing_ok=True)
            # try:
            #     self.__sync_project_10()
            # except Exception as exc: # pylint: disable=broad-except
            #     self.__log.exception('Syncing project 10 failed! %s', exc)
            # if self.stop_event.is_set():
            #     break
            # for laser_project_id in [39, 40]:
            #     try:
            #         self.__sync_laser_checksum_project(laser_project_id)
            #     except Exception as exc:  # pylint: disable=broad-except
            #         self.__log.exception(
            #             'Syncing project %d failed! %s', laser_project_id, exc)
            #     if self.stop_event.is_set():
            #         break
            # try:
            #     self.__headtail_sync()
            # except Exception as exc: # pylint: disable=broad-except
            #     self.__log.exception('Syncing project 19 failed! %s', exc)

            try:
                self._import_laser_tasks(priority='HIGH', project_id=42)
            except Exception as exc:
                self.__log.exception(
                    'Importing laser tasks to 42 failed! %s', exc)
            try:
                self._import_laser_tasks(priority='LOW', project_id=43)
            except Exception as exc:
                self.__log.exception(
                    'Importing laser tasks to 43 failed! %s', exc)

            try:
                self._import_headtail_tasks(priority='HIGH', project_id=44)
            except Exception as exc:
                self.__log.exception(
                    'Importing headtail tasks to 44 failed! %s', exc)
            try:
                self._import_headtail_tasks(priority='LOW', project_id=45)
            except Exception as exc:
                self.__log.exception(
                    'Importing headtail tasks to 45 failed! %s', exc)

            self.__log.info('Projects synced')
            self.sleep_interrupt.clear()
            time_to_sleep = (next_run - dt.datetime.now()).total_seconds()
            if time_to_sleep > 0:
                self.sleep_interrupt.wait(time_to_sleep)

    def run(self):
        """Starts the sync threads
        """
        self.__run_thread.start()

    def stop(self):
        """Stops the sync threads
        """
        self.stop_event.set()
        self.__run_thread.join()
