'''Label Studio Sync
'''
import datetime as dt
import logging
import urllib.parse
from pathlib import Path
from threading import Event, Thread

import psycopg
from psycopg.rows import dict_row

from fishsense_data_processing_spider.backend import get_project_export
from fishsense_data_processing_spider.config import (PG_CONN_STR, get_log_path,
                                                     settings)
from fishsense_data_processing_spider.metrics import (add_thread_to_monitor,
                                                      get_gauge)
from fishsense_data_processing_spider.sql_utils import do_many_query, do_query


class LabelStudioSync:
    """Label Studio Sync thread
    """

    def __init__(self,
                 *,
                 bad_task_links_path: Path = get_log_path() / 'bad_task_links.txt'):
        self.__log = logging.getLogger('LabelStudioSync')
        self.stop_event = Event()
        self.__bad_task_links_path = bad_task_links_path
        self.__run_thread = Thread(target=self.__sync_body, name='label_studio_sync')
        add_thread_to_monitor(self.__run_thread)
        self.sleep_interrupt = Event()

    @staticmethod
    def __extract_old_laser_path(url: str) -> Path:
        return Path(urllib.parse.parse_qs(urllib.parse.urlparse(url).query)['d'][0]).relative_to(
            'fs_png_labeling_project_laser/REEF/data').with_suffix('.ORF').as_posix()

    def __headtail_sync(self):
        export = get_project_export(
            project_id=19,
            label_studio_api_key=settings.label_studio.api_key,
            label_studio_host=settings.label_studio.host
        )
        cksums = {task['id']: Path(urllib.parse.urlparse(
            task['data']['img']).path).stem for task in export}
        results = {task['id']: task['annotations'][0]['result']
                for task in export
                if len(task['annotations']) > 0 and len(task['annotations'][0]['result']) > 0}
        flat_results = {
            task_id: {
                f'{result[0]['value']['keypointlabels'][0]}_x': int(result[0]['value']['x'] / 100 *
                                                                    result[0]['original_width']),
                f'{result[0]['value']['keypointlabels'][0]}_y': int(result[0]['value']['y'] / 100 *
                                                                    result[0]['original_height']),
                f'{result[1]['value']['keypointlabels'][0]}_x': int(result[1]['value']['x'] / 100 *
                                                                    result[1]['original_width']),
                f'{result[1]['value']['keypointlabels'][0]}_y': int(result[1]['value']['y'] / 100 *
                                                                    result[1]['original_height']),
            }
            for task_id, result in results.items()
            if len(result) == 2
        }
        params_seq = []
        for task_id, coords in flat_results.items():
            try:
                params_seq.append({
                    'cksum': cksums[task_id],
                    'head_x': coords['Snout_x'],
                    'head_y': coords['Snout_y'],
                    'tail_x': coords['Fork_x'],
                    'tail_y': coords['Fork_y']
                })
            except KeyError:
                required_keys = set(['Snout_x', 'Snout_y', 'Fork_x', 'Fork_y'])
                if len(required_keys.intersection(set(coords.keys()))) > 0:
                    # Some keys are present but not all
                    with open(self.__bad_task_links_path, 'a', encoding='utf-8') as handle:
                        handle.write(f'https://{settings.label_studio.host}/projects/19/data?'
                                     f'task={task_id}\n')


        with psycopg.connect(PG_CONN_STR, row_factory=dict_row) as con, con.cursor() as cur:
            do_many_query(
                path='sql/insert_headtaillabels.sql',
                cur=cur,
                param_seq=[{
                    'cksum': cksum,
                    'task_id': task_id
                } for task_id, cksum in cksums.items()]
            )

            do_many_query(
                path='sql/update_headtail_labels.sql',
                cur=cur,
                param_seq=params_seq
            )
            con.commit()
        get_gauge('last_label_studio_sync').labels(
            project=19).set_to_current_time()

    def __sync_laser_checksum_project(self, projet_id: int):
        export = get_project_export(
            project_id=projet_id,
            label_studio_api_key=settings.label_studio.api_key,
            label_studio_host=settings.label_studio.host
        )
        param_seq = [{
            'cksum': Path(urllib.parse.urlparse(task['data']['img']).path).stem,
            'x': int(task['annotations'][0]['result'][0]['value']['x'] / 100 *
                     task['annotations'][0]['result'][0]['original_width']),
            'y': int(task['annotations'][0]['result'][0]['value']['y'] / 100 *
                     task['annotations'][0]['result'][0]['original_height'])
        }
            for task in export
            if len(task['annotations']) > 0 and len(task['annotations'][0]['result']) > 0
        ]
        with psycopg.connect(PG_CONN_STR, row_factory=dict_row) as con, con.cursor() as cur:
            do_many_query(
                path='sql/update_laser_by_cksum.sql',
                cur=cur,
                param_seq=param_seq
            )
            con.commit()
        get_gauge('last_label_studio_sync').labels(
            project=projet_id).set_to_current_time()

    def __sync_project_10(self):
        export = get_project_export(
            project_id=10,
            label_studio_api_key=settings.label_studio.api_key,
            label_studio_host=settings.label_studio.host
        )
        image_paths = {task['id']: self.__extract_old_laser_path(task['data']['img'])
                       for task in export}
        with psycopg.connect(PG_CONN_STR, row_factory=dict_row) as con, con.cursor() as cur:
            do_query(
                path='sql/select_image_cksum.sql',
                cur=cur
            )
            result = cur.fetchall()

            cksum_lookup = {row['path']: row['image_md5'] for row in result}
            task_id__cksums = {task_id: cksum_lookup[path]
                            for task_id, path in image_paths.items() if path in cksum_lookup}

            do_many_query(
                path='sql/insert_laser_labels.sql',
                cur=cur,
                param_seq=[
                    {
                        'cksum': cksum,
                        'task_id': task_id
                    }
                    for task_id, cksum in task_id__cksums.items()
                ]
            )
            con.commit()
        coords = {task['id']: (int(task['annotations'][0]['result'][0]['value']['x'] / 100 *
                                   task['annotations'][0]['result'][0]['original_width']),
                               int(task['annotations'][0]['result'][0]['value']['y'] / 100 *
                                   task['annotations'][0]['result'][0]['original_height']))
                  for task in export
                  if len(task['annotations']) > 0 and len(task['annotations'][0]['result']) > 0}
        params_seq = [
            {
                'task_id': task_id,
                'x': coord[0],
                'y': coord[1]
            }
            for task_id, coord in coords.items()
        ]
        with psycopg.connect(PG_CONN_STR, row_factory=dict_row) as con, con.cursor() as cur:
            do_many_query(
                path='sql/update_laser_by_taskid.sql',
                cur=cur,
                param_seq=params_seq
            )
            con.commit()
        get_gauge('last_label_studio_sync').labels(
            project=10).set_to_current_time()

    def __sync_body(self):
        while not self.stop_event.is_set():
            last_run = dt.datetime.now()
            next_run = last_run + settings.label_studio.interval

            self.__log.info('Syncing projects')
            self.__bad_task_links_path.unlink(missing_ok=True)
            try:
                self.__sync_project_10()
            except Exception as exc: # pylint: disable=broad-except
                self.__log.exception('Syncing project 10 failed! %s', exc)
            if self.stop_event.is_set():
                break
            for laser_project_id in [39, 40]:
                try:
                    self.__sync_laser_checksum_project(laser_project_id)
                except Exception as exc:  # pylint: disable=broad-except
                    self.__log.exception(
                        'Syncing project %d failed! %s', laser_project_id, exc)
                if self.stop_event.is_set():
                    break
            try:
                self.__headtail_sync()
            except Exception as exc: # pylint: disable=broad-except
                self.__log.exception('Syncing project 19 failed! %s', exc)
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
