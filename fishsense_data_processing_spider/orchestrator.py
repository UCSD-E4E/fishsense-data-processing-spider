'''Job Orchestrator
'''
import datetime as dt
import enum
import uuid
from threading import Event, Thread
from typing import Any, Dict, List, Optional
import logging
import psycopg
from psycopg.rows import dict_row

from fishsense_data_processing_spider.metrics import (add_thread_to_monitor,
                                                      get_counter)
from fishsense_data_processing_spider.sql_utils import do_many_query, do_query


class JobStatus(enum.StrEnum):
    """Job Status

    """
    PENDING = 'pending'
    IN_PROGRESS = 'in_progress'
    CANCELLED = 'cancelled'
    FAILED = 'failed'
    EXPIRED = 'expired'


JOB_STATUS_MAPPING = {
    JobStatus.PENDING: 0,
    JobStatus.IN_PROGRESS: 1,
    JobStatus.CANCELLED: 2,
    JobStatus.FAILED: 3,
    JobStatus.EXPIRED: 4,
}

class Orchestrator:
    """Job orchestrator
    """

    def __init__(self,
                 pg_conn: str,
                 *,
                 reaper_interval: dt.timedelta = dt.timedelta(minutes=5)):
        self.__log = logging.getLogger('Job Orchestrator')
        self.__pgconn = pg_conn
        self.__reaper_thread = Thread(
            target=self.__reaper_loop,
            name='Job Reaper',
            kwargs={
                'interval': reaper_interval
            }
        )
        add_thread_to_monitor(self.__reaper_thread)
        self.stop_event = Event()
        get_counter(
            'reaped_jobs',
            'Number of reaped jobs',
            labelnames=['job'],
            namespace='e4efs',
            subsystem='spider'
        )

    def start(self):
        """Starts the orchestrator threads
        """
        self.__reaper_thread.start()

    def stop(self):
        """Stops the orchestrator threads
        """
        self.stop_event.set()
        self.__reaper_thread.join()

    def __reaper_loop(self, interval: dt.timedelta):
        reaped_job_counter = get_counter('reaped_jobs')
        while not self.stop_event.is_set():
            last_run = dt.datetime.now()
            next_run = last_run + interval
            with psycopg.connect(self.__pgconn, row_factory=dict_row) as con, con.cursor() as cur:
                reaped_ids = []
                try:
                    do_query(
                        path='sql/update_expired_preprocess_job_images.sql',
                        cur=cur
                    )
                    reaped_preprocess_jobs = cur.fetchall()
                    for reaped_job in reaped_preprocess_jobs:
                        self.__log.info('Reaped preprocess job for %s, id %s',
                                        reaped_job['image_md5'],
                                        reaped_job['job_id'])
                        reaped_ids.append(reaped_job['job_id'])
                        reaped_job_counter.inc()
                except psycopg.errors.Error as exc:
                    self.__log.exception(
                        'Reaping preprocess jobs failed! %s', exc)

                try:
                    do_many_query(
                        path='sql/update_expired_jobs.sql',
                        cur=cur,
                        param_seq=[
                            {
                                'job_id': job_id
                            }
                            for job_id in reaped_ids
                        ]
                    )
                except psycopg.errors.Error as exc:
                    self.__log.exception('Updating jobs table failed! %s', exc)
            time_to_sleep = (next_run - dt.datetime.now()).total_seconds()
            self.stop_event.wait(time_to_sleep)

    def get_next_job_dict(self,
                          worker: str,
                          origin: str,
                          n_images: int = 1000,
                          expiration: int = 3600
                          ) -> Dict[str, Any]:
        """Retrieves the next batch of preprocessing jobs

        Args:
            worker (str): Worker name
            origin (str): Originating API Key
            n_images (int, optional): Max number of images to process. Defaults to 1000.
            expiration (int, optional): Number of seconds in the future to expire.  Defaults to 
            3600.
        Returns:
            Dict[str, Any]: Dictionary of job parameters
        """
        job_document = {
            'jobs': []
        }
        with psycopg.connect(self.__pgconn, row_factory=dict_row) as con, con.cursor() as cur:
            do_query(
                path='sql/select_next_images_for_laser_preprocessing.sql',
                cur=cur,
                params={
                    'limit': n_images
                }
            )
            results: List[Dict[str, Any]] = cur.fetchall()
            for row in results:
                job_id = uuid.uuid4()
                job_definitions = {
                    'jobId': job_id.hex,
                    'frameIds': row['checksums'],
                    'cameraId': row['camera_idx']
                }
                job_document['jobs'].append(job_definitions)
                do_query(
                    path='sql/insert_job.sql',
                    cur=cur,
                    params={
                        'job_id': job_id,
                        'worker': worker,
                        'job_type': 'preprocess',
                        'expiration': dt.datetime.now() + dt.timedelta(seconds=expiration),
                        'origin': origin
                    }
                )
                do_many_query(
                    path='sql/update_preprocess_job.sql',
                    cur=cur,
                    param_seq=[
                        {
                            'job_id': job_id,
                            'checksum': cksum
                        }
                        for cksum in row['checksums']
                    ]
                )
        return job_document

    def get_next_laser_preprocessing_dict(self):
        """tbd

        Raises:
            NotImplementedError: Not implemented yet
        """
        raise NotImplementedError

    def set_job_status(self, job_id: uuid.UUID, status: JobStatus, progress: Optional[int] = None):
        """Updates job status

        Args:
            job_id (uuid.UUID): Job UUID
            status (JobStatus): Status string
            progress (Optional[int], optional): Progress from 0 to 100. Defaults to None.
        """
        with psycopg.connect(self.__pgconn, row_factory=dict_row) as con, con.cursor() as cur:
            if progress is None:
                do_query(
                    path='sql/update_job_status.sql',
                    cur=cur,
                    params={
                        'job_id': job_id,
                        'job_status': JOB_STATUS_MAPPING[status]
                    }
                )
            else:
                do_query(
                    path='sql/update_job_progress.sql',
                    cur=cur,
                    params={
                        'job_id': job_id,
                        'job_status': JOB_STATUS_MAPPING[status],
                        'progress': progress
                    }
                )

            if status == JobStatus.CANCELLED:
                do_query(
                    path='sql/select_job_type.sql',
                    cur=cur,
                    params={
                        'job_id': job_id
                    }
                )
                job_type = cur.fetchone()['job_type']
                query_map = {
                    'preprocess': 'sql/cancel_preprocess_job.sql'
                }
                do_query(
                    path=query_map[job_type],
                    cur=cur,
                    params={
                        'job_id': job_id
                    }
                )
