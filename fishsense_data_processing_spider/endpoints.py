'''Tornado Endpoints
'''
import datetime as dt
import json
import logging
import uuid
from abc import ABC
from http import HTTPStatus
from importlib.metadata import version

from tornado.web import HTTPError, RequestHandler

from fishsense_data_processing_spider import __version__
from fishsense_data_processing_spider.metrics import get_counter, get_summary
from fishsense_data_processing_spider.orchestrator import (JobStatus,
                                                           Orchestrator)
from fishsense_data_processing_spider.web_auth import KeyStore

# pylint: disable=abstract-method
# This is typical behavior for tornado

class BaseHandler(RequestHandler):
    """Base Handler for E4EFS
    """

    def prepare(self):
        request_counter = get_counter(
            name='request_call'
        )
        request_counter.labels(endpoint=self.request.path).inc()
        return super().prepare()

    def on_finish(self):
        request_counter = get_counter(
            name='request_result'
        )
        request_counter.labels(
            endpoint=self.request.path,
            code=self._status_code
        ).inc()

    async def _execute(self, transforms, *args, **kwargs):
        with get_summary('request_timing').labels(endpoint=self.request.path).time():
            await super()._execute(transforms, *args, **kwargs)

    def set_default_headers(self):
        super().set_default_headers()
        self.set_header('Access-Control-Allow-Origin', '*')
        self.set_header('Access-Control-Allow-Headers',
                        'Content-Type, api_key, Authorization')
        self.set_header('Access-Control-Allow-Methods',
                        ', '.join(self.SUPPORTED_METHODS))

    def options(self, *_, **__):
        """Options handler
        """
        logging.debug(self._headers)
        self.set_status(204)
        self.finish()


class HomePageHandler(BaseHandler):
    """Home Page Handler
    """
    SUPPORTED_METHODS = ('GET',)

    def initialize(self, start_time: dt.datetime):
        """Initialization

        Args:
            start_time (dt.datetime): Program start time
        """
        # pylint: disable=attribute-defined-outside-init
        # This is the correct pattern for tornado
        self.__start_time = start_time

    async def get(self, *_, **__) -> None:
        """Handler body
        """
        self.write(
            f'Fishsense Data Processing Orchestrator v{__version__} deployed at '
            f'{self.__start_time.isoformat()}')
        self.set_status(HTTPStatus.OK)


class VersionHandler(BaseHandler):
    """Version Handler

    """
    SUPPORTED_METHODS = ('GET', 'OPTIONS')

    async def get(self, *_, **__) -> None:
        """Gets the version information for this app
        """
        self.write(json.dumps({
            'version': version('fishsense_data_processing_worker')
        }))


class AuthenticatedJobHandler(BaseHandler, ABC):
    """Authenticated Job Handler
    """

    def initialize(self, key_store: KeyStore, orchestrator: Orchestrator):
        """Initializes the handler

        Args:
            key_store (KeyStore): API Key Store
            orchestrator (Orchestrator): Job Orchestration
        """
        # pylint: disable=attribute-defined-outside-init
        # This is the correct pattern for tornado
        self._key_store = key_store
        self._orchestrator = orchestrator

    def authenticate(self) -> bool:
        """Checks whether this request is authorized or not.

        If false, this method will also set up the status and content
        """
        api_key = self.request.headers.get('api_key')
        if not api_key:
            raise HTTPError(HTTPStatus.UNAUTHORIZED, 'Key not provided')
        if not self._key_store.authorize_key(api_key):
            raise HTTPError(HTTPStatus.UNAUTHORIZED, f'Key {api_key} failed')


class RetrieveBatch(AuthenticatedJobHandler):
    """Retrieves a batch of jobs

    """
    SUPPORTED_METHODS = ('POST', 'OPTIONS')

    async def post(self, *_, **__) -> None:
        """HTTP POST entry point
        """
        self.authenticate()

        worker = self.get_query_argument('worker')
        n_images = int(self.get_query_argument('nImages', '1000'))
        job_document = self._orchestrator.get_next_job_dict(
            worker=worker,
            n_images=n_images,
            origin=self.request.headers.get('api_key'),
            expiration=int(self.get_query_argument('expiration', '3600'))
        )
        self.set_status(HTTPStatus.OK)
        self.write(job_document)


class JobStatusHandler(AuthenticatedJobHandler):
    """Job Statuhandler
    """
    SUPPORTED_METHODS = ('PUT', 'OPTIONS')

    async def put(self, *_, **__) -> None:
        """Put method handler
        """
        self.authenticate()

        job_id = uuid.UUID(self.get_query_argument('jobId'))
        status = self.get_query_argument('status')
        if status not in JobStatus:
            raise HTTPError(HTTPStatus.BAD_REQUEST, 'bad status value')
        progress = self.get_query_argument('progress', default=None)
        if progress:
            progress = int(progress)
            if not 0 <= progress <= 100:
                raise HTTPError(HTTPStatus.BAD_REQUEST, 'bad progress value')
        self._orchestrator.set_job_status(
            job_id=job_id,
            status=JobStatus(status),
            progress=progress
        )
        self.set_status(HTTPStatus.OK)
        self.finish()


class NotImplementedHandler(BaseHandler):
    """Endpoint Not Implemented Handler
    """
    SUPPORTED_METHODS = ('GET', 'PUT', 'POST', 'OPTIONS')

    async def get(self, *_, **__) -> None:
        """Get Method Handler

        Raises:
            HTTPError: Not Implemented
        """
        raise HTTPError(HTTPStatus.NOT_IMPLEMENTED,
                        f'{self.request.path} GET not implemented!')

    async def put(self, *_, **__) -> None:
        """Put Method Handler

        Raises:
            HTTPError: Not Implemented
        """
        raise HTTPError(HTTPStatus.NOT_IMPLEMENTED,
                        f'{self.request.path} PUT not implemented!')

    async def post(self, *_, **__) -> None:
        """Post Method Handler

        Raises:
            HTTPError: Not Implemented
        """
        raise HTTPError(HTTPStatus.NOT_IMPLEMENTED,
                        f'{self.request.path} POST not implemented!')
