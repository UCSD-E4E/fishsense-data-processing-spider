'''Tornado Endpoints
'''
import datetime as dt
import json
from http import HTTPStatus
from importlib.metadata import version

from tornado.web import RequestHandler

from fishsense_data_processing_spider import __version__
from fishsense_data_processing_spider.metrics import get_counter, get_summary
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
        self.set_header('Access-Control-Allow-Origin', '*')
        self.set_header('Access-Control-Allow-Headers', 'x-requested-with')
        self.set_header('Access-Control-Allow-Methods',
                        'POST, GET, OPTIONS, PUT')

        return super().set_default_headers()

    def options(self, *_, **__):
        """Options handler
        """
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


class RetrieveBatch(BaseHandler):
    """Retrieves a batch of jobs

    """
    SUPPORTED_METHODS = ('GET', 'OPTIONS')

    def initialize(self, key_store: KeyStore):
        """Initializes the handler

        Args:
            key_store (KeyStore): API Key store
        """
        # pylint: disable=attribute-defined-outside-init
        # This is the correct pattern for tornado
        self.__key_store = key_store

    async def get(self, *_, **__) -> None:
        """HTTP Get entry point
        """
        api_key = self.request.headers.get('api_key')
        if not api_key or not self.__key_store.authorize_key(api_key):
            self.set_status(HTTPStatus.UNAUTHORIZED)
            self.finish()
            return
