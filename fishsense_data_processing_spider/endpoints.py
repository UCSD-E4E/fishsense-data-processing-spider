'''Tornado Endpoints
'''
import datetime as dt
import json
import logging
import uuid
from abc import ABC
from http import HTTPStatus
from importlib.metadata import version
from typing import Optional

from tornado.web import HTTPError, RequestHandler
from fishsense_data_processing_spider import __version__
from fishsense_data_processing_spider.discovery import Crawler
from fishsense_data_processing_spider.metrics import get_counter, get_summary
from fishsense_data_processing_spider.orchestrator import (JobStatus,
                                                           Orchestrator)
from fishsense_data_processing_spider.web_auth import KeyStore, Permission
from fishsense_data_processing_spider.label_studio_sync import LabelStudioSync
from fishsense_data_processing_spider.data_model import DataModel
# pylint: disable=abstract-method, arguments-differ, attribute-defined-outside-init
# This is typical behavior for tornado

class BaseHandler(RequestHandler):
    """Base Handler for E4EFS
    """

    def _request_summary(self):
        remote_ip = self.request.headers.get("X-Real-IP") or \
            self.request.headers.get("X-Forwarded-For") or \
            self.request.remote_ip
        return "%s %s (%s)" % (
            self.request.method,
            self.request.uri,
            remote_ip,
        )

    def prepare(self):
        if hasattr(self, 'PATH_OVERRIDE'):
            request_path = self.PATH_OVERRIDE
        else:
            request_path = self.request.path
        request_counter = get_counter(
            name='request_call'
        )
        request_counter.labels(endpoint=request_path).inc()
        return super().prepare()

    def on_finish(self):
        if hasattr(self, 'PATH_OVERRIDE'):
            request_path = self.PATH_OVERRIDE
        else:
            request_path = self.request.path
        request_counter = get_counter(
            name='request_result'
        )
        request_counter.labels(
            endpoint=request_path,
            code=self._status_code
        ).inc()

    async def _execute(self, transforms, *args, **kwargs):
        if hasattr(self, 'PATH_OVERRIDE'):
            request_path = self.PATH_OVERRIDE
        else:
            request_path = self.request.path
        with get_summary('request_timing').labels(endpoint=request_path).time():
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


class AuthenticatedHandler(BaseHandler, ABC):
    """Authenticated Handler

    """

    def initialize(self, key_store: KeyStore):
        """Initializes the handler

        Args:
            key_store (KeyStore): API Key Store
        """
        # pylint: disable=attribute-defined-outside-init
        # This is the correct pattern for tornado
        self._key_store = key_store

    def authenticate(self, perms: Optional[Permission] = None) -> bool:
        """Checks whether this request is authorized or not.

        If false, this method will also set up the status and content
        """
        api_key = self.request.headers.get('api_key')
        if not api_key:
            raise HTTPError(HTTPStatus.UNAUTHORIZED, 'Key not provided')
        if not self._key_store.authorize_key(api_key, perms):
            raise HTTPError(HTTPStatus.UNAUTHORIZED, f'Key {api_key} failed')


class AuthenticatedDataHandler(AuthenticatedHandler):
    """Authenticated Data Handler
    """

    def initialize(self, key_store: KeyStore, data_model: DataModel):
        """Initializes the handler

        Args:
            key_store (KeyStore): API Key Store
            data_model (DataModel): Data Model
        """
        # pylint: disable=attribute-defined-outside-init
        # This is the correct pattern for tornado
        super().initialize(key_store=key_store)
        self._data_model = data_model

class AuthenticatedJobHandler(AuthenticatedHandler, ABC):
    """Authenticated Job Handler
    """

    def initialize(self, key_store: KeyStore, orchestrator: Orchestrator):
        """Initializes the handler

        Args:
            key_store (KeyStore): API Key Store
            orchestrator (Orchestrator): Job orchestrator
        """
        # pylint: disable=attribute-defined-outside-init
        # This is the correct pattern for tornado
        super().initialize(key_store=key_store)
        self._orchestrator = orchestrator


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


class DoDiscoveryHandler(AuthenticatedHandler):
    """Do Discovery handler
    """
    SUPPORTED_METHODS = ('POST', 'OPTIONS')

    def initialize(self, key_store: KeyStore, crawler: Crawler):
        """Initializes this handler

        Args:
            key_store (KeyStore): Web Key Store
            crawler (Crawler): Crawler

        """
        # pylint: disable=attribute-defined-outside-init
        # This is the correct pattern for tornado
        self._crawler = crawler
        super().initialize(key_store)

    async def post(self, *_, **__) -> None:
        """POST method
        """
        self.authenticate(Permission.DO_DISCOVERY)
        self._crawler.sleep_interrupt.set()


class DoLabelStudioSyncHandler(AuthenticatedHandler):
    """Label Studio Sync execution endpoing
    """
    SUPPORTED_METHODS = ('POST', 'OPTIONS')

    def initialize(self, key_store: KeyStore, label_studio: LabelStudioSync):
        """Initializes this handler

        Args:
            key_store (KeyStore): Web Key Store
            label_studio (LabelStudioSync): Label Studio Syncer

        """
        # pylint: disable=attribute-defined-outside-init
        # This is the correct pattern for tornado
        self._label_studio = label_studio
        super().initialize(key_store)

    async def post(self, *_, **__) -> None:
        """POST method
        """
        self.authenticate(Permission.DO_LABEL_STUDIO_SYNC)
        self._label_studio.sleep_interrupt.set()


class RawDataHandler(AuthenticatedDataHandler):
    """Raw Data Handler
    """
    SUPPORTED_METHODS = ('GET', 'OPTIONS')
    PATH_OVERRIDE = '/api/v1/data/raw'

    def initialize(self, key_store, data_model: DataModel):
        self._logger = logging.getLogger('RawDataHandler')
        return super().initialize(key_store, data_model)

    async def get(self, checksum: str) -> None:
        """Get method implementation

        Args:
            checksum (str): Raw File checksum
        """
        self.authenticate(Permission.GET_RAW_FILE)
        try:
            blob = self._data_model.get_raw_file_bytes(checksum)
        except KeyError as exc:
            raise HTTPError(HTTPStatus.NOT_FOUND, 'Invalid checksum')
        self._logger.debug('Retrieved %d bytes', len(blob))
        self.set_header('Content-Type', 'application/octet-stream')
        self.write(blob)
        self.flush()
        self.finish()


class LensCalHandler(AuthenticatedHandler):
    """Lens Calibration Data Handler
    """
    SUPPORTED_METHODS = ('GET', 'OPTIONS')
    PATH_OVERRIDE = '/api/v1/data/lens_cal'

    def initialize(self, key_store, data_model: DataModel):
        self._data_model = data_model
        self._logger = logging.getLogger('LensCalHandler')
        return super().initialize(key_store)

    async def get(self, camera_id: str) -> None:
        """Get method implementation

        Args:
            checksum (str): Raw File checksum
        """
        self.authenticate(Permission.GET_RAW_FILE)
        blob = self._data_model.get_lens_cal_bytes(camera_id)
        self._logger.debug('Retrieved %d bytes', len(blob))
        self.set_header('Content-Type', 'application/octet-stream')
        self.write(blob)
        self.flush()
        self.finish()


class PreprocessJpegHandler(AuthenticatedHandler):
    """Preprocess Jpeg handler
    """
    SUPPORTED_METHODS = ('PUT', 'OPTIONS', 'GET')
    PATH_OVERRIDE = '/api/v1/data/preprocess_jpeg'

    def initialize(self, key_store, data_model: DataModel):
        self._data_model = data_model
        self._logger = logging.getLogger('PreprocessJpegHandler')
        return super().initialize(key_store)

    async def put(self, checksum: str) -> None:
        """Put Method Handler

        Args:
            checksum (str): Raw File Checksum

        Raises:
            HTTPError: Not Found
        """
        self.authenticate(Permission.PUT_PREPROCESS_JPEG)
        try:
            self._data_model.verify_raw_checksum(checksum)
        except KeyError:
            raise HTTPError(HTTPStatus.NOT_FOUND)

        self._data_model.put_preprocess_jpeg(
            checksum=checksum,
            data=self.request.body
        )
        self.set_status(HTTPStatus.OK)
        self.finish()

    async def get(self, checksum: str) -> None:
        """Get method implementation

        Args:
            checksum (str): Raw File checksum
        """
        blob = self._data_model.get_preprocess_jpeg(checksum)
        self._logger.debug('Retrieved %d bytes', len(blob))
        self.set_header('Content-Type', 'image/jpeg')
        self.set_header(
            'Cache-Control',
            'max-age=3600, must-revalidate, no-transform, immutable'
        )
        self.write(blob)
        self.flush()
        self.finish()


class LaserLabelHandler(AuthenticatedHandler):
    """Laser Label Handler
    """
    SUPPORTED_METHODS = ('GET', 'OPTIONS')
    PATH_OVERRIDE = '/api/v1/data/laser_label'

    def initialize(self, key_store, data_model: DataModel):
        self._data_model = data_model
        self._logger = logging.getLogger('LaserLabelHandler')
        return super().initialize(key_store)

    async def get(self, checksum: str) -> None:
        """Get method implementation

        Args:
            checksum (str): Raw File Checksum
        """
        self.authenticate(Permission.GET_RAW_FILE)
        try:
            document = self._data_model.get_laser_label(checksum)
        except KeyError:
            self._logger.warning('Invalid raw file checksum %s', checksum)
            raise HTTPError(HTTPStatus.NOT_FOUND)
        if not document:
            self._logger.info('No laser label for %s', checksum)
            raise HTTPError(HTTPStatus.NOT_FOUND)
        self._logger.debug('Got %s', document)
        self.finish(document)


class HeadTailLabelHandler(AuthenticatedHandler):
    """Head Tail label handler
    """
    SUPPORTED_METHODS = ('DELETE', 'OPTIONS')
    PATH_OVERRIDE = '/api/v1/data/head_tail'

    def initialize(self, key_store, data_model: DataModel):
        self._data_model = data_model
        self._logger = logging.getLogger('HeadTailLabelHandler')
        return super().initialize(key_store)

    async def delete(self, checksum: str) -> None:
        """Delete method

        Args:
            checksum (str): Raw file checksum
        """
        self.authenticate(Permission.ADMIN)
        self._data_model.delete_headtail_label(checksum)
        self.set_status(HTTPStatus.OK)
        self.finish()
        self._logger.debug('Deleted %s', checksum)

class PreprocessLaserJpegHandler(AuthenticatedHandler):
    """Preprocess Laser Jpeg handler
    """
    SUPPORTED_METHODS = ('PUT', 'OPTIONS', 'GET', 'DELETE')
    PATH_OVERRIDE = '/api/v1/data/laser_jpeg'

    def initialize(self, key_store, data_model: DataModel):
        self._data_model = data_model
        self._logger = logging.getLogger('PreprocessLaserJpegHandler')
        return super().initialize(key_store)

    async def put(self, checksum: str) -> None:
        """Put Method Handler

        Args:
            checksum (str): Raw File Checksum

        Raises:
            HTTPError: Not Found
        """
        self.authenticate(Permission.PUT_LASER_FRAME)
        try:
            self._data_model.verify_raw_checksum(checksum)
        except KeyError:
            raise HTTPError(HTTPStatus.NOT_FOUND)

        self._data_model.put_preprocess_laser_jpeg(
            checksum=checksum,
            data=self.request.body
        )
        self.set_status(HTTPStatus.OK)
        self.finish()

    async def get(self, checksum: str) -> None:
        """Get method implementation

        Args:
            checksum (str): Raw File checksum
        """
        blob = self._data_model.get_preprocess_laser_jpeg(checksum)
        self._logger.debug('Retrieved %d bytes', len(blob))
        self.set_header('Content-Type', 'image/jpeg')
        self.set_header(
            'Cache-Control',
            'max-age=3600, must-revalidate, no-transform, immutable'
        )
        self.write(blob)
        self.flush()
        self.finish()

    async def delete(self, checksum: str) -> None:
        self.authenticate(Permission.ADMIN)
        try:
            self._data_model.verify_raw_checksum(checksum)
        except KeyError:
            raise HTTPError(HTTPStatus.NOT_FOUND)

        self._data_model.delete_preprocess_laser_jpeg(checksum)
        self.set_status(HTTPStatus.OK)
        self.finish()
        self._logger.debug('Deleted %s', checksum)

class DebugDataHandler(AuthenticatedJobHandler):
    """Debug Data Handler
    """
    SUPPORTED_METHODS = ('PUT', 'OPTIONS')
    PATH_OVERRIDE = '/api/v1/debug'

    def initialize(self, key_store, orchestrator: Orchestrator, data_model: DataModel):
        self._data_model = data_model
        self._logger = logging.getLogger('DebugDataHandler')
        return super().initialize(key_store, orchestrator)

    async def put(self, job_id: str) -> None:
        self.authenticate(Permission.PUT_DEBUG_BLOB)
        try:
            job_id = uuid.UUID(job_id)
        except ValueError:
            raise HTTPError(HTTPStatus.BAD_REQUEST, 'Invalid job id')
        if not self._orchestrator.is_valid_job(job_id):
            raise HTTPError(HTTPStatus.NOT_FOUND, 'Job not found')
        self._data_model.put_debug_data(
            job_id=job_id,
            data=self.request.body
        )
        self.set_status(HTTPStatus.OK)
        self.finish()
        self._logger.debug('Stored %d bytes', len(self.request.body))


class ApiKeyAdminHandler(AuthenticatedHandler):
    """API Key Admin Handler
    """
    SUPPORTED_METHODS = ('PUT', 'DELETE', 'GET', 'OPTIONS')

    def initialize(self, key_store: KeyStore):
        self._key_store = key_store
        return super().initialize(key_store)

    async def put(self, *_, **__) -> None:
        self._modify_perms(True)

    def _modify_perms(self, value: bool) -> None:
        self.authenticate(Permission.ADMIN)
        key = self.get_query_argument('key')
        body = json.loads(self.request.body.decode('utf-8'))
        if 'scopes' not in body:
            raise HTTPError(HTTPStatus.BAD_REQUEST, 'Scopes not provided')
        if not isinstance(body['scopes'], list):
            raise HTTPError(HTTPStatus.BAD_REQUEST, 'Scopes not a list')
        if not all(isinstance(x, str) for x in body['scopes']):
            raise HTTPError(HTTPStatus.BAD_REQUEST,
                            'Scopes not a list of strings')
        if not all(x in Permission for x in body['scopes']):
            raise HTTPError(HTTPStatus.BAD_REQUEST,
                            'Scopes not valid permissions')
        for scope in body['scopes']:
            self._key_store.set_perm(key, Permission(scope), value)
        self.set_status(HTTPStatus.OK)
        self.finish()

    async def delete(self, *_, **__) -> None:
        self._modify_perms(False)

    async def get(self, *_, **__) -> None:
        self.authenticate()
        key = self.get_query_argument(
            'key', default=self.request.headers.get('api_key'))
        perms = self._key_store.get_perm(key)
        self.set_status(HTTPStatus.OK)
        self.set_header('Content-Type', 'application/json')
        self.write({
            'scopes': [
                perm.value for perm in perms
            ]
        })
        self.finish()


class NewKeyHandler(AuthenticatedHandler):
    """New Key Handler
    """
    SUPPORTED_METHODS = ('POST', 'OPTIONS')

    def initialize(self, key_store: KeyStore):
        self._key_store = key_store
        return super().initialize(key_store)

    async def post(self, *_, **__) -> None:
        """post method handler
        """
        self.authenticate(Permission.ADMIN)
        comment = self.get_query_argument('comment')
        expiration = dt.datetime.fromisoformat(
            self.get_query_argument('expiration', default=None))
        scopes = self.get_query_arguments('scopes')
        key, expires = self._key_store.get_new_key(
            comment=comment,
            expires=expiration
        )
        for scope in scopes:
            self._key_store.set_perm(key, Permission(scope), True)
        self.set_status(HTTPStatus.OK)
        self.set_header('Content-Type', 'application/json')
        self.write({
            'key': key,
            'expires': expires.isoformat(),
        })
        self.finish()


class FrameMetadataHandler(AuthenticatedDataHandler):
    """Frame metadata handler
    """
    SUPPORTED_METHODS = ('GET', 'OPTIONS')
    PATH_OVERRIDE = '/api/v1/metadata/frame'

    def initialize(self, key_store, data_model):
        self._logger = logging.getLogger('FrameMetadataHandler')
        return super().initialize(key_store, data_model)

    async def get(self, checksum: str) -> None:
        """Get method implementation

        Args:
            checksum (str): Raw File Checksum
        """
        self.authenticate(Permission.GET_METADATA)
        try:
            document = self._data_model.get_frame_metadata(checksum)
        except KeyError as exc:
            self._logger.warning('Invalid raw file checksum %s', checksum)
            raise HTTPError(HTTPStatus.NOT_FOUND) from exc
        if not document:
            self._logger.info('No frame metadata for %s', checksum)
            raise HTTPError(HTTPStatus.NOT_FOUND)

        self.finish(json.dumps(document, default=str))


class DiveMetadataHandler(AuthenticatedDataHandler):
    """Dive metadata handler
    """
    SUPPORTED_METHODS = ('GET', 'OPTIONS')
    PATH_OVERRIDE = '/api/v1/metadata/dive'

    def initialize(self, key_store, data_model):
        self._logger = logging.getLogger('DiveMetadataHandler')
        return super().initialize(key_store, data_model)

    async def get(self, checksum: str) -> None:
        """Get method implementation

        Args:
            checksum (str): Dive checksum
        """
        self.authenticate(Permission.GET_METADATA)
        document = self._data_model.get_dive_metadata(checksum)
        self.finish(document)


class DiveListHandler(AuthenticatedDataHandler):
    """Dive List Handler
    """
    SUPPORTED_METHODS = ('GET', 'OPTIONS')

    def initialize(self, key_store, data_model):
        self._logger = logging.getLogger('DiveListHandler')
        return super().initialize(key_store, data_model)

    async def get(self) -> None:
        """Dive list handler GET
        """
        self.authenticate(Permission.GET_METADATA)
        dive_list = self._data_model.list_dives()
        self.finish({
            'dives': dive_list
        })
