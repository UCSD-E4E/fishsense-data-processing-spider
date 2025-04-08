'''Data Model
'''
import logging
from pathlib import Path
from typing import Dict

import psycopg
from psycopg.rows import dict_row

from fishsense_data_processing_spider.sql_utils import do_query


class DataModel:
    """Postgres Data Model
    """
    def __init__(self,
                 data_path_mapping: Dict[Path, Path],
                 pg_conn_str: str,
                 preprocess_jpeg_path: Path,
                 *,
                 max_raw_data_file_size: int = 20_000_000
                 ):
        self._data_path_mapping = data_path_mapping
        self._pg_conn = pg_conn_str
        self._log = logging.getLogger('DataModel')
        self._log.setLevel(logging.INFO)
        self._max_raw_data_size = max_raw_data_file_size
        self._preprocess_jpg_store = preprocess_jpeg_path

    def get_lens_cal_bytes(self, camera_id: int) -> bytes:
        """Retrieves the lens calibration package

        Args:
            camera_id (int): Camera ID

        Raises:
            KeyError: Camera ID not found

        Returns:
            bytes: Lens calibration package bytes
        """
        with psycopg.connect(self._pg_conn, row_factory=dict_row) as con, con.cursor() as cur:
            do_query(
                path='sql/select_camera_lens_cal_unc_by_id.sql',
                cur=cur,
                params={
                    'camera_id': camera_id
                }
            )
            result = cur.fetchone()
        if result is None:
            raise KeyError(f'{camera_id} is not a recognized camera')
        path = Path(result['path'])
        local_path = self.map_local_path(path)
        if not local_path.is_file():
            raise FileNotFoundError(f'{local_path} not found!')

        with open(local_path, 'rb') as handle:
            return handle.read(self._max_raw_data_size)

    def get_raw_file_bytes(self, checksum: str) -> bytes:
        """Retrieves the raw file bytes

        Args:
            checksum (str): Checksum of raw file

        Raises:
            KeyError: Checksum not found
            FileNotFoundError: Mount not found
            FileNotFoundError: File not found

        Returns:
            bytes: Raw file bytes
        """
        path = self.verify_raw_checksum(checksum)
        local_path = self.map_local_path(path)
        if not local_path.is_file():
            raise FileNotFoundError(f'{local_path} not found!')

        with open(local_path, 'rb') as handle:
            return handle.read(self._max_raw_data_size)

    def verify_raw_checksum(self, checksum: str) -> Path:
        """Verifies the raw checksum

        Args:
            checksum (str): Raw file checksum

        Raises:
            KeyError: Checksum does not exist

        Returns:
            Path: UNC path to raw file
        """
        with psycopg.connect(self._pg_conn, row_factory=dict_row) as con, con.cursor() as cur:
            do_query(
                path='sql/select_unc_path_by_cksum.sql',
                cur=cur,
                params={
                    'cksum': checksum
                }
            )
            result = cur.fetchone()
        if result is None:
            raise KeyError(f'{checksum} is not a recognized checksum')
        path = Path(result['path'])
        return path

    def map_local_path(self, unc_path: Path) -> Path:
        """Map UNC path to local path

        Args:
            unc_path (Path): UNC path

        Raises:
            FileNotFoundError: Volume not mounted
            FileNotFoundError: File not found

        Returns:
            Path: Local path
        """
        matching_paths = {volume: mount
                          for volume, mount in self._data_path_mapping.items()
                          if unc_path.is_relative_to(volume)}
        if len(matching_paths) == 0:
            raise FileNotFoundError(f'{unc_path.parent} not mounted!')
        if len(matching_paths) > 1:
            self._log.warning('Multiple mounts found! %s', matching_paths)
        volume = list(matching_paths.keys())[0]
        mount = matching_paths[volume]
        local_path = mount / unc_path.relative_to(volume)
        self._log.debug('path: %s', unc_path.as_posix())
        self._log.debug('volume: %s', volume.as_posix())
        self._log.debug('mount: %s', mount.as_posix())
        self._log.debug('local_path: %s', local_path.as_posix())
        return local_path

    def put_preprocess_jpeg(self, checksum: str, data: bytes) -> None:
        """Put Preprocessed JPEG

        Args:
            checksum (str): Raw File checksum
            data (bytes): File data
        """
        final_path = self._preprocess_jpg_store / (checksum + '.JPG')
        local_path = self.map_local_path(final_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        with open(local_path, 'wb') as handle:
            handle.write(data)
        with psycopg.connect(self._pg_conn, row_factory=dict_row) as con, con.cursor() as cur:
            do_query(
                path='sql/update_preprocess_jpeg_path.sql',
                cur=cur,
                params={
                    'unc_path': final_path.as_posix(),
                    'cksum': checksum
                }
            )
            con.commit()

    def get_preprocess_jpeg(self, checksum: str) -> bytes:
        """Retrievs the preprocess jpeg data

        Args:
            checksum (str): Raw File checksum

        Raises:
            FileNotFoundError: File not found

        Returns:
            bytes: Binary contents of file
        """
        self.verify_raw_checksum(checksum=checksum)
        final_path = self._preprocess_jpg_store / (checksum + '.JPG')
        local_path = self.map_local_path(final_path)
        if not local_path.is_file():
            raise FileNotFoundError(f'{local_path} not found!')

        with open(local_path, 'rb') as handle:
            return handle.read(self._max_raw_data_size)
