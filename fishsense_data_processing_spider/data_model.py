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
                 *,
                 max_raw_data_file_size: int = 20_000_000
                 ):
        self._data_path_mapping = data_path_mapping
        self._pg_conn = pg_conn_str
        self._log = logging.getLogger('DataModel')
        self._max_raw_data_size = max_raw_data_file_size

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
        matching_paths = {volume: mount
                          for volume, mount in self._data_path_mapping.items()
                          if path.is_relative_to(volume)}
        if len(matching_paths) == 0:
            raise FileNotFoundError(f'{path.parent} not mounted!')
        if len(matching_paths) > 1:
            self._log.warning('Multiple mounts found! %s', matching_paths)
        volume = list(matching_paths.keys())[0]
        mount = matching_paths[volume]
        local_path = mount / path.relative_to(volume)
        self._log.debug('path: %s', path.as_posix())
        self._log.debug('volume: %s', volume.as_posix())
        self._log.debug('mount: %s', mount.as_posix())
        self._log.debug('local_path: %s', local_path.as_posix())
        if not local_path.is_file():
            raise FileNotFoundError(f'{local_path} not found!')
        with open(local_path, 'rb') as handle:
            return handle.read(self._max_raw_data_size)
