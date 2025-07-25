"""Data Model"""

import logging
import uuid
from pathlib import Path
from typing import Any, Collection, Dict, List, Optional

import psycopg
from psycopg.rows import dict_row

from fishsense_data_processing_spider.file_cache import FileCache
from fishsense_data_processing_spider.sql_utils import do_query


class DataModel:
    """Postgres Data Model"""

    def __init__(
        self,
        data_path_mapping: Dict[Path, Path],
        pg_conn_str: str,
        preprocess_jpeg_path: Path,
        preprocess_laser_jpeg_path: Path,
        debug_data_path: Path,
        *,
        max_raw_data_file_size: int = 20_000_000,
        bad_query_handler: Collection[logging.Handler] = [],
    ):
        self._data_path_mapping = data_path_mapping
        self._pg_conn = pg_conn_str
        self._log = logging.getLogger("DataModel")
        self.__bad_query_handler = logging.getLogger("DataModelBadQuery")
        for handler in bad_query_handler:
            self.__bad_query_handler.addHandler(handler)
        self._log.setLevel(logging.INFO)
        self._max_raw_data_size = max_raw_data_file_size
        self._preprocess_jpg_store = preprocess_jpeg_path
        self._preprocess_laser_jpg_store = preprocess_laser_jpeg_path
        self._debug_data_path = debug_data_path

    def get_lens_cal_bytes(self, camera_id: int) -> bytes:
        """Retrieves the lens calibration package

        Args:
            camera_id (int): Camera ID

        Raises:
            KeyError: Camera ID not found

        Returns:
            bytes: Lens calibration package bytes
        """
        with psycopg.connect(
            self._pg_conn, row_factory=dict_row
        ) as con, con.cursor() as cur:
            do_query(
                path="sql/select_camera_lens_cal_unc_by_id.sql",
                cur=cur,
                params={"camera_id": camera_id},
            )
            result = cur.fetchone()
        if result is None:
            raise KeyError(f"{camera_id} is not a recognized camera")
        path = Path(result["path"])
        local_path = self.map_cache_path(path)
        if not local_path.is_file():
            raise FileNotFoundError(f"{local_path} not found!")

        with open(local_path, "rb") as handle:
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
        local_path = self.map_cache_path(path)
        if not local_path.is_file():
            raise FileNotFoundError(f"{local_path} not found!")

        with open(local_path, "rb") as handle:
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
        with psycopg.connect(
            self._pg_conn, row_factory=dict_row
        ) as con, con.cursor() as cur:
            do_query(
                path="sql/select_unc_path_by_cksum.sql",
                cur=cur,
                params={"cksum": checksum},
            )
            result = cur.fetchone()
        if result is None:
            self.__bad_query_handler.error("%s is not a recognized checksum", checksum)
            raise KeyError(f"{checksum} is not a recognized checksum")
        path = Path(result["path"])
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
        matching_paths = {
            volume: mount
            for volume, mount in self._data_path_mapping.items()
            if unc_path.is_relative_to(volume)
        }
        if len(matching_paths) == 0:
            raise FileNotFoundError(f"{unc_path.parent} not mounted!")
        if len(matching_paths) > 1:
            self._log.warning("Multiple mounts found! %s", matching_paths)
        volume = list(matching_paths.keys())[0]
        mount = matching_paths[volume]
        local_path = mount / unc_path.relative_to(volume)
        self._log.debug("path: %s", unc_path.as_posix())
        self._log.debug("volume: %s", volume.as_posix())
        self._log.debug("mount: %s", mount.as_posix())
        self._log.debug("local_path: %s", local_path.as_posix())
        return local_path

    def map_cache_path(self, unc_path: Path) -> Path:
        """Map UNC path to cache path

        Args:
            unc_path (Path): UNC path

        Raises:
            FileNotFoundError: Volume not mounted
            FileNotFoundError: File not found

        Returns:
            Path: Cache path
        """
        file_cache = FileCache.instance

        local_path = self.map_local_path(unc_path)
        cache_path = file_cache.get_cached_file(local_path)

        self._log.debug("cache_path: %s", cache_path.as_posix())

        return cache_path

    def put_preprocess_jpeg(self, checksum: str, data: bytes) -> None:
        """Put Preprocessed JPEG

        Args:
            checksum (str): Raw File checksum
            data (bytes): File data
        """
        final_path = self._preprocess_jpg_store / (checksum + ".JPG")
        local_path = self.map_local_path(final_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        with open(local_path, "wb") as handle:
            handle.write(data)
        with psycopg.connect(
            self._pg_conn, row_factory=dict_row
        ) as con, con.cursor() as cur:
            do_query(
                path="sql/update_preprocess_jpeg_path.sql",
                cur=cur,
                params={"unc_path": final_path.as_posix(), "cksum": checksum},
            )
            con.commit()

    def put_preprocess_laser_jpeg(self, checksum: str, data: bytes) -> None:
        """Put Preprocessed Laser JPEG

        Args:
            checksum (str): Raw File checksum
            data (bytes): File data
        """
        final_path = self._preprocess_laser_jpg_store / (checksum + ".JPG")
        local_path = self.map_local_path(final_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        with open(local_path, "wb") as handle:
            handle.write(data)
        with psycopg.connect(
            self._pg_conn, row_factory=dict_row
        ) as con, con.cursor() as cur:
            do_query(
                path="sql/update_preprocess_laser_jpeg_path.sql",
                cur=cur,
                params={"unc_path": final_path.as_posix(), "cksum": checksum},
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
        final_path = self._preprocess_jpg_store / (checksum + ".JPG")
        local_path = self.map_cache_path(final_path)
        if not local_path.is_file():
            raise FileNotFoundError(f"{local_path} not found!")

        with open(local_path, "rb") as handle:
            return handle.read(self._max_raw_data_size)

    def get_preprocess_laser_jpeg(self, checksum: str) -> bytes:
        """Retrievs the preprocess laser jpeg data

        Args:
            checksum (str): Raw File checksum

        Raises:
            FileNotFoundError: File not found

        Returns:
            bytes: Binary contents of file
        """
        self.verify_raw_checksum(checksum=checksum)
        final_path = self._preprocess_laser_jpg_store / (checksum + ".JPG")
        local_path = self.map_cache_path(final_path)
        if not local_path.is_file():
            raise FileNotFoundError(f"{local_path} not found!")

        with open(local_path, "rb") as handle:
            return handle.read(self._max_raw_data_size)

    def delete_preprocess_laser_jpeg(self, checksum: str) -> None:
        self.verify_raw_checksum(checksum=checksum)
        final_path = self._preprocess_laser_jpg_store / (checksum + ".JPG")
        local_path = self.map_local_path(final_path)
        if local_path.is_file():
            local_path.unlink()
        with psycopg.connect(
            self._pg_conn, row_factory=dict_row
        ) as con, con.cursor() as cur:
            do_query(
                path="sql/delete_preprocess_laser_jpeg_path.sql",
                cur=cur,
                params={"cksum": checksum},
            )
            con.commit()

    def get_laser_label(self, checksum: str) -> Optional[Dict[str, int]]:
        """Retrieves the laser label

        Args:
            checksum (str): Raw file checksum

        Returns:
            Optional[Dict[str, int]]: Laser Label dict if exists, otherwise None
        Raises:
            KeyError: Raw file does not exist
        """
        self.verify_raw_checksum(checksum=checksum)
        with psycopg.connect(
            self._pg_conn, row_factory=dict_row
        ) as con, con.cursor() as cur:
            do_query(
                path="sql/select_laser_label_by_cksum.sql",
                cur=cur,
                params={"cksum": checksum},
            )
            result = cur.fetchone()
        if result is None:
            return None
        return {"task_id": result["task_id"], "x": result["x"], "y": result["y"]}

    def put_debug_data(self, job_id: uuid.UUID, data: bytes) -> None:
        """Put Debug Data

        This method stores the debug data for a given job ID. The data is saved in a zip file format
        with the job ID as the filename in the specified debug data path.

        Args:
            job_id (uuid.UUID): Job ID
            data (bytes): File data
        """
        self._log.debug("put_debug_data %s", job_id)
        final_path = self._debug_data_path / (str(job_id) + ".zip")
        local_path = self.map_local_path(final_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        with open(local_path, "wb") as handle:
            handle.write(data)

    def delete_headtail_label(self, checksum: str) -> None:
        self.verify_raw_checksum(checksum=checksum)
        with psycopg.connect(
            self._pg_conn, row_factory=dict_row
        ) as con, con.cursor() as cur:
            do_query(
                path="sql/delete_headtail_label_by_cksum.sql",
                cur=cur,
                params={"cksum": checksum},
            )
            con.commit()

    def get_frame_metadata(self, checksum: str) -> Dict[str, Any]:
        self.verify_raw_checksum(checksum=checksum)
        with psycopg.connect(
            self._pg_conn, row_factory=dict_row
        ) as con, con.cursor() as cur:
            do_query(
                path="sql/select_frame_metadata_by_cksum.sql",
                cur=cur,
                params={"cksum": checksum},
            )
            result = cur.fetchone()
        return dict(result)

    def get_dive_metadata(self, checksum: str) -> Dict[str, Any]:
        """Gets the dive metadata

        Args:
            checksum (str): Dive checksum

        Returns:
            Dict[str, Any]: Dictionary of dive data
        """
        with psycopg.connect(
            self._pg_conn, row_factory=dict_row
        ) as con, con.cursor() as cur:
            do_query(
                path="sql/select_dive_frame_checksum_by_dive_checksum.sql",
                cur=cur,
                params={"cksum": checksum},
            )
            frame_ids = [row["frames"] for row in cur.fetchall()]
        return {"frames": frame_ids}

    def list_dives(self) -> List[str]:
        """List dives

        Returns:
            List[str]: List of dive checksums
        """
        with psycopg.connect(
            self._pg_conn, row_factory=dict_row
        ) as con, con.cursor() as cur:
            do_query(path="sql/select_dive_checksums.sql", cur=cur)
            return [row["checksum"] for row in cur.fetchall()]
