import logging
import pickle
import shutil
import threading
import uuid
from pathlib import Path
from typing import Dict, Self

from fishsense_data_processing_spider.config import settings


class FileCache:
    instance: Self = None

    def __init__(self, max_storage_mb: int=None):
        if max_storage_mb is None:
            max_storage_mb = settings.cache.max_storage_mb
        self.__log.debug("max_storage_mb: %d", max_storage_mb)

        self.__log = logging.getLogger('FileCache')

        self.__cache_path: Path = settings.cache.path
        self.__cache_path.mkdir(parents=True, exist_ok=True)

        self.__log.debug("Cache path: %s", self.__cache_path)

        self.__cache_map_pickle_path = self.__cache_path / "cache_map.pickle"
        self.__cache_map_lock = threading.Lock()
        if self.__cache_map_pickle_path.exists():
            with self.__cache_map_pickle_path.open("rb") as file:
                self.__cache_map = pickle.load(file)
        else:
            self.__cache_map: Dict[Path, Path] = dict()

        self.__max_storage_mb = max_storage_mb
        self.__occupied_storage = self.__get_occupied_storage()

        self.__garbage_collector_lock = threading.Lock()
        self._garbage_collector_thread = threading.Thread(
            target=self.__do_collect_garbage,
            name='collect_garbage',
            daemon=True
        )

    def __get_occupied_storage(self) -> int:
        return sum(f.lstat().st_size for f in self.__cache_map.values())

    def __do_collect_garbage(self):        
        with self.__garbage_collector_lock:
            if self.__occupied_storage < self.__max_storage_mb:
                return # Exit early
            
            file_keys = list(self.__cache_map.keys())
            # Sort so that earliest time is first.
            file_keys.sort(key=lambda k: self.__cache_map[k].lstat().st_atime)

            while file_keys:
                key_to_delete = file_keys.pop(0)
                self.remove_from_cache(key_to_delete)

                if self.__occupied_storage < self.__max_storage_mb:
                    # We have deleted enough files that we are now fine.
                    break

    def __pickle_cache_map(self):
        with self.__cache_map_pickle_path.open("wb") as file:
            pickle.dump(self.__cache_map, file)

    def _collect_garbage(self):
        # Don't collect garbage if the lock is already held
        if self.__garbage_collector_lock.locked():
            return

        if self.__occupied_storage >= self.__max_storage_mb:
            self._garbage_collector_thread.start()

    def add_to_cache(self, key: Path):
        if self.test_cached_file(key):
            return
        
        target_file_name = str(uuid.uuid1())
        target_path = self.__cache_path / target_file_name

        shutil.copy(key, target_path)
        with self.__cache_map_lock:
            self.__cache_map[key] = target_path

            self.__pickle_cache_map()

        # We only need to worry about collecting garbage when adding.
        self._collect_garbage()

    def get_cached_file(self, key: Path) -> Path:
        if not self.test_cached_file(key):
            self.add_to_cache(key)

        return self.__cache_map[key]

    def test_cached_file(self, key: Path) -> bool:
        return key in self.__cache_map

    def remove_from_cache(self, key: Path):
        with self.__cache_map_lock:
            file_to_remove = self.__cache_map.pop(key)
            self.__occupied_storage -= file_to_remove.lstat().st_size
            file_to_remove.unlink()

            self.__pickle_cache_map()

FileCache.instance = FileCache()
