from hashlib import md5
from pathlib import Path

from fishsense_data_processing_spider.file_cache import FileCache


def test_singleton():
    file_cache = FileCache.instance

    assert isinstance(file_cache, FileCache)

def test_add_remove_to_cache():
    file_cache = FileCache.instance

    file_path = Path(__file__)

    assert not file_cache.test_cached_file(file_path)
    file_cache.add_to_cache(file_path)
    assert file_cache.test_cached_file(file_path)

    file_cache.remove_from_cache(file_path)
    assert not file_cache.test_cached_file(file_path)

def test_get_cached_file():
    file_cache = FileCache.instance

    file_path = Path(__file__)

    assert not file_cache.test_cached_file(file_path)
    file_cache.add_to_cache(file_path)

    assert file_cache.test_cached_file(file_path)

    cache_path = file_cache.get_cached_file(file_path)
    assert cache_path.exists()

    orig_hasher = md5()
    orig_hasher.update(file_path.read_bytes())

    cache_hasher = md5()
    cache_hasher.update(cache_path.read_bytes())

    assert orig_hasher.digest() == cache_hasher.digest()

    file_cache.remove_from_cache(file_path)

def test_garbage_collection():
    big_file_cache = FileCache.instance

    file_path = Path(__file__)
    big_file_cache.add_to_cache(file_path)

    small_file_cache = FileCache(max_storage_mb=0)
    
    assert small_file_cache.test_cached_file(file_path)
    small_file_cache._collect_garbage()
    small_file_cache._garbage_collector_thread.join()
    assert not small_file_cache.test_cached_file(file_path)