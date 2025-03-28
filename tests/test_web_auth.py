'''Web Auth Tests
'''
import contextlib
import sqlite3
from pathlib import Path

from fishsense_data_processing_spider.web_auth import KeyStore


def test_from_empty(tmp_path: Path):
    """Tests loading db from empty dir

    Args:
        tmp_path (Path): Temporary path
    """
    db_path = tmp_path / 'keys.db'
    dut = KeyStore(db_path)
    assert dut

    with contextlib.closing(sqlite3.connect(db_path)) as con, \
            contextlib.closing(con.cursor()) as cur:
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        table_names = {row[0] for row in cur.fetchall()}
        assert table_names == {'keys', 'params', 'version'}
