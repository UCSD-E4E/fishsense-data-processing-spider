'''Web Auth
'''
import contextlib
import datetime as dt
import hashlib
import secrets
import sqlite3
from pathlib import Path
from typing import Optional, Tuple


class KeyStore:
    """API Key Store
    """
    VERSION = 1
    ITERATIONS = 200000

    def __init__(self,
                 path: Path):
        self.__path = path
        self.__salt: str = None
        self.__iterations: int = self.ITERATIONS
        self.initialize_db()

    def initialize_db(self):
        """Initializes the database
        """
        with contextlib.closing(sqlite3.connect(self.__path)) as con, \
                contextlib.closing(con.cursor()) as cur:
            try:
                cur.execute('SELECT version FROM version;')
                version, = cur.fetchone()
            except sqlite3.OperationalError:
                version = 0

            if version < 1:
                cur.execute(
                    'CREATE TABLE keys (hash TEXT PRIMARY KEY, expires INTEGER, comment TEXT);'
                )
                cur.execute(
                    'CREATE TABLE params (idx INTEGER PRIMARY KEY, salt TEXT, iterations INTEGER);'
                )
                new_salt = secrets.token_hex()
                cur.execute(
                    'INSERT INTO params (idx, salt, iterations) VALUES (0, :salt, :iterations);',
                    {
                        'salt': new_salt,
                        'iterations': self.ITERATIONS
                    }
                )
                cur.execute(
                    'CREATE TABLE version (version INTEGER PRIMARY KEY);'
                )
                cur.execute(
                    'INSERT INTO version (version) VALUES (:version);',
                    {
                        'version': 1
                    }
                )
            con.commit()
            cur.execute(
                'SELECT salt, iterations FROM params WHERE idx = 0;'
            )
            self.__salt, self.__iterations = cur.fetchone()

    def get_new_key(self, comment: str, expires: Optional[dt.datetime] = None) -> str:
        """Generates and stores a new API key

        Args:
            comment (str): Comment describing this key
            expires (Optional[dt.datetime]): Expiration.  Defaults to 400 days

        Returns:
            str: New API Key
        """
        new_key = secrets.token_hex()
        new_hash = self.__hash_key(new_key)
        if not expires:
            expires = dt.datetime.now() + dt.timedelta(days=400)
        with contextlib.closing(sqlite3.connect(self.__path)) as con, \
                contextlib.closing(con.cursor()) as cur:
            cur.execute(
                'INSERT INTO keys (hash, expires, comment) VALUES (:hash, :expires, :comment);',
                {
                    'hash': new_hash,
                    'expires': int(expires.timestamp()),
                    'comment': comment
                }
            )
            con.commit()
        return new_key

    def __hash_key(self, new_key: str) -> str:
        return hashlib.pbkdf2_hmac('sha256',
                                   new_key.encode(),
                                   self.__salt.encode(),
                                   self.__iterations)

    def authorize_key(self, key: str) -> bool:
        """Checks if the key is authorized

        Args:
            key (str): Key to check

        Returns:
            bool: True if authorized, otherwise False
        """
        hash_to_verify = self.__hash_key(key)
        with contextlib.closing(sqlite3.connect(self.__path)) as con, \
                contextlib.closing(con.cursor()) as cur:
            cur.execute(
                'SELECT expires FROM keys WHERE hash = :hash LIMIT 1;',
                {
                    'hash': hash_to_verify
                }
            )
            result: Optional[Tuple[int]] = cur.fetchone()
        if not result:
            return False
        expires = dt.datetime.fromtimestamp(result[0])
        if expires < dt.datetime.now():
            return False
        return True
