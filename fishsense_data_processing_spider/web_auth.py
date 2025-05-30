'''Web Auth
'''
import contextlib
import datetime as dt
import enum
import hashlib
import logging
import secrets
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Tuple


class Permission(enum.Enum):
    """Operation ID permissions
    """
    DO_DISCOVERY = 'doDiscovery'
    DO_LABEL_STUDIO_SYNC = 'doLabelStudioSync'
    GET_RAW_FILE = 'getRawFile'
    PUT_PREPROCESS_JPEG = 'putPreprocessedFrame'
    GET_LASER_LABEL = 'getLaserLabel'
    GET_LASER_FRAME = 'getLaserFrame'
    PUT_LASER_FRAME = 'putLaserFrame'
    PUT_DEBUG_BLOB = 'putDebugBlob'
    ADMIN = 'admin'
    GET_METADATA = 'getMetadata'
class KeyStore:
    """API Key Store
    """
    ITERATIONS = 200000

    def __init__(self,
                 path: Path):
        self.__log = logging.getLogger('keystore')
        self.__log.setLevel(logging.INFO)
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
            if version < 2:
                cur.execute(
                    'ALTER TABLE keys ADD COLUMN doDiscovery INTEGER DEFAULT FALSE;'
                )
                cur.execute(
                    'UPDATE version SET version=2;'
                )
            if version < 3:
                cur.execute(
                    'ALTER TABLE keys ADD COLUMN doLabelStudioSync INTEGER DEFAULT FALSE;'
                )
                cur.execute(
                    'UPDATE version SET version=3;'
                )
            if version < 4:
                cur.execute(
                    'ALTER TABLE keys ADD COLUMN getRawFile INTEGER DEFAULT FALSE;'
                )
                cur.execute('UPDATE version SET version=4;')
            if version < 5:
                cur.execute(
                    'ALTER TABLE keys ADD COLUMN putPreprocessedFrame INTEGER DEFAULT FALSE;'
                )
                cur.execute('UPDATE version SET version=5;')
            if version < 6:
                cur.execute(
                    'ALTER TABLE keys ADD COLUMN getLaserLabel INTEGER DEFAULT FALSE;'
                )
                cur.execute('UPDATE version SET version=6;')
            if version < 7:
                cur.execute(
                    'ALTER TABLE keys ADD COLUMN getLaserFrame INTEGER DEFAULT FALSE;'
                )
                cur.execute(
                    'ALTER TABLE keys ADD COLUMN putLaserFrame INTEGER DEFAULT FALSE;'
                )
                cur.execute('UPDATE version SET version=7;')
            if version < 8:
                cur.execute(
                    'ALTER TABLE keys ADD COLUMN putDebugBlob INTEGER DEFAULT FALSE;'
                )
                cur.execute(
                    'ALTER TABLE keys ADD COLUMN admin INTEGER DEFAULT FALSE;'
                )
                cur.execute('UPDATE version SET version=8;')
            if version < 9:
                cur.execute(
                    'ALTER TABLE keys ADD COLUMN getMetadata INTEGER DEFAULT FALSE;'
                )
                cur.execute('UPDATE version SET version=9;')
            con.commit()
            cur.execute(
                'SELECT salt, iterations FROM params WHERE idx = 0;'
            )
            self.__salt, self.__iterations = cur.fetchone()

    def get_new_key(self,
                    comment: str,
                    expires: Optional[dt.datetime] = None) -> Tuple[str, dt.datetime]:
        """Generates and stores a new API key

        Args:
            comment (str): Comment describing this key
            expires (Optional[dt.datetime]): Expiration.  Defaults to 400 days

        Returns:
            Tuple[str, dt.datetime]: New API Key, expiration timestamp
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
        return new_key, expires

    def list_hashes(self) -> Dict[str, str]:
        """List hashes

        Returns:
            Dict[str, str]: Mapping of hash to comment
        """
        with contextlib.closing(sqlite3.connect(self.__path)) as con, \
                contextlib.closing(con.cursor()) as cur:
            cur.execute(
                'SELECT hash, comment FROM keys WHERE expires > :now;',
                {
                    'now': int(dt.datetime.now().timestamp())
                }
            )
            return {
                row[0]: row[1]
                for row in cur.fetchall()
            }

    def set_perm(self, key: str, op: Permission, value: bool) -> None:
        """Sets the permissions for the given key

        Args:
            key (str): Key to set perms for
            op (Permission): Operation ID to set
            value (bool): Allow
        """
        key_hash = self.__hash_key(key)
        with contextlib.closing(sqlite3.connect(self.__path)) as con, \
                contextlib.closing(con.cursor()) as cur:
            cur.execute(
                f'UPDATE keys SET {op.value} = :value WHERE hash = :hash;',
                {
                    'hash': key_hash,
                    'value': value
                }
            )
            con.commit()

    def get_perm(self, key: str) -> List[Permission]:
        """Gets the list of permissions associated with this key

        Args:
            key (str): Key to check

        Returns:
            List[Permission]: List of allowed Permissions
        """
        key_hash = self.__hash_key(key)
        allowed_perms: List[Permission] = []
        with contextlib.closing(sqlite3.connect(self.__path)) as con, \
                contextlib.closing(con.cursor()) as cur:
            for perm in Permission:
                cur.execute(
                    f'SELECT {perm.value} FROM keys WHERE hash = :hash;',
                    {
                        'hash': key_hash
                    }
                )
                result = cur.fetchone()
                if result[0] == 1:
                    allowed_perms.append(perm)
        return allowed_perms

    def __hash_key(self, key: str) -> str:
        return hashlib.pbkdf2_hmac('sha256',
                                   key.encode(),
                                   self.__salt.encode(),
                                   self.__iterations)

    def authorize_key(self, key: str, perm: Optional[Permission] = None) -> bool:
        """Checks if the key is authorized

        Args:
            key (str): Key to check
            perm (Permission): ACL to check

        Returns:
            bool: True if authorized, otherwise False
        """
        hash_to_verify = self.__hash_key(key)
        self.__log.debug('Computed hash %s', hash_to_verify)
        with contextlib.closing(sqlite3.connect(self.__path)) as con, \
                contextlib.closing(con.cursor()) as cur:
            if perm:
                query = f'SELECT expires, {perm.value} FROM keys WHERE hash = :hash LIMIT 1;'
            else:
                query = 'SELECT expires FROM keys WHERE hash = :hash LIMIT 1;'
            cur.execute(
                query,
                {
                    'hash': hash_to_verify
                }
            )
            result: Optional[Tuple[int]] = cur.fetchone()
        if not result:
            self.__log.info('Key failed - not present')
            return False
        expires = dt.datetime.fromtimestamp(result[0])
        if expires < dt.datetime.now():
            self.__log.info('Key failed - expired')
            return False
        if perm and not bool(result[1]):
            self.__log.info('Key failed - not authorized for %s', perm)
            return False
        return True
