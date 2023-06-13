"""
Threaded PyMySQL Implementation
"""
import getpass
from typing import Optional, Any, List

import pypool
import pymysql

from ..uri import DatabaseURI
from ..interface import *

#** Variables **#

#: typehint for timeout specification
Timeout = Optional[float]

#: alias for pymysql connection
RawConn = pymysql.Connection

#: common connection exception
NotAquired = ConnectionError('Mysql connection not acquired')

#** Funcitons **#

def setdefault(kwargs: dict, 
    uri: DatabaseURI, key: str, convert: type, default: Any = None):
    """set-default for kwargs from uri options"""
    default = uri.options.get(key) or default
    if default is not None:
        kwargs.setdefault(key, convert(default))

#** Classes **#

class ConnPool:
    """PyMYSQL Connection Pool Implementation"""
    pool: pypool.Pool[RawConn]

    def __init__(self, uri: DatabaseURI, **kwargs):
        setdefault(kwargs, uri, 'max_size', int)
        setdefault(kwargs, uri, 'min_size', int)
        setdefault(kwargs, uri, 'expiratioin', float)
        setdefault(kwargs, uri, 'timeout', float)
        self.uri  = uri
        self.pool = pypool.Pool(self.factory, cleanup=self.cleanup, **kwargs)

    def factory(self) -> RawConn:
        """connection factory function"""
        ssl = self.uri.options.get('ssl')
        if isinstance(ssl, str):
            ssl = ssl.lower()
            self.uri.options['ssl'] = {'true': True, 'false': False}[ssl]
        return RawConn(
            host=self.uri.hostname,
            port=self.uri.port or 3306,
            user=self.uri.username or getpass.getuser(),
            password=self.uri.password or getpass.getpass(),
            db=self.uri.database,
            autocommit=True,
            **self.uri.options,
        )

    def cleanup(self, conn: RawConn):
        """connection cleanup function"""
        if conn.open:
            conn.close()

    def get(self, block: bool = True, timeout: Timeout = None) -> RawConn:
        """retrieve connection from the pool"""
        return self.pool.get(block, timeout)

    def put(self, conn: RawConn, block: bool = True, timeout: Timeout = None):
        """place connection back into the pool"""
        self.pool.put(conn, block, timeout)

    def drain(self):
        """drain and close all open connections"""
        self.pool.drain()

class MysqlTransaction(ITransaction):
    """Internal Mysql Transaction Interface"""
 
    def __init__(self, conn: 'MysqlConnection'):
        self.conn    = conn
        self.is_root = False
        self.savepoint: Optional[str] = None
 
    def _execute(self, query: str):
        """internal executor function"""
        if self.conn.conn is None:
            raise NotAquired
        if self.is_root:
            self.conn.conn.query(query)
            return
        with self.conn.conn.cursor() as cursor:
            cursor.execute(query)

class MysqlConnection(IConnection):
    """Internal Mysql Connection Interface"""

    def __init__(self, pool: ConnPool):
        self.pool: ConnPool          = pool
        self.conn: Optional[RawConn] = None

    def acquire(self):
        """

        """
        if self.conn is not None:
            raise ConnectionError('Mysql connection already aquired')
        self.conn = self.pool.get()

    def release(self):
        """

        """
        if self.conn is None:
            raise NotAquired
        self.pool.put(self.conn)
        self.conn = None

    def fetch_one(self, query: Query) -> Optional[Record]:
        """

        """
        if self.conn is None:
            raise NotAquired
        with self.conn.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchone()

    def fetch_all(self, query: Query) -> List[Record]:
        """

        """
        if self.conn is None:
            raise NotAquired
        with self.conn.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()

    def execute(self, query: Query):
        """

        """
        if self.conn is None:
            raise NotAquired
        with self.conn.cursor() as cursor:
            cursor.execute(query)

    def transaction(self) -> ITransaction:
        """

        """
        return MysqlTransaction(self)

    @property
    def raw_connection(self):
        """

        """
        return self.conn

class MysqlDatabase(IDatabase):
    """Internal Mysql Database Interface"""

    def __init__(self, uri: DatabaseURI, **kwargs):
        self.uri    = uri
        self.kwargs = kwargs
        self.pool: Optional[ConnPool] = None

    def connect(self):
        """

        """
        if self.pool is not None:
            raise ConnectionError('Mysql already connected')
        self.pool = ConnPool(self.uri, **self.kwargs)

    def disconnect(self):
        """

        """
        if self.pool is None:
            raise ConnectionError('Mysql not connected')
        self.pool.drain()
        self.poool = None

    def connection(self) -> IConnection:
        """

        """
        if self.pool is None:
            raise ConnectionError('Mysql not connected')
        return MysqlConnection(self.pool)
