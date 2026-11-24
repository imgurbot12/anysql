"""
Threaded PyMySQL Implementation
"""
import getpass
import logging
from typing import Generator, Optional, List

import pypool
import pymysql

from . import Timeout, setdefault
from ..uri import DatabaseURI
from ..interface import IConnection, IDatabase, ITransaction, Query, Record

#** Variables **#

#: backend logger instance
logger = logging.getLogger('anysql.mysql')

#: alias for pymysql connection
RawConn = pymysql.Connection

#: common connection exception
NotAquired = ConnectionError('Mysql connection not acquired')

#** Classes **#

class ConnPool:
    """
    PyMYSQL Connection Pool Implementation
    """
    __slots__ = ('uri', 'pool')

    pool: pypool.Pool[RawConn]

    def __init__(self, uri: DatabaseURI, **kwargs):
        setdefault(kwargs, uri, 'max_size', int)
        setdefault(kwargs, uri, 'min_size', int)
        setdefault(kwargs, uri, 'expiration', float, 600)
        setdefault(kwargs, uri, 'timeout', float)
        self.uri  = uri
        self.pool = pypool.Pool(self.factory, cleanup=self.cleanup, **kwargs)
        logger.debug('conn-pool max={0.max_size} min={0.min_size} expr={0.expiration}'.format(self.pool))

    def factory(self) -> RawConn:
        """
        Connection factory function
        """
        logger.debug(f'conn-pool size={self.pool.pool_size} max={self.pool.min_size}')
        ssl = self.uri.options.get('ssl')
        if isinstance(ssl, str):
            ssl = ssl.lower()
            self.uri.options['ssl'] = {'true': True, 'false': False}[ssl]
        return RawConn(
            host=self.uri.hostname,
            port=self.uri.port or 3306,
            user=self.uri.username or getpass.getuser(),
            password=self.uri.password or '',
            database=self.uri.database,
            autocommit=True,
            **self.uri.options,
        )

    def cleanup(self, conn: RawConn):
        """
        Connection cleanup function
        """
        if conn.open:
            conn.close()

    def get(self, block: bool = True, timeout: Timeout = None) -> RawConn:
        """
        Retrieve connection from the pool
        """
        return self.pool.get(block, timeout)

    def put(self, conn: RawConn, block: bool = True, timeout: Timeout = None):
        """
        Place connection back into the pool
        """
        self.pool.put(conn, block, timeout)

    def drain(self):
        """
        Drain and close all open connections
        """
        logger.debug(f'draining {self.pool.pool_size} connections')
        self.pool.drain()

class MysqlTransaction(ITransaction):
    """
    Internal MySQL Transaction Interface
    """
    __slots__ = ('conn', 'is_root', 'savepoint')

    def __init__(self, conn: 'MysqlConnection'):
        self.conn      = conn
        self.is_root   = False
        self.savepoint = None

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
    """
    Internal MySQL Connection Interface
    """
    __slots__ = ('pool', 'conn')

    def __init__(self, pool: ConnPool):
        self.pool: ConnPool          = pool
        self.conn: Optional[RawConn] = None

    def acquire(self):
        """
        Acquire MySQL connection
        """
        if self.conn is not None:
            raise ConnectionError('Mysql connection already aquired')
        self.conn = self.pool.get()

    def release(self):
        """
        Release MySQL connection
        """
        if self.conn is None:
            raise NotAquired
        self.pool.put(self.conn)
        self.conn = None

    def fetch_one(self, query: Query) -> Optional[Record]:
        """
        Fetch a single record using the specified query
        """
        if self.conn is None:
            raise NotAquired
        with self.conn.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchone()

    def fetch_all(self, query: Query) -> List[Record]:
        """
        Fetch a list of records using the specified query
        """
        if self.conn is None:
            raise NotAquired
        with self.conn.cursor() as cursor:
            cursor.execute(query)
            return list(cursor.fetchall())

    def fetch_yield(self, query: Query) -> Generator[Record, None, None]:
        """
        Fetch a generator of records using the specified query
        """
        if self.conn is None:
            raise NotAquired
        with self.conn.cursor() as cursor:
            cursor.execute(query)
            for row in cursor:
                yield row

    def execute(self, query: Query) -> int:
        """
        Execute the following query
        """
        if self.conn is None:
            raise NotAquired
        with self.conn.cursor() as cursor:
            cursor.execute(query)
            return cursor.rowcount

    def transaction(self) -> ITransaction:
        """
        Spawn transaction handler for MySQL
        """
        return MysqlTransaction(self)

    @property
    def raw_connection(self):
        """
        Retrieve underlying MySQL database object
        """
        return self.conn

class MysqlDatabase(IDatabase):
    """
    Internal MySQL Database Interface
    """
    __slots__ = ('uri', 'kwargs', 'pool')

    def __init__(self, uri: DatabaseURI, **kwargs):
        self.uri    = uri
        self.kwargs = kwargs
        self.pool: Optional[ConnPool] = None

    def connect(self):
        """
        Connect to MySQL database
        """
        logger.debug(f'connecting to {self.uri.obscure_password}')
        if self.pool is not None:
            raise ConnectionError('Mysql already connected')
        self.pool = ConnPool(self.uri, **self.kwargs)

    def disconnect(self):
        """
        Disconnect from MySQL database
        """
        logger.debug(f'disconecting from {self.uri.obscure_password}')
        if self.pool is None:
            raise ConnectionError('Mysql not connected')
        self.pool.drain()
        self.pool = None

    def connection(self) -> IConnection:
        """
        Return connection for MySQL database
        """
        if self.pool is None:
            raise ConnectionError('Mysql not connected')
        return MysqlConnection(self.pool)
