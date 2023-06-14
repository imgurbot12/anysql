"""
Threaded Psycopg2
"""
import getpass
import logging
from typing import Optional, List, Any, TypeAlias

import pypool
import psycopg2

from . import Timeout, setdefault
from ..uri import DatabaseURI
from ..interface import *

#** Variables **#

#: backend logging instance
logger = logging.getLogger('anysql.postgres')

#: postgres connection type
RawConn: TypeAlias = Any #type: psycopg2.connection

#: common connection exception
NotAquired = ConnectionError('Mysql connection not acquired')

#** Classes **#

class ConnPool:
    """Psycopg2 Connection Pool Implementation"""
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
        """connection factory function"""
        logger.debug(f'conn-pool size={self.pool.pool_size} max={self.pool.min_size}')
        ssl = self.uri.options.get('ssl')
        if isinstance(ssl, str):
            ssl = ssl.lower()
            self.uri.options['ssl'] = {'true': True, 'false': False}[ssl]
        conn = psycopg2.connect(
            host=self.uri.hostname,
            port=self.uri.port or 5432,
            user=self.uri.username or getpass.getuser(),
            password=self.uri.password,
            database=self.uri.database,
            **self.uri.options,
        )
        conn.autocommit = True
        return conn

    def cleanup(self, conn: RawConn):
        """connection cleanup function"""
        if not conn.closed:
            conn.close()

    def get(self, block: bool = True, timeout: Timeout = None) -> RawConn:
        """retrieve connection from the pool"""
        return self.pool.get(block, timeout)

    def put(self, conn: RawConn, block: bool = True, timeout: Timeout = None):
        """place connection back into the pool"""
        self.pool.put(conn, block, timeout)

    def drain(self):
        """drain and close all open connections"""
        logger.debug(f'draining {self.pool.pool_size} connections')
        self.pool.drain()

class PostgresTransaction(ITransaction):
    """Internal Psycopg2 Transaction Interface"""
 
    def __init__(self, conn: 'PostgresConnection'):
        self.conn    = conn
        self.is_root = False
        self.savepoint: Optional[str] = None
 
    def _execute(self, query: str):
        """internal executor function"""
        if self.conn.conn is None:
            raise NotAquired
        with self.conn.conn.cursor() as cursor:
            cursor.execute(query)

class PostgresConnection(IConnection):
    """Internal Psycopg2 Connection Interface"""

    def __init__(self, pool: ConnPool):
        self.pool: ConnPool          = pool
        self.conn: Optional[RawConn] = None

    def acquire(self):
        """
        acquire mysql connection
        """
        if self.conn is not None:
            raise ConnectionError('Mysql connection already aquired')
        self.conn = self.pool.get()

    def release(self):
        """
        release mysql connection
        """
        if self.conn is None:
            raise NotAquired
        self.pool.put(self.conn)
        self.conn = None

    def fetch_one(self, query: Query) -> Optional[Record]:
        """
        fetch a single record using the specified query
        """
        if self.conn is None:
            raise NotAquired
        with self.conn.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchone()

    def fetch_all(self, query: Query) -> List[Record]:
        """
        fetch a list of records using the specified query
        """
        if self.conn is None:
            raise NotAquired
        with self.conn.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()

    def execute(self, query: Query):
        """
        execute the following query
        """
        if self.conn is None:
            raise NotAquired
        with self.conn.cursor() as cursor:
            cursor.execute(query)

    def transaction(self) -> ITransaction:
        """
        spawn transaction handler for mysql
        """
        return PostgresTransaction(self)

    @property
    def raw_connection(self):
        """
        retreive underlying pymysql database object
        """
        return self.conn

class PostgresDatabase(IDatabase):
    """Internal Psycopg2 Database Interface"""

    def __init__(self, uri: DatabaseURI, **kwargs):
        self.uri    = uri
        self.kwargs = kwargs
        self.pool: Optional[ConnPool] = None

    def connect(self):
        """
        connect to mysql database
        """
        logger.debug(f'connecting to {self.uri.obscure_password}')
        if self.pool is not None:
            raise ConnectionError('Postgres already connected')
        self.pool = ConnPool(self.uri, **self.kwargs)

    def disconnect(self):
        """
        disconnect from mysql database
        """
        logger.debug(f'disconecting from {self.uri.obscure_password}')
        if self.pool is None:
            raise ConnectionError('Postgres not connected')
        self.pool.drain()
        self.pool = None

    def connection(self) -> IConnection:
        """
        return connection for mysql database
        """
        if self.pool is None:
            raise ConnectionError('Postgres not connected')
        return PostgresConnection(self.pool)
