"""
Threaded SQLite Implementation
"""
import queue
import logging
import sqlite3
import functools
import threading
from concurrent.futures import Future
from typing import Callable, Generator, Optional, Any, List, TypeVar
from typing_extensions import ParamSpec, Self

from ..uri import DatabaseURI
from ..interface import *

#** Variables **#

P = ParamSpec('P')
R = TypeVar('R')

#: backend logging instance
logger = logging.getLogger('anysql.sqlite')

#: funciton in charge of generating a connection
Connector = Callable[[], sqlite3.Connection]

#: common exception raised on connection error
ConnectionClosed = ConnectionError('Connection closed')

#** Classes **#

class Cursor:
    """Sqlite Thread Supported Cursor Instance"""
    __slots__ = ('db', 'cur')

    def __init__(self, db: 'Database', cur: sqlite3.Cursor):
        self.db  = db
        self.cur = cur

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_):
        self.close()

    def _execute(self, func: Callable, *args, **kwargs):
        """execute function using database thread to run"""
        return self.db._execute(func, *args, **kwargs)

    def execute(self, query: str) -> Self:
        """
        execute the given query and return the cursor result
        """
        self._execute(self.cur.execute, query)
        return self

    def fetchone(self):
        """
        retrieve single result from executed query
        """
        return self._execute(self.cur.fetchone)

    def fetchall(self):
        """
        retrieve all results from the executed query
        """
        return self._execute(self.cur.fetchall)

    def fetchyield(self, query: Query) -> Generator[Record, None, None]:
        """
        retrieve generator of results from executed query
        """
        cursor = self._execute(self.cur.execute, query)
        while True:
            try:
                yield self._execute(next, cursor)
            except StopIteration:
                break

    def close(self):
        """
        close cursor and exit
        """
        return self._execute(self.cur.close)

class Database(threading.Thread):
    """Sqlite Thread Supported Database Connection Instance"""
    __slots__ = ('daemon', 'queue', 'boundry', 'connector', 'connection')

    queue:      queue.Queue
    connection: Optional[sqlite3.Connection]

    def __init__(self, connector: Connector):
        super().__init__()
        self.daemon     = True
        self.queue      = queue.Queue()
        self.boundry    = threading.Barrier(2)
        self.connector  = connector
        self.connection = None

    def run(self):
        """main function used for calling sqlite actions"""
        while not self.boundry.n_waiting:
            try:
                future, function = self.queue.get(timeout=0.1)
            except queue.Empty:
                continue
            try:
                result = function()
                future.set_result(result)
            except BaseException as exc:
                future.set_exception(exc)
        self.boundry.wait()

    def _execute(self,
        func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> R:
        """
        internal function to pass func to run into thread and get result
        """
        if not self.connection:
            raise ConnectionClosed
        future: Future[R] = Future()
        if args or kwargs:
            func = functools.partial(func, *args, **kwargs)
        self.queue.put((future, func))
        return future.result()

    def __enter__(self) -> Self:
        self.connect()
        return self

    def __exit__(self, *_):
        self.disconnect()

    def connect(self):
        """
        connect to sqlite w/ the specified connector
        """
        if not self.is_alive():
            self.start()
        if self.connection is not None:
            return
        future = Future()
        self.queue.put((future, self.connector))
        self.connection = future.result()

    def disconnect(self):
        """
        disconnect from sqlite if connected
        """
        if self.connection is None:
            return
        self._execute(self.connection.close)
        self.boundry.wait()
        self.connection = None
        self.boundry.reset()

    def commit(self):
        """
        commit queued changes to db
        """
        if self.connection is None:
           raise ConnectionClosed
        return self._execute(self.connection.commit)

    def rollback(self):
        """
        revert queued changes from db
        """
        if self.connection is None:
           raise ConnectionClosed
        return self._execute(self.connection.rollback)

    def cursor(self) -> Cursor:
        """
        generate a sqlite cursor object for making queries
        """
        if self.connection is None:
           raise ConnectionClosed
        cursor = self._execute(self.connection.cursor)
        return Cursor(self, cursor)

    def execute(self, query: str, close: bool = False) -> Cursor:
        """
        execute the given query on the db and return a cursor
        """
        if self.connection is None:
           raise ConnectionClosed
        rawcur = self._execute(self.connection.execute, query)
        cursor = Cursor(self, rawcur)
        if close:
            cursor.close()
        return cursor

class SqliteTransaction(ITransaction):
    """Internal SQLite Backend Transaction Interface"""
    __slots__ = ('db', 'is_root', 'savepoint')

    def __init__(self, db: Database):
        self.db        = db
        self.is_root   = False
        self.savepoint = None

    def _execute(self, query: str):
        """handle query execution"""
        self.db.execute(query, close=True)

class SqliteConnection(IConnection):
    """Internal Sqlite Connection Interface"""
    __slots__ = ('db', 'connected')

    def __init__(self, db: Database):
        self.db        = db
        self.connected = False

    def acquire(self):
        """
        acquire sqlite connection
        """
        if self.connected:
            raise ConnectionError('Connection already acquired')
        self.connected = True

    def release(self):
        """
        release sqlite connection
        """
        if not self.connected:
            raise ConnectionError('Connection not acquired')
        self.connected = False

    def fetch_one(self, query: Query) -> Optional[Record]:
        """
        fetch a single record using the specified query
        """
        cursor = self.db.execute(query)
        return cursor.fetchone()

    def fetch_all(self, query: Query) -> List[Record]:
        """
        fetch a list of records using the specified query
        """
        cursor = self.db.execute(query)
        return cursor.fetchall()

    def fetch_yield(self, query: Query) -> Generator[Record, None, None]:
        """
        fetch a generator of records using the specified query
        """
        cursor = self.db.cursor()
        return cursor.fetchyield(query)

    def execute(self, query: Query):
        """
        execute the following query
        """
        self.db.execute(query)

    def transaction(self) -> ITransaction:
        """
        spawn transaction handler for sqlite
        """
        return SqliteTransaction(self.db)

    @property
    def raw_connection(self):
        """
        retreive underlying sqlite database object
        """
        return self.db

class SqliteDatabase(IDatabase):
    """Internal Sqlite Database Interface"""
    __slots__ = ('uri', 'db', 'conn')

    def __init__(self, uri: DatabaseURI, **kwargs: Any):
        kwargs.setdefault('isolation_level', None)
        url       = str(uri).split('//', 1)[-1]
        self.uri  = uri
        self.db   = Database(lambda: sqlite3.connect(url, **kwargs))
        self.conn = SqliteConnection(self.db)

    def connect(self):
        """
        connect to sqlite database
        """
        logger.debug(f'connecting to {self.uri.obscure_password}')
        return self.db.connect()

    def disconnect(self):
        """
        disconnect from sqlite database
        """
        logger.debug(f'disconecting from {self.uri.obscure_password}')
        return self.db.disconnect()

    def connection(self) -> IConnection:
        """
        return connection for sqlite database
        """
        return self.conn
