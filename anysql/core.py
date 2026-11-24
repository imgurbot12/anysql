"""
AnySQL Core Implementation
"""
import logging
import functools
import threading
import importlib
import contextvars
from typing import Any, Generator, List, Sequence, Optional, Callable
from typing_extensions import Self

from .uri import DatabaseURI
from .escape import mogrify, prepare
from .interface import Args, IConnection, IDatabase, ITransaction, Record, Stmt

#** Variables **#
__all__ = [
    'BACKENDS',

    'Database',
    'Connection',
    'Transaction'
]

#: core logging instance
logger = logging.getLogger('anysql')

#: supported backends registry
BACKENDS = {
    'mysql':    ('anysql.backends.pymysql', 'MysqlDatabase'),
    'sqlite':   ('anysql.backends.sqlite', 'SqliteDatabase'),
    'postgres': ('anysql.backends.postgres', 'PostgresDatabase'),
}

#** Functions **#

def get_backend(uri: DatabaseURI, kwargs: dict) -> IDatabase:
    """
    Generate and retrieve backend with the specified URI
    """
    module_name, attr = BACKENDS.get(uri.scheme) or BACKENDS[uri.dialect]
    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        if exc.name != module_name:
            raise exc from None
        raise ImportError(f'Cannot find module: {module_name!r}')
    instance = module
    try:
        for name in attr.split('.'):
            instance = getattr(instance, name)
    except AttributeError as exc:
        raise ImportError(f'Attribute: {attr!r} not found in {module_name!r}')
    # ensure instance is the correct type
    if not isinstance(instance, type) or not issubclass(instance, IDatabase):
        raise ImportError(
            f'Attribute: {attr!r} from {module_name!r} not an IDatabase')
    return instance(uri, **kwargs)

#** Classes **#

class Database:
    """
    AnySQL Thread-safe Database Implementation
    """
    __slots__ = ('uri', 'backend', 'connected', 'context', 'cache_statements')

    context: contextvars.ContextVar

    def __init__(self, uri: str, cache_statements: bool = True, **kwargs):
        self.uri              = DatabaseURI(uri)
        self.backend          = get_backend(self.uri, kwargs)
        self.connected        = False
        self.context          = contextvars.ContextVar("connection_context")
        self.cache_statements = cache_statements

    def __enter__(self) -> Self:
        """
        Auto-connect on start of context
        """
        self.connect()
        return self

    def __exit__(self, *_):
        """
        Auto-disconnect at end of context
        """
        self.disconnect()

    def to_async(self) -> 'AsyncDatabase':
        """
        Convert database instance into async compatible version
        """
        return AsyncDatabase(self)

    def connect(self):
        """
        Connect to database if not already connected
        """
        if self.connected:
            logger.debug(f'{self.uri.obscure_password} already connected.')
            return
        logger.debug(f'connecting to {self.uri.obscure_password}')
        self.backend.connect()
        self.connected = True

    def disconnect(self):
        """
        Disconnect from database if not already disconnected
        """
        if not self.connected:
            logger.debug(f'{self.uri.obscure_password} already disconnected.')
            return
        logger.debug(f'disconnecting from {self.uri.obscure_password}')
        self.backend.disconnect()

    def connection(self) -> 'Connection':
        """
        Spawn/return connection based on local thread context
        """
        try:
            return self.context.get()
        except LookupError:
            connection = Connection(self.backend.connection())
            self.context.set(connection)
            return connection

    def fetch_one(self, query: Stmt, values: Args = None) -> Optional[Record]:
        """
        Fetch a single record matching the given SQL statement
        """
        with self.connection() as conn:
            return conn.fetch_one(query, values)

    def fetch_all(self, query: Stmt, values: Args = None) -> List[Record]:
        """
        Fetch all the records matching the given SQL statement
        """
        with self.connection() as conn:
            return conn.fetch_all(query, values)

    def fetch_yield(self,
        query: Stmt, values: Args = None) -> Generator[Record, None, None]:
        """
        Fetch generator of records matching the given SQL statement
        """
        with self.connection() as conn:
            for record in conn.fetch_yield(query, values):
                yield record

    def execute(self, query: Stmt, values: Args = None) -> int:
        """
        Execute the SQL statement once with the given values
        """
        with self.connection() as conn:
            return conn.execute(query, values)

    def execute_many(self, query: Stmt, values: Sequence[Args]):
        """
        Execute the SQL statement with multiple sets of values
        """
        with self.connection() as conn:
            return conn.execute_many(query, values)

    def transaction(self, force_rollback: bool = False, **options) -> 'Transaction':
        """
        Generate transaction and return controls with the given settings
        """
        return Transaction(None, self.connection, force_rollback, **options)

class Connection:
    """
    AnySQL Threadsafe Connection Implementation
    """
    __slots__ = ('backend', 'conn_lock', 'conn_count',
        'tran_lock', 'tran_stack', 'cache_statements')

    def __init__(self, backend: IConnection, cache_statements: bool = True):
        self.backend    = backend
        self.conn_lock  = threading.Lock()
        self.conn_count = 0
        self.tran_lock  = threading.Lock()
        self.tran_stack: List['Transaction'] = []
        self.cache_statements = cache_statements

    def __enter__(self) -> Self:
        """
        Track number of connections and acquire connection when one
        """
        with self.conn_lock:
            self.conn_count += 1
            if self.conn_count == 1:
                try:
                    self.backend.acquire()
                except BaseException as exc:
                    self.conn_count -= 1
                    raise exc
        return self

    def __exit__(self, *_):
        """
        Track number of connections and release connection when zero
        """
        with self.conn_lock:
            self.conn_count -= 1
            if self.conn_count == 0:
                self.backend.release()

    def fetch_one(self, query: Stmt, values: Args = None) -> Optional[Record]:
        """
        Fetch a single record matching the given SQL statement
        """
        query = mogrify(query, values, self.cache_statements)
        return self.backend.fetch_one(query)

    def fetch_all(self, query: Stmt, values: Args = None) -> List[Record]:
        """
        Fetch all the records matching the given SQL statement
        """
        query = mogrify(query, values, self.cache_statements)
        return self.backend.fetch_all(query)

    def fetch_yield(self,
        query: Stmt, values: Args = None) -> Generator[Record, None, None]:
        """
        Fetch generator of records matching the given SQL statement
        """
        query = mogrify(query, values, self.cache_statements)
        return self.backend.fetch_yield(query)

    def execute(self, query: Stmt, values: Args = None):
        """
        Execute the SQL statement once with the given values
        """
        query = mogrify(query, values, self.cache_statements)
        return self.backend.execute(query)

    def execute_many(self, query: Stmt, values: Sequence[Args]):
        """
        Execute the sql statement with multiple sets of values
        """
        query = prepare(query)
        for vset in values:
            stmt = mogrify(query, vset, self.cache_statements)
            self.backend.execute(stmt)

    def transaction(self, force_rollback: bool = False, **options) -> 'Transaction':
        """
        Generate transaction and return controls with the given settings
        """
        return Transaction(self, None, force_rollback, **options)

    @property
    def raw_connection(self):
        """
        Retrieve and return raw-connection underneath thread-safe controls
        """
        return self.backend.raw_connection

class Transaction:
    """
    AnySQL Thread-safe Transaction Implementation
    """
    __slots__ = ('connection', 'conn_callback',
        'force_rollback', 'options', 'transaction')

    def __init__(self,
        connection:     Optional[Connection]               = None,
        conn_callback:  Optional[Callable[[], Connection]] = None,
        force_rollback: bool                               = False,
        **options:     Any,
    ):
        assert connection or conn_callback, 'must set connection'
        self.connection     = connection
        self.conn_callback  = conn_callback
        self.force_rollback = force_rollback
        self.options        = options
        self.transaction: Optional[ITransaction] = None

    def __enter__(self) -> Self:
        """
        Start the transaction and return self
        """
        self.start()
        return self

    def __exit__(self, eclass, *_):
        """
        Roll-back transaction on exception or forced-rollback, else commit
        """
        func = self.rollback if eclass or self.force_rollback else self.commit
        if self.transaction:
            func()

    def __call__(self, func: Callable):
        """
        Allow for `@database.transaction()` decorator
        """
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            with self:
                return func(*args, **kwargs)
        return wrapper

    def start(self):
        """
        Start transaction and update internal trackers
        """
        self.connection  = self.connection or self.conn_callback() #type: ignore
        self.transaction = self.connection.backend.transaction()
        with self.connection.tran_lock:
            is_root = len(self.connection.tran_stack) == 0
            self.connection.__enter__()
            self.transaction.start(is_root, **self.options)
            self.connection.tran_stack.append(self)
        return self

    def commit(self):
        """
        Commit current transaction and update internal trackers
        """
        if not self.transaction or not self.connection:
            raise RuntimeError('Transaction closed')
        with self.connection.tran_lock:
            last = self.connection.tran_stack.pop()
            assert last is self, 'invalid transaction stack'
            self.transaction.commit()
            self.connection.__exit__()
            self.transaction = None

    def rollback(self):
        """
        Roll-back current transaction and update internal trackers
        """
        if not self.transaction or not self.connection:
            raise RuntimeError('Transaction closed')
        with self.connection.tran_lock:
            last = self.connection.tran_stack.pop()
            assert last is self, 'invalid transaction stack'
            self.transaction.rollback()
            self.connection.__exit__()
            self.transaction = None

#** Imports **#
from .async_core import AsyncDatabase
