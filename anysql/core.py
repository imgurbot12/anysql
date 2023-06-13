"""
AnySQL Core Implementation
"""
import functools
import threading
import importlib
import contextvars
from typing import Any, List, Sequence, Optional, Callable
from typing_extensions import Self

from .uri import DatabaseURI
from .escape import mogrify, prepare
from .interface import *

#** Variables **#
__all__ = [
    'BACKENDS',

    'Database', 
    'Connection', 
    'Transaction'
]

#: supported backends registry
BACKENDS = {
    'mysql':  ('anysql.backends.pymysql', 'MysqlDatabase'),
    'sqlite': ('anysql.backends.sqlite', 'SqliteDatabase'),
}

#** Functions **#

def get_backend(uri: DatabaseURI, kwargs: dict) -> IDatabase:
    """
    generate and retrieve backend w/ the specified uri

    :param uri:    uri used to retrieve relevant backend and pass to idatabase
    :param kwargs: kwargs to pass to idatabase initialization
    :return:       generated idatabase instance
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
    """AnySQL Threadsafe Database Implementation"""

    def __init__(self, uri: str, cache_statements: bool = True, **kwargs):
        self.uri              = DatabaseURI(uri)
        self.backend          = get_backend(self.uri, kwargs)
        self.connected        = False
        self.context          = contextvars.ContextVar("connection_context")
        self.cache_statements = cache_statements
 
    def __enter__(self) -> Self:
        """auto-connect on start of context"""
        self.connect()
        return self
 
    def __exit__(self, *_):
        """auto-disconnect at end of context"""
        self.disconnect()

    def connect(self):
        """
        connect to database if not already connected
        """
        if self.connected:
            return
        self.backend.connect()

    def disconnect(self):
        """
        disconnect from database if not already disconected
        """
        if not self.connected:
            return
        self.backend.disconnect()

    def connection(self) -> 'Connection':
        """
        spawn/return connection based on local thread context

        :return: database connection object
        """
        try:
            return self.context.get()
        except LookupError:
            connection = Connection(self.backend.connection())
            self.context.set(connection)
            return connection
 
    def fetch_one(self, query: Stmt, values: Args = None) -> Optional[Record]:
        """
        fetch a single record matchingt the given sql statement

        :param sql:    raw paramd sql statement
        :param values: values to pass into sql statement
        :return:       single sql row record (if any)
        """
        with self.connection() as conn:
            return conn.fetch_one(query, values)

    def fetch_all(self, query: Stmt, values: Args = None) -> List[Record]:
        """
        fetch all the records matching the given sql statement

        :param sql:    raw paramd sql statement
        :param values: values to pass into sql statement
        :return:       sql rows in a list
        """
        with self.connection() as conn:
            return conn.fetch_all(query, values)

    def execute(self, query: Stmt, values: Args = None):
        """
        execute the sql statement once with the given values

        :param sql:    raw paramd sql statement
        :param values: values to pass into sql statement
        """
        with self.connection() as conn:
            return conn.execute(query, values)

    def execute_many(self, query: Stmt, values: Sequence[Args]):
        """
        execute the sql statement with multiple sets of values

        :param sql:    raw paramd sql statement
        :parma values: list of values to pass into the statement
        """
        with self.connection() as conn:
            return conn.execute_many(query, values)

    def transaction(self, force_rollback: bool = False, **options) -> 'Transaction':
        """
        generate transaction and return controls w/ the given settings

        :param force_rollback: force-rollback unless explicitly committed
        :param options:        additional settings to pass to transaction
        :return:               transaction manager instance
        """
        return Transaction(None, self.connection, force_rollback, **options)

class Connection:
    """AnySQL Threadsafe Connection Implementation"""

    def __init__(self, backend: IConnection, cache_statements: bool = True):
        self.backend    = backend
        self.conn_lock  = threading.Lock()
        self.conn_count = 0
        self.tran_lock  = threading.Lock()
        self.tran_stack: List['Transaction'] = []
        # self.query_lock = threading.Lock()
        self.cache_statements = cache_statements

    def __enter__(self) -> Self:
        """track number of connections and acquire conn when one"""
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
        """track number of connections and release conn when zero"""
        with self.conn_lock:
            self.conn_count -= 1
            if self.conn_count == 0:
                self.backend.release()

    def fetch_one(self, query: Stmt, values: Args = None) -> Optional[Record]:
        """
        fetch a single record matchingt the given sql statement

        :param sql:    raw paramd sql statement
        :param values: values to pass into sql statement
        :return:       single sql row record (if any)
        """
        query = mogrify(query, values, self.cache_statements)
        return self.backend.fetch_one(query)

    def fetch_all(self, query: Stmt, values: Args = None) -> List[Record]:
        """
        fetch all the records matching the given sql statement

        :param sql:    raw paramd sql statement
        :param values: values to pass into sql statement
        :return:       sql rows in a list
        """
        query = mogrify(query, values, self.cache_statements)
        return self.backend.fetch_all(query)

    def execute(self, query: Stmt, values: Args = None):
        """
        execute the sql statement once with the given values

        :param sql:    raw paramd sql statement
        :param values: values to pass into sql statement
        """
        query = mogrify(query, values, self.cache_statements)
        return self.backend.execute(query)

    def execute_many(self, query: Stmt, values: Sequence[Args]):
        """
        execute the sql statement with multiple sets of values

        :param sql:    raw paramd sql statement
        :parma values: list of values to pass into the statement
        """
        query = prepare(query)
        for vset in values:
            stmt = mogrify(query, vset, self.cache_statements)
            self.backend.execute(stmt)

    def transaction(self, force_rollback: bool = False, **options) -> 'Transaction':
        """
        generate transaction and return controls w/ the given settings

        :param force_rollback: force-rollback unless explicitly committed
        :param options:        additional settings to pass to transaction
        :return:               transaction manager instance
        """
        return Transaction(self, None, force_rollback, **options)

    @property
    def raw_connection(self):
        """
        retrieve and return raw-connection underneath thread-safe controls
        """
        return self.backend.raw_connection

class Transaction:
    """AnySQL Threadsafe Transaction Implementation"""

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
        """start the transaction and return self"""
        self.start()
        return self

    def __exit__(self, eclass, *_):
        """rollback transaction on exception or forced-rollback, else commit"""
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
        start transaction and update internal trackers
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
        commit current transaction and update internal trackers
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
        rollback current transaction and update internal trackers
        """
        if not self.transaction or not self.connection:
            raise RuntimeError('Transaction closed')
        with self.connection.tran_lock:
            last = self.connection.tran_stack.pop()
            assert last is self, 'invalid transaction stack'
            self.transaction.rollback()
            self.connection.__exit__()
            self.transaction = None
