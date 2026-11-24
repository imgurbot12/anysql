"""
AnySQL Async Compatibility Layer
"""
import asyncio
import functools
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, List, Optional, Sequence, TypeVar, Union, cast, overload
from typing_extensions import AsyncGenerator, ParamSpec

from .uri import DatabaseURI
from .core import Connection, Database, Transaction
from .interface import Args, Record, Stmt

#** Variables **#
__all__ = ['AsyncDatabase']

P = ParamSpec('P')
R = TypeVar('R')

#** Classes **#

class AsyncDatabase:
    """
    AnySQL Async Thread-Safe Database Implementation
    """
    __slots__ = ('db', 'pool', '_loop')

    @overload
    def __init__(self, uri_or_db: str, cache_statements: bool = True, **kwargs):
        ...

    @overload
    def __init__(self, uri_or_db: Database):
        ...

    def __init__(self, uri_or_db: Union[str, Database],
        cache_statements: bool = True, **kwargs):
        """
        """
        self.db = uri_or_db \
            if isinstance(uri_or_db, Database) else \
            Database(uri_or_db, cache_statements, **kwargs)
        self.pool = ThreadPoolExecutor(thread_name_prefix='anysql_')
        self._loop = asyncio._get_running_loop()

    @property
    def uri(self) -> DatabaseURI:
        return self.db.uri

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        if self._loop is None:
            self._loop = asyncio.get_event_loop()
        return self._loop

    async def _run_thread(self,
        func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> R:
        """
        """
        task = functools.partial(func, *args, **kwargs)
        return await self.loop.run_in_executor(self.pool, task)

    def to_sync(self) -> Database:
        """
        Unwrap the Database instance back its sync version
        """
        return self.db

    async def connect(self):
        """
        Connect to database if not already connected
        """
        await self._run_thread(self.db.connect)

    async def disconnect(self):
        """
        Disconnect from database if not already disconnected
        """
        await self._run_thread(self.db.disconnect)

    async def fetch_one(self,
        query: Stmt, values: Args = None) -> Optional[Record]:
        """
        Fetch a single record matching the given SQL statement
        """
        return await self._run_thread(self.db.fetch_one, query, values)

    async def fetch_all(self, query: Stmt, values: Args = None) -> List[Record]:
        """
        Fetch all the records matching the given SQL statement
        """
        return await self._run_thread(self.db.fetch_all, query, values)

    async def fetch_yield(self,
        query: Stmt, values: Args = None) -> AsyncGenerator[Record, None]:
        """
        Fetch generator of records matching the given SQL statement
        """
        queue = asyncio.Queue()
        def task():
            for record in self.db.fetch_yield(query, values):
                self.loop.call_soon_threadsafe(queue.put_nowait, record)
            self.loop.call_soon_threadsafe(queue.put_nowait, None)
        self.loop.run_in_executor(self.pool, task)
        while True:
            item = await queue.get()
            if item is None:
                break
            yield item

    async def execute(self, query: Stmt, values: Args = None) -> int:
        """
        Execute the SQL statement once with the given values
        """
        return await self._run_thread(self.db.execute, query, values)

    async def execute_many(self, query: Stmt, values: Sequence[Args]):
        """
        Execute the SQL statement with multiple sets of values
        """
        return await self._run_thread(self.db.execute_many, query, values)

    async def transaction(self,
        func: Callable[[Connection, Transaction], R],
        force_rollback: bool = False,
        **options
    ) -> R:
        """
        Generate a transaction and run the specified function to process it
        """
        def tx_task() -> R:
            with self.db.transaction(force_rollback, **options) as tx:
                return func(cast(Connection, tx.connection), tx)
        return await self._run_thread(tx_task)



