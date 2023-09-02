"""
Abstract SQL Interface Implementations
"""
import enum
import uuid
from abc import abstractmethod
from collections.abc import Sequence
from typing import Union, Mapping, Protocol, Any, Optional, List
from typing_extensions import runtime_checkable

from .uri import DatabaseURI

#** Variables **#
__all__ = [
    'Args',
    'Stmt',
    'ProgrammingError',
    'ArgMode',
    'Prepared',
    'Query',
    'Record',
    'ITransaction',
    'IConnection',
    'IDatabase'
]

#: typehint for valid arguments type
Args = Union[Mapping[str, Any], Sequence, None]

#: typehint for all valid mogrify query types
Stmt = Union[str, 'Prepared', 'Query']

#** Classes **#

class ProgrammingError(Exception):
    """Exception to Raise on Invalid SQL-Query"""
    pass

class ArgMode(enum.Enum):
    """Enum to Denote the Type of Argument Implementation"""
    NONE   = 0
    ARGS   = 1
    KWARGS = 2

class Prepared(str):
    """Custom StringType to Denote Prepared/Parsed SQL Expression"""
    mode: ArgMode = ArgMode.NONE

class Query(Prepared):
    """Custom StringType to Denote Complete QueryString w/o Placeholders"""
    pass

class Record(Sequence):
 
    @abstractmethod
    def __iter__(self):
        raise NotImplementedError

    @abstractmethod
    def __getitem__(self, key: str) -> Any:
        raise NotImplementedError

class ITransaction(Protocol):
    is_root:   bool
    savepoint: Optional[str]
 
    @abstractmethod
    def _execute(self, query: str):
        raise NotImplementedError

    def start(self, is_root: bool, **options):
        """internal handler for supporting transaction-start"""
        self.is_root = is_root
        if self.is_root:
            self._execute('BEGIN')
            return
        self.savepoint = f'SP_{str(uuid.uuid4()).replace("-", "_")}'
        self._execute(f'SAVEPOINT {self.savepoint}')

    def commit(self):
        """
        commit changes made in transaction
        """
        if self.is_root:
            self._execute('COMMIT')
            return
        self._execute(f'RELEASE SAVEPOINT {self.savepoint}')
 
    def rollback(self):
        """
        rollback any changes made during the transaction
        """
        if self.is_root:
            self._execute('ROLLBACK')
            return
        self._execute(f'ROLLBACK TO SAVEPOINT {self.savepoint}')

class IConnection(Protocol):

    @abstractmethod
    def acquire(self):
        raise NotImplementedError

    @abstractmethod
    def release(self):
        raise NotImplementedError

    @abstractmethod
    def fetch_one(self, query: Query) -> Optional[Record]:
        raise NotImplementedError

    @abstractmethod
    def fetch_all(self, query: Query) -> List[Record]:
        raise NotImplementedError

    @abstractmethod
    def execute(self, query: Query):
        raise NotImplementedError

    @abstractmethod
    def transaction(self) -> ITransaction:
        raise NotImplementedError

    @property
    @abstractmethod
    def raw_connection(self):
        raise NotImplementedError

@runtime_checkable
class IDatabase(Protocol):
 
    @abstractmethod
    def __init__(self, uri: DatabaseURI, **kwargs: Any):
        raise NotImplementedError

    @abstractmethod
    def connect(self):
        raise NotImplementedError

    @abstractmethod
    def disconnect(self):
        raise NotImplementedError

    @abstractmethod
    def connection(self) -> IConnection:
        raise NotImplementedError
