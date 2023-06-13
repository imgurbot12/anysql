"""
Abstract SQL Interface Implementations
"""
import enum
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
Args = Union[Mapping[str, Any], Sequence[Any], None]

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
    def __getitem__(self, key: str) -> Any:
        raise NotImplementedError

class ITransaction(Protocol):

    @abstractmethod
    def start(self, is_root: bool, **options):
        raise NotImplementedError

    @abstractmethod
    def commit(self):
        raise NotImplementedError

    @abstractmethod
    def rollback(self):
        raise NotImplementedError

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
