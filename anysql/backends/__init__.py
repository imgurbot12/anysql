"""
AnySQL Database Backend Interface Implementations
"""
from typing import Any, Optional

from ..uri import DatabaseURI

#** Variables **#
__all__ = ['Timeout', 'setdefault']

#: typehint for timeout specification
Timeout = Optional[float]

#** Functions **#

def setdefault(kwargs: dict, 
    uri: DatabaseURI, key: str, convert: type, default: Any = None):
    """set-default for kwargs from uri options"""
    default = uri.options.get(key) or default
    if default is not None:
        kwargs.setdefault(key, convert(default))
