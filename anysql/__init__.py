"""
SQL Flavor-Agnostic Synchronous Client Implementation
"""

#** Variables **#
__all__ = ['AsyncDatabase', 'Database', 'DatabaseURI']

#** Imports **#
from .uri import DatabaseURI
from .core import Database
from .async_core import AsyncDatabase
