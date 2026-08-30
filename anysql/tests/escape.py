"""
Escape/Mogrify UnitTests for AnySQL
"""
from datetime import date, datetime
from enum import Enum
from typing import Any
from unittest import TestCase

from ..escape import ProgrammingError, ArgMode, escape_arg, is_placeholder, mogrify, prepare

#** Variables **#
__all__ = ['EscapeTests']

#** Classes **#

class EscapeTests(TestCase):
    """
    Escape/Mogrify UnitTests
    """

    def assertEscape(self, arg: Any, expect: str):
        self.assertEqual(escape_arg(arg), expect)

    def assertPrepare(self, stmt: str, result: str, mode: ArgMode):
        prepped = prepare(stmt)
        self.assertEqual(prepped, result)
        self.assertEqual(prepped.mode, mode)

    def assertMogrify(self, sql: str, result: str, args: Any):
        self.assertEqual(mogrify(sql, args, cache=False), result)

    def test_escape(self):
        """
        ensure value escape works as intended
        """
        now = datetime.now()
        today = date.today()
        self.assertEscape(1, '1')
        self.assertEscape(1.23, '1.23e0')
        self.assertEscape(None, 'NULL')
        self.assertEscape(True, '1')
        self.assertEscape(False, '0')
        self.assertEscape(b'ayylmao', "'ayylmao'")
        self.assertEscape(now, repr(now.isoformat(' ')))
        self.assertEscape(today, repr(today.isoformat()))
        self.assertEscape([1, 'a', 1.23], "(1,'a',1.23e0)")
        self.assertEscape({'a':1,'b':'b','c':1.23}, "{'a': '1', 'b': \"'b'\", 'c': '1.23e0'}")

    def test_escape_enum(self):
        """
        ensure escaping enum works as intended
        """
        class Foo(int, Enum):
            A = 1
        class Bar(str, Enum):
            B = 'b'
        class Baz(float, Enum):
            C = 1.23
        self.assertEscape(Foo.A, '1')
        self.assertEscape(Bar.B, "'b'")
        self.assertEscape(Baz.C, '1.23e0')

    def test_escape_subclass(self):
        """
        ensure escaping works on subclasses of base
        """
        class Quote(str): pass
        class Digit(int): pass
        self.assertEscape(Quote('ayylmao'), "'ayylmao'")
        self.assertEscape(Digit(12345), "12345")

    def test_placeholder(self):
        """
        ensure placeholder replacement works as intended
        """
        self.assertEqual(is_placeholder(':ayy'), 'ayy')
        self.assertEqual(is_placeholder('%(lmao)s'), 'lmao')
        self.assertEqual(is_placeholder('{what}'), 'what')
        self.assertEqual(is_placeholder('{is-up}'), 'is-up')
        self.assertEqual(is_placeholder('%s'), '')
        self.assertEqual(is_placeholder('?'), '')

        self.assertEqual(is_placeholder('a:yy'), None)
        self.assertEqual(is_placeholder(':ayy:'), None)
        self.assertEqual(is_placeholder('(lmao)s'), None)
        self.assertEqual(is_placeholder('%(lmao)'), None)
        self.assertEqual(is_placeholder('{what'), None)
        self.assertEqual(is_placeholder('what}'), None)
        self.assertEqual(is_placeholder('{{what}}'), None)
        self.assertEqual(is_placeholder('%%s'), None)
        self.assertEqual(is_placeholder('%ss'), None)
        self.assertEqual(is_placeholder('??'), None)

    def test_prepare(self):
        """
        ensure sql statement preparation works as intended
        """
        self.assertPrepare(
            'SELECT 1 FROM Foo WHERE Foo=:ayy',
            'SELECT 1 FROM Foo WHERE Foo={ayy}',
            ArgMode.KWARGS,
        )
        self.assertPrepare(
            'UPDATE Bar SET A=%(a)s,B=:b WHERE C={c}',
            'UPDATE Bar SET A={a},B={b} WHERE C={c}',
            ArgMode.KWARGS
        )
        self.assertPrepare(
            'UPDATE Baz SET A=?,B=%s WHERE C=?',
            'UPDATE Baz SET A={0},B={1} WHERE C={2}',
            ArgMode.ARGS
        )
        self.assertPrepare(
            'INSERT INTO Foo VALUES (?, ?, ?, ?)',
            'INSERT INTO Foo VALUES ({0}, {1}, {2}, {3})',
            ArgMode.ARGS
        )
        self.assertPrepare(
            'UPDATE Baz SET A="w?",B=\'%ss\'',
            'UPDATE Baz SET A="w?",B=\'%ss\'',
            ArgMode.NONE,
        )
        with self.assertRaises(ProgrammingError):
            prepare('UPDATE Foo SET A=?,B={b}')

    def test_mogrify(self):
        """
        ensure mogrification works as intended
        """
        self.assertMogrify(
            'SELECT 1 FROM Foo WHERE Foo=:ayy',
            'SELECT 1 FROM Foo WHERE Foo=1',
            {'ayy': 1}
        )
        self.assertMogrify(
            'UPDATE Bar SET A=%(a)s,B=:b WHERE C={c}',
            "UPDATE Bar SET A=1,B='b' WHERE C=1.23e0",
            {'a': 1, 'b': 'b', 'c': 1.23}
        )
        self.assertMogrify(
            'UPDATE Baz SET A=?,B=%s WHERE C=?',
            "UPDATE Baz SET A=1,B='b' WHERE C=NULL",
            (1, 'b', None)
        )
        self.assertMogrify(
            'INSERT INTO Foo VALUES (?, ?, ?, ?)',
            "INSERT INTO Foo VALUES (1, 'Billy', 'Bob', NULL)",
            (1, 'Billy', 'Bob', None)
        )
        with self.assertRaises(ProgrammingError):
            mogrify('SELECT 1 FROM Foo WHERE A=? AND B=%s', {'a': 1, 'b': 2})
        with self.assertRaises(ProgrammingError):
            mogrify('SELECT 1 FROM Foo WHERE A=:a AND B=:b', (1, 2))
        with self.assertRaises(KeyError):
            mogrify('SELECT 1 FROM Foo WHERE A=:a AND B=:b', {'a': 1})
        with self.assertRaises(IndexError):
            mogrify('SELECT 1 FROM Foo WHERE A=? AND B=?', (1, ))
