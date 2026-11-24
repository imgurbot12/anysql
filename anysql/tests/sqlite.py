"""
Sqlite Core and Backend UnitTests for AnySQL
"""
from datetime import datetime, timedelta
from unittest import IsolatedAsyncioTestCase, TestCase

from .. import AsyncDatabase, Database

#** Variables **#
__all__ = ['SqliteTests', 'AsyncSqliteTests']

DATABASE = """
CREATE TABLE IF NOT EXISTS People (
    PersonId  INT,
    Firstname VARCHAR(100),
    Lastname  VARCHAR(100),
    DoB       DATETIME
);
"""

#** Classes **#

class SqliteTests(TestCase):
    """
    """
    db: Database

    def setUp(self) -> None:
        self.db = Database('sqlite://:memory:')
        self.db.connect()
        self.db.execute(DATABASE)

    def tearDown(self) -> None:
        self.db.disconnect()

    def test_basics(self):
        """
        Ensure basic CRUD works as intended
        """
        dob   = datetime.now() - timedelta(days=365 * 50)
        billy = (1, 'Billy', 'Smith', dob.isoformat(' '))

        rec = self.db.fetch_all('SELECT * FROM People')
        self.assertListEqual(rec, [])

        self.db.execute('INSERT INTO People VALUES (?, ?, ?, ?)', billy)
        rec = self.db.fetch_all('SELECT * FROM People')
        self.assertListEqual(rec, [billy])

        c1 = self.db.execute('UPDATE People SET PersonId=2 WHERE PersonId=1')
        c2 = self.db.execute('UPDATE People SET PersonId=2 WHERE PersonId=1')
        self.assertEqual(c1, 1)
        self.assertEqual(c2, 0)
        rec = self.db.fetch_all('SELECT * FROM People')
        self.assertListEqual(rec, [(2, *billy[1:])])

        self.db.execute('DELETE FROM People');
        rec = self.db.fetch_all('SELECT * FROM People')
        self.assertListEqual(rec, [])

    def test_tx(self):
        """
        Ensure transaction behavior works as intended
        """
        dob = datetime.now() - timedelta(days=365 * 50)
        bob = (1, 'Bob', 'Jones', dob.isoformat(' '))
        with self.db.transaction():
            self.db.execute('INSERT INTO People VALUES (?, ?, ?, ?)', bob)
        rec = self.db.fetch_all('SELECT * FROM People')
        self.assertListEqual(rec, [bob])

        self.db.execute('DELETE FROM People');
        with self.db.transaction() as tr:
            self.db.execute('INSERT INTO People VALUES (?, ?, ?, ?)', bob)
            tr.rollback()
        rec = self.db.fetch_all('SELECT * FROM People')
        self.assertListEqual(rec, [])

        with self.db.transaction(force_rollback=True) as tr:
            self.db.execute('INSERT INTO People VALUES (?, ?, ?, ?)', bob)
        rec = self.db.fetch_all('SELECT * FROM People')
        self.assertListEqual(rec, [])

        with self.db.transaction(force_rollback=True) as tr:
            self.db.execute('INSERT INTO People VALUES (?, ?, ?, ?)', bob)
            tr.commit()
        rec = self.db.fetch_all('SELECT * FROM People')
        self.assertListEqual(rec, [bob])

class AsyncSqliteTests(IsolatedAsyncioTestCase):
    """
    """
    db: AsyncDatabase

    async def asyncSetUp(self) -> None:
        self.db = AsyncDatabase('sqlite://:memory:')
        await self.db.connect()
        await self.db.execute(DATABASE)

    async def asyncTearDown(self) -> None:
        await self.db.disconnect()

    async def test_basics(self):
        """
        Ensure basic CRUD works as intended
        """
        dob   = datetime.now() - timedelta(days=365 * 50)
        billy = (1, 'Billy', 'Smith', dob.isoformat(' '))

        rec = await self.db.fetch_all('SELECT * FROM People')
        self.assertListEqual(rec, [])

        await self.db.execute('INSERT INTO People VALUES (?, ?, ?, ?)', billy)
        rec = await self.db.fetch_all('SELECT * FROM People')
        self.assertListEqual(rec, [billy])

        c1 = await self.db.execute('UPDATE People SET PersonId=2 WHERE PersonId=1')
        c2 = await self.db.execute('UPDATE People SET PersonId=2 WHERE PersonId=1')
        self.assertEqual(c1, 1)
        self.assertEqual(c2, 0)
        rec = await self.db.fetch_all('SELECT * FROM People')
        self.assertListEqual(rec, [(2, *billy[1:])])

        await self.db.execute('DELETE FROM People');
        rec = await self.db.fetch_all('SELECT * FROM People')
        self.assertListEqual(rec, [])

    async def test_tx(self):
        """
        Ensure transaction behavior works as intended
        """
        dob = datetime.now() - timedelta(days=365 * 50)
        bob = (1, 'Bob', 'Jones', dob.isoformat(' '))
        def func1(conn, _):
            conn.execute('INSERT INTO People VALUES (?, ?, ?, ?)', bob)
        await self.db.transaction(func1)
        rec = await self.db.fetch_all('SELECT * FROM People')
        self.assertListEqual(rec, [bob])

        await self.db.execute('DELETE FROM People');
        def func2(conn, tx):
            conn.execute('INSERT INTO People VALUES (?, ?, ?, ?)', bob)
            tx.rollback()
        await self.db.transaction(func2)
        rec = await self.db.fetch_all('SELECT * FROM People')
        self.assertListEqual(rec, [])

        def func3(conn, _):
            conn.execute('INSERT INTO People VALUES (?, ?, ?, ?)', bob)
        await self.db.transaction(func3, force_rollback=True)
        rec = await self.db.fetch_all('SELECT * FROM People')
        self.assertListEqual(rec, [])

        def func4(conn, tx):
            conn.execute('INSERT INTO People VALUES (?, ?, ?, ?)', bob)
            tx.commit()
        await self.db.transaction(func4, force_rollback=True)
        rec = await self.db.fetch_all('SELECT * FROM People')
        self.assertListEqual(rec, [bob])
