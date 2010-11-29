import sys
import unittest
import uuid

from nose import SkipTest
from nose.tools import assert_raises
from pycassa import ConnectionPool, ColumnFamily, ConsistencyLevel, NotFoundException
import pycassa.batch as batch_mod

ROWS = {'1': {'a': '123', 'b':'123'},
        '2': {'a': '234', 'b':'234'},
        '3': {'a': '345', 'b':'345'}}

class TestMutator(unittest.TestCase):

    def setUp(self):
        credentials = {'username': 'jsmith', 'password': 'havebadpass'}
        self.pool = ConnectionPool(keyspace='Keyspace1', credentials=credentials)
        self.cf = ColumnFamily(self.pool, 'Standard1')
        self.scf = ColumnFamily(self.pool, 'Super1')

    def tearDown(self):
        self.cf.truncate()
        self.scf.truncate()
        self.pool.dispose()

    def test_insert(self):
        batch = self.cf.batch()
        for key, cols in ROWS.iteritems():
            batch.insert(key, cols)
        batch.send()
        for key, cols in ROWS.items():
            assert self.cf.get(key) == cols

    def test_insert_supercolumns(self):
        batch = self.scf.batch()
        batch.insert('one', ROWS)
        batch.insert('two', ROWS)
        batch.insert('three', ROWS)
        batch.send()
        assert self.scf.get('one') == ROWS
        assert self.scf.get('two') == ROWS
        assert self.scf.get('three') == ROWS

    def test_queue_size(self):
        batch = self.cf.batch(queue_size=2)
        batch.insert('1', ROWS['1'])
        batch.insert('2', ROWS['2'])
        batch.insert('3', ROWS['3'])
        assert self.cf.get('1') == ROWS['1']
        assert_raises(NotFoundException, self.cf.get, '3')
        batch.send()
        for key, cols in ROWS.items():
            assert self.cf.get(key) == cols

    def test_remove_key(self):
        batch = self.cf.batch()
        batch.insert('1', ROWS['1'])
        batch.remove('1')
        batch.send()
        assert_raises(NotFoundException, self.cf.get, '1')

    def test_remove_columns(self):
        batch = self.cf.batch()
        batch.insert('1', {'a':'123', 'b':'123'})
        batch.remove('1', ['a'])
        batch.send()
        assert self.cf.get('1') == {'b':'123'}

    def test_remove_supercolumns(self):
        batch = self.scf.batch()
        batch.insert('one', ROWS)
        batch.insert('two', ROWS)
        batch.insert('three', ROWS)
        batch.remove('two', ['b'], '2')
        batch.send()
        assert self.scf.get('one') == ROWS
        assert self.scf.get('two')['2'] == {'a': '234'}
        assert self.scf.get('three') == ROWS

    def test_chained(self):
        batch = self.cf.batch()
        batch.insert('1', ROWS['1']).insert('2', ROWS['2']).insert('3', ROWS['3']).send()
        assert self.cf.get('1') == ROWS['1']
        assert self.cf.get('2') == ROWS['2']
        assert self.cf.get('3') == ROWS['3']

    def test_contextmgr(self):
        if sys.version_info < (2,5):
            raise SkipTest("No context managers in Python < 2.5")
        exec """with self.cf.batch(queue_size=2) as b:
    b.insert('1', ROWS['1'])
    b.insert('2', ROWS['2'])
    b.insert('3', ROWS['3'])
assert self.cf.get('3') == ROWS['3']"""

    def test_multi_column_family(self):
        batch = batch_mod.Mutator(self.pool)
        cf2 = self.cf
        batch.insert(self.cf, '1', ROWS['1'])
        batch.insert(self.cf, '2', ROWS['2'])
        batch.remove(cf2, '1', ROWS['1'])
        batch.send()
        assert self.cf.get('2') == ROWS['2']
        assert_raises(NotFoundException, self.cf.get, '1')
