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

def setup_module():
    global pool, cf, scf
    credentials = {'username': 'jsmith', 'password': 'havebadpass'}
    pool = ConnectionPool(keyspace='PycassaTestKeyspace', credentials=credentials)
    cf = ColumnFamily(pool, 'Standard1')
    scf = ColumnFamily(pool, 'Super1')

def teardown_module():
    pool.dispose()

class TestMutator(unittest.TestCase):

    def tearDown(self):
        cf.truncate()
        scf.truncate()

    def test_insert(self):
        batch = cf.batch()
        for key, cols in ROWS.iteritems():
            batch.insert(key, cols)
        batch.send()
        for key, cols in ROWS.items():
            assert cf.get(key) == cols

    def test_insert_supercolumns(self):
        batch = scf.batch()
        batch.insert('one', ROWS)
        batch.insert('two', ROWS)
        batch.insert('three', ROWS)
        batch.send()
        assert scf.get('one') == ROWS
        assert scf.get('two') == ROWS
        assert scf.get('three') == ROWS

    def test_queue_size(self):
        batch = cf.batch(queue_size=2)
        batch.insert('1', ROWS['1'])
        batch.insert('2', ROWS['2'])
        batch.insert('3', ROWS['3'])
        assert cf.get('1') == ROWS['1']
        assert_raises(NotFoundException, cf.get, '3')
        batch.send()
        for key, cols in ROWS.items():
            assert cf.get(key) == cols

    def test_remove_key(self):
        batch = cf.batch()
        batch.insert('1', ROWS['1'])
        batch.remove('1')
        batch.send()
        assert_raises(NotFoundException, cf.get, '1')

    def test_remove_columns(self):
        batch = cf.batch()
        batch.insert('1', {'a':'123', 'b':'123'})
        batch.remove('1', ['a'])
        batch.send()
        assert cf.get('1') == {'b':'123'}

    def test_remove_supercolumns(self):
        batch = scf.batch()
        batch.insert('one', ROWS)
        batch.insert('two', ROWS)
        batch.insert('three', ROWS)
        batch.remove('two', ['b'], '2')
        batch.send()
        assert scf.get('one') == ROWS
        assert scf.get('two')['2'] == {'a': '234'}
        assert scf.get('three') == ROWS

    def test_chained(self):
        batch = cf.batch()
        batch.insert('1', ROWS['1']).insert('2', ROWS['2']).insert('3', ROWS['3']).send()
        assert cf.get('1') == ROWS['1']
        assert cf.get('2') == ROWS['2']
        assert cf.get('3') == ROWS['3']

    def test_contextmgr(self):
        if sys.version_info < (2,5):
            raise SkipTest("No context managers in Python < 2.5")
        exec """with cf.batch(queue_size=2) as b:
    b.insert('1', ROWS['1'])
    b.insert('2', ROWS['2'])
    b.insert('3', ROWS['3'])
assert cf.get('3') == ROWS['3']"""

    def test_multi_column_family(self):
        batch = batch_mod.Mutator(pool)
        cf2 = cf
        batch.insert(cf, '1', ROWS['1'])
        batch.insert(cf, '2', ROWS['2'])
        batch.remove(cf2, '1', ROWS['1'])
        batch.send()
        assert cf.get('2') == ROWS['2']
        assert_raises(NotFoundException, cf.get, '1')
