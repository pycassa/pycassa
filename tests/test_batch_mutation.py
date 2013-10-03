from __future__ import with_statement

import sys
import unittest

from nose import SkipTest
from nose.tools import assert_raises, assert_equal
from pycassa import ConnectionPool, ColumnFamily, NotFoundException
import pycassa.batch as batch_mod
from pycassa.system_manager import SystemManager

ROWS = {'1': {'a': '123', 'b': '123'},
        '2': {'a': '234', 'b': '234'},
        '3': {'a': '345', 'b': '345'}}

pool = cf = scf = counter_cf = super_counter_cf = sysman = None

def setup_module():
    global pool, cf, scf, counter_cf, super_counter_cf, sysman
    credentials = {'username': 'jsmith', 'password': 'havebadpass'}
    pool = ConnectionPool(keyspace='PycassaTestKeyspace', credentials=credentials)
    cf = ColumnFamily(pool, 'Standard1')
    scf = ColumnFamily(pool, 'Super1')
    sysman = SystemManager()
    counter_cf = ColumnFamily(pool, 'Counter1')
    super_counter_cf = ColumnFamily(pool, 'SuperCounter1')

def teardown_module():
    pool.dispose()

class TestMutator(unittest.TestCase):

    def tearDown(self):
        for key, cols in cf.get_range():
            cf.remove(key)
        for key, cols, in scf.get_range():
            scf.remove(key)

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

    def test_insert_counters(self):
        batch = counter_cf.batch()
        batch.insert('one', {'col': 1})
        batch.insert('two', {'col': 2})
        batch.insert('three', {'col': 3})
        batch.send()
        assert_equal(counter_cf.get('one'), {'col': 1})
        assert_equal(counter_cf.get('two'), {'col': 2})
        assert_equal(counter_cf.get('three'), {'col': 3})

        batch = super_counter_cf.batch()
        batch.insert('one', {'scol': {'col1': 1, 'col2': 2}})
        batch.insert('two', {'scol': {'col1': 3, 'col2': 4}})
        batch.send()
        assert_equal(super_counter_cf.get('one'), {'scol': {'col1': 1, 'col2': 2}})
        assert_equal(super_counter_cf.get('two'), {'scol': {'col1': 3, 'col2': 4}})

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
        batch.insert('1', {'a': '123', 'b': '123'})
        batch.remove('1', ['a'])
        batch.send()
        assert cf.get('1') == {'b': '123'}

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
        if sys.version_info < (2, 5):
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

    def test_atomic_insert_at_mutator_creation(self):
        batch = cf.batch(atomic=True)
        for key, cols in ROWS.iteritems():
            batch.insert(key, cols)
        batch.send()
        for key, cols in ROWS.items():
            assert cf.get(key) == cols

    def test_atomic_insert_at_send(self):
        batch = cf.batch(atomic=True)
        for key, cols in ROWS.iteritems():
            batch.insert(key, cols)
        batch.send(atomic=True)
        for key, cols in ROWS.items():
            assert cf.get(key) == cols
