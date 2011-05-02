from pycassa.system_manager import *

TEST_KS = 'PycassaTestKeyspace'

def setup_package():
    sys = SystemManager()
    if TEST_KS in sys.list_keyspaces():
        sys.drop_keyspace(TEST_KS)
    try:
        sys.create_keyspace(TEST_KS, 1)
        sys.create_column_family(TEST_KS, 'Standard1')
        sys.create_column_family(TEST_KS, 'Super1', super=True)
        sys.create_column_family(TEST_KS, 'Indexed1')
        sys.create_index(TEST_KS, 'Indexed1', 'birthdate', LONG_TYPE)
    except Exception, e:
        try:
            sys.drop_keyspace(TEST_KS)
        except:
            pass
        raise e
    sys.close()

def teardown_package():
    sys = SystemManager()
    if TEST_KS in sys.list_keyspaces():
        sys.drop_keyspace(TEST_KS)
    sys.close()
