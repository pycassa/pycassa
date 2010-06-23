import threading

from pycassa import connect, connect_thread_local

def version_check(connection, version):
    assert connection.describe_version() == version

def test_connections():
    version = connect('Keyspace1').describe_version()

    thread_local = connect_thread_local('Keyspace1')
    threads = []
    for i in xrange(10):
        threads.append(threading.Thread(target=version_check,
                                        args=(thread_local, version)))
        threads[-1].start()
    for thread in threads:
        thread.join()
