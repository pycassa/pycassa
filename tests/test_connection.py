import threading

from pycasso import connect, connect_pooled, connect_thread_local

def version_check(connection, version):
    assert connection.get_string_property('version') == version

def test_connections():
    version = connect().get_string_property('version')

    thread_local = connect_thread_local()
    threads = []
    for i in xrange(10):
        threads.append(threading.Thread(target=version_check,
                                        args=(thread_local, version)))
        threads[-1].start()
    for thread in threads:
        thread.join()


    threads = []
    pooled = connect_pooled()
    for i in xrange(10):
        threads.append(threading.Thread(target=version_check,
                                        args=(pooled, version)))
        threads[-1].start()
    for thread in threads:
        thread.join()
