Development
===========

New thrift API
--------------

pycassa includes Cassandra's Python Thrift API in various sub-packages under
`pycassa.cassandra`. With new versions of Cassandra there's new versions of
the API and new Python files need to be generated.

The following explains the procedure using Mac OS Lion as an example. Other
Linux and BSD versions should work in similar ways. Of course you need to
have a supported Java JDK installed, the Apple provided JDK is fine. This
approach doesn't install any of the tools globally but keeps them isolated.
As such we are using `virtualenv <http://pypi.python.org/pypi/virtualenv>`.

First you need some prerequisites, installed via macports or some other
package management system::

    sudo port install boost libevent

Download and unpack thrift (http://thrift.apache.org/download/)::

    wget http://apache.osuosl.org/thrift/0.8.0/thrift-0.8.0.tar.gz
    tar xzf thrift-0.8.0.tar.gz

Create a virtualenv and tell thrift to install into it::

    cd thrift-0.8.0
    virtualenv-2.7 .
    export PY_PREFIX=$PWD

Configure and build thrift with the minimal functionality we need::

    ./configure --prefix=$PWD --disable-static --with-boost=/opt/local \
        --with-libevent=/opt/local --without-csharp --without-cpp \
        --without-java --without-erlang --without-perl --without-php \
        --without-php_extension --without-ruby --without-haskell --without-go
    make
    make install

You can test the successful install::

    bin/thrift -version
    bin/python -c "from thrift.protocol import fastbinary; print(fastbinary)"

Next up is Cassandra. Clone the Git repository::

    cd ..
    git clone http://git-wip-us.apache.org/repos/asf/cassandra.git
    cd cassandra

We will build the Thrift API for the 1.1.1 release, so checkout the tag
(instructions simplified from build.xml `gen-thrift-py`)::

    git checkout cassandra-1.1.1
    cd interface
    ../../thrift-0.8.0/bin/thrift --gen py:new_style -o thrift/ \
        cassandra.thrift

We are only interested in the generated Python modules::

    ls thrift/gen-py/cassandra/*.py

These should be copied into a new sub-package under `pycassa/cassandra` like
`c11`. There's a couple more places that need to be updated in `setup.py`,
`pycassa/connection.py`, `cassandra/constants.py` and `cassandra/ttypes.py`.
Don't forget to review the documentation either.

Make sure you run the tests, especially if adjusting the default protocol
version or introducing backwards incompatible API changes.

References
----------

* http://thrift.apache.org/docs/install/
* http://wiki.apache.org/cassandra/HowToContribute
* http://wiki.apache.org/cassandra/InstallThrift
