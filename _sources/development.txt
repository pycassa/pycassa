Development
===========

New thrift API
--------------

pycassa includes Cassandra's Python Thrift API in `pycassa.cassandra`.
Since Cassandra 1.1.0, the generated Thrift definitions are fully backwards
compatible, allowing you to use attributes that have been deprecated or
removed in recent versions of Cassandra. So, even though the code is
generated from a Cassandra 1.1.0 definition, you can use the resulting code
with 0.7 and still have full access to attributes that were removed after
0.7, such as the memtable flush thresholds.

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

These should replace the python files in `pycassa/cassandra`, allowing you
to use the latest Thrift methods and object definitions, such as CfDef (which
controls what attributes you may set when creating or updating a column
family). Don't forget to review the documentation.

Make sure you run the tests, especially if adjusting the default protocol
version or introducing backwards incompatible API changes.

References
----------

* http://thrift.apache.org/docs/install/
* http://wiki.apache.org/cassandra/HowToContribute
* http://wiki.apache.org/cassandra/InstallThrift
