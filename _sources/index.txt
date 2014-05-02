pycassa |release| Documentation
===============================
pycassa is a Thrift-based Python client for
`Apache Cassandra <http://cassandra.apache.org>`_.

pycassa does not support CQL or Cassandra's native protocol, which are a
replacement for the Thrift interface that pycassa is based on. If you are
starting a new project, *it is highly recommended that you use the newer*
`DataStax python driver <https://github.com/datastax/python-driver>`_ instead
of pycassa.

pycassa is open source under the `MIT license <http://www.opensource.org/licenses/mit-license.php>`_.
The source code repository for pycassa can be found on `Github <http://github.com/pycassa/pycassa>`_.

Contents
--------
:doc:`installation`
  How to install pycassa.

:doc:`tutorial`
  A short overview of pycassa usage.

:doc:`api/index`
  The pycassa API documentation.

:doc:`Assorted Functionality <assorted/index>`
  How to work with various Cassandra and pycassa features.

:doc:`using_with/index`
  How to use pycassa with other projects, including eventlet and Celery.

:doc:`changelog`
    The changelog for every version of pycassa.

:doc:`development`
    Notes for developing pycassa itself.

Help
------------
Mailing Lists
  * User list: mail to `pycassa-discuss@googlegroups.com <mailto:pycassa-discuss@googlegroups.com>`_ or `view online <http://groups.google.com/group/pycassa-discuss>`_.
  * Developer list: mail to `pycassa-devel@googlegroups.com <mailto:pycassa-devel@googlegroups.com>`_ or `view online <http://groups.google.com/group/pycassa-devel>`_.

IRC
  * Use #cassandra on `irc.freenode.net <http://freenode.net>`_.  If you don't have an IRC client, you can use `freenode's web based client <http://webchat.freenode.net/?channels=#cassandra>`_.

Issues
------
Bugs and feature requests for pycassa are currently tracked through the `github issue tracker <http://github.com/pycassa/pycassa/issues>`_.

Contributing
------------
You are encouraged to offer any contributions or ideas you have.
Contributing to the documentation or examples, reporting bugs, requesting
features, and (of course) improving the code are all equally welcome.
To contribute, fork the project on
`github <http://github.com/pycassa/pycassa/>`_ and make a 
`pull request <http://help.github.com/send-pull-requests/>`_.


About This Documentation
------------------------
This documentation is generated using the `Sphinx
<http://sphinx.pocoo.org/>`_ documentation generator. The source files
for the documentation are located in the *doc/* directory of 
pycassa. To generate the documentation, run the
following command from the root directory of pycassa:

.. code-block:: bash

  $ python setup.py doc

.. toctree::
   :hidden:

   installation
   tutorial
   example/index
   api/index
   changelog
   assorted/index
   using_with/index
   development
