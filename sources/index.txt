pycassa |release| Documentation
===============================

Contents
--------
**pycassa** is a Python client for
`Apache Cassandra <http://cassandra.apache.org>`_.


:doc:`installation`
  How to install **pycassa**.

:doc:`tutorial`
  A short overview of **pycassa** usage.

:doc:`example/index`
  An example of how to use **pycassa** with `Twissandra <http://github.com/ericflo/twissandra>`_, an example project that uses Cassandra to provide functionality similar to Twitter.

:doc:`api/index`
  The **pycassa** API documentation.


Help
------------
Mailing Lists
  * User list: mail to `pycassa-discuss@googlegroups.com <mailto:pycassa-discuss@googlegroups.com>`_ or `view online <http://groups.google.com/group/pycassa-discuss>`_.
  * Developer list: mail to `pycassa-devel@googlegroups.com <mailto:pycassa-devel@googlegroups.com>`_ or `view online <http://groups.google.com/group/pycassa-devel>`_.

IRC
  * Use #cassandra on `irc.freenode.net <http://freenode.net>`_.  If you don't have an IRC client, you can use `freenode's web based client <http://webchat.freenode.net/?channels=#cassandra>`_.

Issues
------
Bugs and feature requests for **pycassa** are currently tracked through the `github issue tracker <http://github.com/pycassa/pycassa/issues>`_.

Contributing
------------
**pycassa** encourages you to offer any contributions or ideas you have.
Contributing to the documentation or examples, reporting bugs, requesting
features, and (of course) improving the code are all equally welcome.
To contribute, fork the project on
`github <http://github.com/pycassa/pycassa/>`_ and make a pull request.

Changes
-------
The :doc:`changelog` lists the changes between versions of **pycassa**.

About This Documentation
------------------------
This documentation is generated using the `Sphinx
<http://sphinx.pocoo.org/>`_ documentation generator. The source files
for the documentation are located in the *doc/* directory of 
**pycassa**. To generate the documentation, run the
following command from the root directory of **pycassa**:

.. code-block:: bash

  $ python setup.py doc

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. toctree::
   :hidden:

   installation
   tutorial
   example/index
   api/index
   changelog

