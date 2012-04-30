.. _installing:

Installing
==========

Installing from PyPi
--------------------
If you have :file:`pip` installed, you can simply do:

.. code-block:: bash

  $ pip install pycassa

This will also install the Thrift python bindings automatically.

Manual Installation
-------------------
Make sure that you have Thrift's python bindings installed:

.. code-block:: bash

  $ pip install thrift

You can download a release from 
`github <http://github.com/pycassa/pycassa/downloads>`_
or check out the latest source from github::

  $ git clone git://github.com/pycassa/pycassa.git

You can simply copy the pycassa directory into your project, or
you can install pycassa system-wide:

.. code-block:: bash

  $ cd pycassa/
  $ sudo python setup.py install
