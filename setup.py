#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

import sys
import os

try:
    import subprocess
    has_subprocess = True
except:
    has_subprocess = False

from ez_setup import use_setuptools
use_setuptools()
from setuptools import setup
from distutils.cmd import Command

__version_info__ = (1, 0, 4)
__version__ = '.'.join([str(v) for v in __version_info__])

long_description = """pycassa is a python client library for Apache Cassandra with the following features:

1. Auto-failover single or thread-local connections
2. Connection pooling
3. A batch interface
4. Simplified version of the Thrift interface
5. A method to map an existing class to a Cassandra column family
"""

class doc(Command):

    description = "generate or test documentation"

    user_options = [("test", "t",
                     "run doctests instead of generating documentation")]

    boolean_options = ["test"]

    def initialize_options(self):
        self.test = False

    def finalize_options(self):
        pass

    def run(self):
        if self.test:
            path = "doc/_build/doctest"
            mode = "doctest"
        else:
            path = "doc/_build/%s" % __version__
            mode = "html"

            try:
                os.makedirs(path)
            except:
                pass

        if has_subprocess:
            status = subprocess.call(["sphinx-build", "-b", mode, "doc", path])

            if status:
                raise RuntimeError("documentation step '%s' failed" % mode)

            print ""
            print "Documentation step '%s' performed, results here:" % mode
            print "   %s/" % path
        else:
            print """
`setup.py doc` is not supported for this version of Python.

Please ask in the user forums for help.
"""


setup(
      name = 'pycassa',
      version = __version__,
      author = 'Jonathan Hseu',
      author_email = 'vomjom AT vomjom.net',
      maintainer = 'Tyler Hobbs',
      maintainer_email = 'pycassa.maintainer@gmail.com',
      description = 'Python client library for Apache Cassandra',
      long_description = long_description,
      url = 'http://github.com/pycassa/pycassa',
      download_url = 'http://github.com/downloads/pycassa/pycassa/pycassa-%s.tar.gz' % __version__,
      keywords = 'cassandra client db distributed thrift',
      packages = ['pycassa', 'pycassa.cassandra', 'pycassa.logging'],
      py_modules = ['ez_setup'],
      requires = ['thrift05'],
      scripts=['pycassaShell'],
      cmdclass={"doc": doc},
      classifiers=[
          'Development Status :: 5 - Production/Stable',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: MIT License',
          'Natural Language :: English',
          'Operating System :: OS Independent',
          'Programming Language :: Python :: 2.4',
          'Programming Language :: Python :: 2.5',
          'Programming Language :: Python :: 2.6',
          'Programming Language :: Python :: 2.7',
          'Topic :: Software Development :: Libraries :: Python Modules'
          ]
      )
