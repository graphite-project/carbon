#!/usr/bin/env python

from __future__ import with_statement

import os
import ConfigParser

import platform
from glob import glob

try:
    from io import BytesIO
except ImportError:
    from StringIO import StringIO as BytesIO

# Graphite historically has an install prefix set in setup.cfg. Being in a
# configuration file, it's not easy to override it or unset it (for installing
# graphite in a virtualenv for instance).
# The prefix is now set by ``setup.py`` and *unset* if an environment variable
# named ``GRAPHITE_NO_PREFIX`` is present.
# While ``setup.cfg`` doesn't contain the prefix anymore, the *unset* step is
# required for installations from a source tarball because running
# ``python setup.py sdist`` will re-add the prefix to the tarball's
# ``setup.cfg``.
with open('setup.cfg', 'r') as f:
    orig_setup_cfg = f.read()
cf = ConfigParser.ConfigParser()
cf.readfp(BytesIO(orig_setup_cfg), 'setup.cfg')

if os.environ.get('GRAPHITE_NO_PREFIX'):
    cf.remove_section('install')
else:
    try:
        cf.add_section('install')
    except ConfigParser.DuplicateSectionError:
        pass
    cf.set('install', 'prefix', '/opt/graphite')
    cf.set('install', 'install-lib', '%(prefix)s/lib')

with open('setup.cfg', 'wb') as f:
    cf.write(f)

if os.environ.get('USE_SETUPTOOLS'):
  from setuptools import setup
  setup_kwargs = dict(zip_safe=0)

else:
  from distutils.core import setup
  setup_kwargs = dict()


storage_dirs = [ ('storage/whisper',[]), ('storage/lists',[]),
                 ('storage/log',[]), ('storage/rrd',[]) ]
conf_files = [ ('conf', glob('conf/*.example')) ]

install_files = storage_dirs + conf_files

# If we are building on RedHat, let's use the redhat init scripts.
if platform.dist()[0] == 'redhat':
    init_scripts = [ ('/etc/init.d', ['distro/redhat/init.d/carbon-cache',
                                      'distro/redhat/init.d/carbon-relay',
                                      'distro/redhat/init.d/carbon-aggregator']) ]
    install_files += init_scripts

try:
    setup(
      name='carbon',
      version='0.9.12',
      url='http://graphite-project.github.com',
      author='Chris Davis',
      author_email='chrismd@gmail.com',
      license='Apache Software License 2.0',
      description='Backend data caching and persistence daemon for Graphite',
      long_description='Backend data caching and persistence daemon for Graphite',
      packages=['carbon', 'carbon.aggregator', 'twisted.plugins'],
      package_dir={'' : 'lib'},
      scripts=glob('bin/*'),
      package_data={ 'carbon' : ['*.xml'] },
      data_files=install_files,
      install_requires=['twisted', 'txamqp'],
      **setup_kwargs
    )
finally:
    with open('setup.cfg', 'w') as f:
        f.write(orig_setup_cfg)
