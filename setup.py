#!/usr/bin/env python

from __future__ import with_statement

import os
import platform
from glob import glob

if os.environ.get('USE_SETUPTOOLS'):
  from setuptools import setup
  setup_kwargs = dict(zip_safe=0)
else:
  from distutils.core import setup
  setup_kwargs = dict()


storage_dirs = [ ('storage/ceres', []), ('storage/whisper',[]),
                 ('storage/lists',[]), ('storage/log',[]),
                 ('storage/rrd',[]) ]
conf_files = [ ('conf', glob('conf/*.example')) ]

install_files = storage_dirs + conf_files

# Let's include redhat init scripts, despite build platform
# but won't put them in /etc/init.d/ automatically anymore
init_scripts = [ ('examples/init.d', ['distro/redhat/init.d/carbon-cache',
                                      'distro/redhat/init.d/carbon-relay',
                                      'distro/redhat/init.d/carbon-aggregator']) ]
install_files += init_scripts


setup(
    name='carbon',
    version='1.1.0',
    url='http://graphiteapp.org/',
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
    install_requires=['Twisted', 'txAMQP', 'cachetools'],
    classifiers=(
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ),
    **setup_kwargs
)
