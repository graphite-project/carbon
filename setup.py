#!/usr/bin/env python

from __future__ import with_statement

import os
from glob import glob
try:
    from ConfigParser import ConfigParser, DuplicateSectionError  # Python 2
except ImportError:
    from configparser import ConfigParser, DuplicateSectionError  # Python 3


# Graphite historically has an install prefix set in setup.cfg. Being in a
# configuration file, it's not easy to override it or unset it (for installing
# graphite in a virtualenv for instance).
# The prefix is now set by ``setup.py`` and *unset* if an environment variable
# named ``GRAPHITE_NO_PREFIX`` is present.
# While ``setup.cfg`` doesn't contain the prefix anymore, the *unset* step is
# required for installations from a source tarball because running
# ``python setup.py sdist`` will re-add the prefix to the tarball's
# ``setup.cfg``.
cf = ConfigParser()

with open('setup.cfg', 'r') as f:
    orig_setup_cfg = f.read()
    f.seek(0)
    cf.readfp(f, 'setup.cfg')

if os.environ.get('GRAPHITE_NO_PREFIX'):
    cf.remove_section('install')
else:
    print('#' * 80)
    print('')
    print('Carbon\'s default installation prefix is "/opt/graphite".')
    print('')
    print('To install Carbon in the Python\'s default location run:')
    print('$ GRAPHITE_NO_PREFIX=True python setup.py install')
    print('')
    print('#' * 80)
    try:
        cf.add_section('install')
    except DuplicateSectionError:
        pass
    if not cf.has_option('install', 'prefix'):
        cf.set('install', 'prefix', '/opt/graphite')
    if not cf.has_option('install', 'install-lib'):
        cf.set('install', 'install-lib', '%(prefix)s/lib')

with open('setup.cfg', 'w') as f:
    cf.write(f)


if os.environ.get('USE_SETUPTOOLS'):
  from setuptools import setup
  setup_kwargs = dict(zip_safe=0)
else:
  from distutils.core import setup
  setup_kwargs = dict()


storage_dirs = [ ('storage/ceres/dummy.txt', []), ('storage/whisper/dummy.txt',[]),
                 ('storage/lists',[]), ('storage/log/dummy.txt',[]),
                 ('storage/rrd/dummy.txt',[]) ]
conf_files = [ ('conf', glob('conf/*.example')) ]

install_files = storage_dirs + conf_files

# Let's include redhat init scripts, despite build platform
# but won't put them in /etc/init.d/ automatically anymore
init_scripts = [ ('examples/init.d', ['distro/redhat/init.d/carbon-cache',
                                      'distro/redhat/init.d/carbon-relay',
                                      'distro/redhat/init.d/carbon-aggregator']) ]
install_files += init_scripts

def read(fname):
    with open(os.path.join(os.path.dirname(__file__), fname)) as f:
        return f.read()

try:
    setup(
        name='carbon',
        version='1.1.8',
        url='http://graphiteapp.org/',
        author='Chris Davis',
        author_email='chrismd@gmail.com',
        license='Apache Software License 2.0',
        description='Backend data caching and persistence daemon for Graphite',
        long_description=read('README.md'),
        long_description_content_type='text/markdown',
        packages=['carbon', 'carbon.aggregator', 'twisted.plugins'],
        package_dir={'' : 'lib'},
        scripts=glob('bin/*'),
        package_data={ 'carbon' : ['*.xml'] },
        data_files=install_files,
        install_requires=['Twisted', 'txAMQP', 'cachetools', 'urllib3'],
        classifiers=(
            'Intended Audience :: Developers',
            'Natural Language :: English',
            'License :: OSI Approved :: Apache Software License',
            'Programming Language :: Python',
            'Programming Language :: Python :: 2',
            'Programming Language :: Python :: 2.7',
            'Programming Language :: Python :: 3',
            'Programming Language :: Python :: 3.5',
            'Programming Language :: Python :: 3.6',
            'Programming Language :: Python :: 3.7',
            'Programming Language :: Python :: 3.8',
            'Programming Language :: Python :: Implementation :: CPython',
            'Programming Language :: Python :: Implementation :: PyPy',
        ),
        **setup_kwargs
    )
finally:
    with open('setup.cfg', 'w') as f:
        f.write(orig_setup_cfg)
