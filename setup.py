#!/usr/bin/env python
import os
from os import path
from setuptools import setup
import imp

NAME = 'redpipe'

MYDIR = path.abspath(os.path.dirname(__file__))
long_description = open(os.path.join(MYDIR, 'README.rst')).read()
version = imp.load_source(
    'version',
    path.join('.', 'redpipe', 'version.py')).__version__

cmdclass = {}
ext_modules = []

setup(
    name=NAME,
    version=version,
    description='Easy Redis pipelines',
    author='John Loehrer',
    author_email='72squared@gmail.com',
    url='https://github.com/72squared/%s' % NAME,
    download_url='https://github.com/72squared/%s/archive/%s.tar.gz' %
                 (NAME, version),
    keywords='redis redis-pipeline orm database',
    packages=[NAME],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Programming Language :: Python',
        'Programming Language :: Python',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Environment :: Web Environment',
        'Operating System :: POSIX'],
    license='MIT',
    install_requires=['redis>=2.10.2', 'six'],
    include_package_data=True,
    long_description=long_description,
    cmdclass=cmdclass,
    ext_modules=ext_modules
)
