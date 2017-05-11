#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from os import path
from setuptools import setup
from distutils.cmd import Command

NAME = 'redpipe'

ROOTDIR = path.abspath(os.path.dirname(__file__))

with open(os.path.join(ROOTDIR, 'README.rst')) as f:
    readme = f.read()

with open(os.path.join(ROOTDIR, 'docs', 'release-notes.rst')) as f:
    history = f.read()

with open(os.path.join(ROOTDIR, 'redpipe', 'VERSION')) as f:
    version = str(f.read().strip())


class TestCommand(Command):
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        import sys
        import subprocess

        raise SystemExit(
            subprocess.call([sys.executable, '-m', 'test']))


cmdclass = {'test': TestCommand}
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
        'Development Status :: 5 - Production/Stable',
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
    tests_require=['redislite>=3.0.271', 'redis-py-cluster>=1.3.0'],
    include_package_data=True,
    long_description=readme + '\n\n' + history,
    cmdclass=cmdclass,
    ext_modules=ext_modules
)
