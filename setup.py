#!/usr/bin/env python
try:
    from setuptools import setup
except ImportError:
    from distuils.core import setup


packages = [
    'datalib'
]


setup(
    name='datalib',
    version='v0.1',
    description='pipeline architecture data library',
    author='Yasufumi Taniguchi',
    author_email='yasufumi.taniguchi@gmail.com',
    packages=packages,
    licence='MIT'
)
