#!/usr/bin/env python3

from __future__ import annotations

import os
from setuptools import setup, find_packages


__version__ = '1.0'


SETUP_KWARGS = dict(
    name='redis-cache-lock',
    version=__version__,
    author='HoverHell',
    author_email='hoverhell@gmail.com',
    packages=find_packages(exclude=['tests*']),
    install_requires=[
        'attrs',
        'aioredis',  # not technically required, but hard to avoid anyway.
    ],
)


if __name__ == '__main__':
    setup(**SETUP_KWARGS)
