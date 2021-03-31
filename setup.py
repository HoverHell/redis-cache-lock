#!/usr/bin/env python3

from __future__ import annotations

import os
from setuptools import setup, find_packages


__version__ = '1.0'


LONG_DESCRIPTION = '''
Similar to `aioredis lock
<https://github.com/aio-libs/aioredis-py/blob/master/aioredis/lock.py>`_,
but optimized for synchronizing cache, to reduce work done.

Highly similar to `redis-memolock
<https://github.com/kristoff-it/redis-memolock>`_.
'''


SETUP_KWARGS = dict(
    name='redis-cache-lock',
    version=__version__,

    python_requires='>=3.7',
    install_requires=[
        'attrs',
        'aioredis',  # not technically required, but hard to avoid anyway.
    ],
    packages=find_packages('src'),
    # # to consider:
    package_dir={'': 'src'},
    # py_modules=['redis_cache_lock'],
    # include_package_data=True,

    description='Synchronizing cache generation to reduce work',
    long_description=LONG_DESCRIPTION,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Topic :: Software Development :: Libraries :: Python Modules',
        # TODO: more
    ],

    author='HoverHell',
    author_email='hoverhell@gmail.com',
    url='https://gitlab.com/hoverhell/redis-cache-lock/',
    license='MIT',
)


if __name__ == '__main__':
    setup(**SETUP_KWARGS)
