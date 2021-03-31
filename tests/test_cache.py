# pylint: disable=redefined-outer-name

from __future__ import annotations

import asyncio
import collections
import contextlib
import functools
import itertools
import json
import logging
import time
import uuid
from typing import (
    TYPE_CHECKING, Any, AsyncContextManager, AsyncGenerator,
    Awaitable, Callable, Tuple, TypeVar, Union,
)

import aioredis
import pytest

from redis_cache_lock.main import RedisCacheLock

if TYPE_CHECKING:
    from aioredis import Redis


ZERO_TIME = time.monotonic()
MONOTONIC_SHIFT = time.time() - time.monotonic()


@contextlib.asynccontextmanager
async def localhost_cli_acm() -> AsyncGenerator[Redis, None]:
    cli = await aioredis.create_redis('redis://localhost')
    try:
        yield cli
    finally:
        cli.close()
        await cli.wait_closed()


@pytest.fixture
async def cli_acm() -> Callable[[], AsyncContextManager[Redis]]:
    # override point
    return localhost_cli_acm


def lock_mgr(cli_acm, **kwargs) -> RedisCacheLock:
    logs: collections.deque[Any] = collections.deque(maxlen=100)

    def log_func(msg, details, logs=logs):  # pylint: disable=dangerous-default-value
        ts = round(time.monotonic() - ZERO_TIME, 4)
        item = (ts, msg, details)
        logs.append(item)
        logging.debug('RedisCacheLock %r: %r', mgr, item)

    full_kwargs = dict(
        client_acm=cli_acm,
        resource_tag='tst',
        lock_ttl_sec=2.3,
        data_ttl_sec=5.6,
        debug_log=log_func,
    )
    full_kwargs.update(kwargs)
    mgr = RedisCacheLock(**full_kwargs)
    setattr(mgr, 'logs_', logs)
    return mgr


@pytest.fixture
def lock_mgr_gen(cli_acm) -> Callable[[], RedisCacheLock]:
    return functools.partial(lock_mgr, cli_acm=cli_acm)


_GF_RET_TV = TypeVar('_GF_RET_TV')


def wrap_generate_func(
        func: Callable[[], Awaitable[_GF_RET_TV]],
        serialize: Callable[[Any], Union[bytes, str]] = json.dumps,
        default_encoding='utf-8',
) -> Callable[[], Awaitable[Tuple[bytes, _GF_RET_TV]]]:

    async def wrapped_generate_func() -> Tuple[bytes, _GF_RET_TV]:
        result = await func()
        result_b = serialize(result)
        if isinstance(result_b, str):
            result_b = result_b.encode(default_encoding)
        return result_b, result

    return wrapped_generate_func


class CounterGenerate:
    def __init__(self, sleep_time: float = 0.01):
        self.sleep_time = sleep_time
        self.cnt = itertools.count(1)

    async def _raw_generate_func(self):
        await asyncio.sleep(self.sleep_time)
        return dict(value=next(self.cnt))

    @property
    def generate_func(self):
        return wrap_generate_func(self._raw_generate_func)


@pytest.mark.asyncio
async def test_minimal_lock(lock_mgr_gen):
    count = 5
    key = str(uuid.uuid4())

    lock_mgr = lock_mgr_gen()
    gen = CounterGenerate()

    for idx in range(count):
        result_b, result_raw = await lock_mgr.generate_with_lock(
            key=key, generate_func=gen.generate_func,
        )
        if idx == 0:
            assert result_raw == dict(value=1)
        else:
            assert result_raw is None
        assert result_b == b'{"value": 1}', idx

    result_b, result_raw = await lock_mgr.generate_with_lock(
        key=key + '02', generate_func=gen.generate_func,
    )
    assert result_raw == dict(value=2)
    assert result_b == b'{"value": 2}'


@pytest.mark.asyncio
async def test_sync_lock(lock_mgr_gen):
    count = 7
    key = str(uuid.uuid4())

    mgrs = [lock_mgr_gen() for _ in range(count)]
    gen = CounterGenerate()

    results = await asyncio.gather(
        *[
            lock_mgr.generate_with_lock(key=key, generate_func=gen.generate_func)
            for lock_mgr in mgrs
        ],
    )

    assert results
    item_full = (b'{"value": 1}', {'value': 1})
    item_cached = (b'{"value": 1}', None)
    res_full = [item for item in results if item == item_full]
    res_cached = [item for item in results if item == item_cached]
    assert len(res_full) == 1, results
    assert len(res_cached) == count - 1, results


@pytest.mark.asyncio
async def test_sync_long_lock(lock_mgr_gen):
    count = 7
    key = str(uuid.uuid4())

    mgrs = [lock_mgr_gen() for _ in range(count)]
    gen = CounterGenerate(sleep_time=5)

    results = await asyncio.gather(
        *[
            lock_mgr.generate_with_lock(key=key, generate_func=gen.generate_func)
            for lock_mgr in mgrs
        ],
    )

    assert results
    item_full = (b'{"value": 1}', {'value': 1})
    item_cached = (b'{"value": 1}', None)
    res_full = [item for item in results if item == item_full]
    res_cached = [item for item in results if item == item_cached]
    assert len(res_full) == 1, results
    assert len(res_cached) == count - 1, results
