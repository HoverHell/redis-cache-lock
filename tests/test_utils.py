from __future__ import annotations

import asyncio
import time

import pytest

from redis_cache_lock.utils import CacheShareSingleton


async def some_generate(key: str):
    print(f'Generating {key}...')
    await asyncio.sleep(0.5)
    result = time.time()
    print(f'Generated {key}: {result}')
    return result


async def some_generate_with_cache(caches, key):
    return key, await caches.generate_with_cache(
        key=key,
        generate=lambda key=key: some_generate(key),
    )


@pytest.mark.asyncio
async def test_cache_share():
    caches = CacheShareSingleton(debug=True)

    count = 20
    results = await asyncio.gather(
        *[
            some_generate_with_cache(caches=caches, key=f'kk{idx // 4}')
            for idx in range(count)
        ],
    )
    # While usually the `some_generate` is called `count` times, there is,
    # technically, no guarantee, as it depends on the ordering.
    assert results
