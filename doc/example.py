#!/usr/bin/env python3

from __future__ import annotations

import asyncio
import contextlib
import datetime
import json
import sys
from typing import Any, AsyncGenerator

import aioredis

from redis_cache_lock.main import RedisCacheLock
from redis_cache_lock.utils import wrap_generate_func


@contextlib.asynccontextmanager
async def connect_to_redis(**_: Any) -> AsyncGenerator[aioredis.Redis, None]:
    # Some newer versions of aioredis might have a pool to do this.
    cli = await aioredis.create_redis("redis://localhost")
    try:
        yield cli
    finally:
        cli.close()
        await cli.wait_closed()


async def make_data() -> Any:
    await asyncio.sleep(2.5)
    return dict(value=datetime.datetime.utcnow().isoformat())


async def amain() -> None:
    key = "example_key" if len(sys.argv) < 2 else sys.argv[1]
    rcl = RedisCacheLock(
        key=key,
        client_acm=connect_to_redis,
        resource_tag="example_ns",
        lock_ttl_sec=1.1,
        data_ttl_sec=3600,
    )
    result_b, result = await rcl.generate_with_lock(
        generate_func=wrap_generate_func(make_data),
    )
    if result is None:
        result = json.loads(result_b)
    sys.stdout.write("Result: {!r}\n".format(result))


if __name__ == "__main__":
    asyncio.run(amain())
