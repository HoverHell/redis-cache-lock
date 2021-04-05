from __future__ import annotations

import asyncio
import contextlib
import json
import multiprocessing
import os
import random
import time
import traceback
from typing import (
    TYPE_CHECKING, Any, AsyncGenerator, Awaitable,
    Callable, List, Optional, Set, TextIO, Tuple,
)

import aioredis
import attr

from redis_cache_lock.main import RedisCacheLock
from redis_cache_lock.utils import wrap_generate_func

if TYPE_CHECKING:
    from contextlib import AsyncExitStack

    from aioredis import Redis, RedisSentinel


@attr.s(auto_attribs=True)
class SentinelPool:
    """
    TODO: find an equivalent in the aioredis itself.
    """
    sentinels: list[tuple[str, int]]
    db: int
    password: str
    service_name: str

    _sentinel_cli: Optional[RedisSentinel] = None
    _main_redis_cli: Optional[Redis] = None
    _busy_clients: Optional[Set[Redis]] = None
    _free_clients: Optional[List[Redis]] = None

    def __attrs_post_init__(self) -> None:
        self._reset()

    def _reset(self) -> None:
        self._free_clients = []
        self._busy_clients = set()
        self._sentinel_cli = None

    @contextlib.asynccontextmanager
    async def manage(self) -> AsyncGenerator[RedisSentinel, None]:
        assert self._sentinel_cli is None
        assert self._free_clients is not None
        assert self._busy_clients is not None
        sentinel_cli = await aioredis.create_sentinel(
            sentinels=self.sentinels,
            db=self.db,
            password=self.password,
        )
        self._sentinel_cli = sentinel_cli
        try:
            self._main_redis_cli = sentinel_cli.master_for(self.service_name)
            yield sentinel_cli
        finally:
            try:
                # Possibly unnecessary, but force it anyway:
                all_clients: List[Redis] = (
                    [self._main_redis_cli]
                    + self._free_clients
                    + list(self._busy_clients)
                )
                for cli in all_clients:
                    cli.close()
                sentinel_cli.close()

                asyncio.gather(*[cli.wait_closed() for cli in all_clients])
                await sentinel_cli.wait_closed()
            finally:
                self._reset()

    async def _make_cli(self, master: bool = True) -> Redis:
        assert self._sentinel_cli is not None
        if master:
            return self._sentinel_cli.master_for(self.service_name)
        return self._sentinel_cli.slave_for(self.service_name)

    @contextlib.asynccontextmanager
    async def cli_acm(
            self, *, master: bool = True, exclusive: bool = True,
    ) -> AsyncGenerator[Redis, None]:
        if not exclusive:
            assert self._main_redis_cli is not None
            yield self._main_redis_cli
            return

        # elif exclusive:
        assert self._free_clients is not None
        assert self._busy_clients is not None

        try:
            cli = self._free_clients.pop(0)
        except IndexError:
            cli = await self._make_cli(master=master)

        self._busy_clients.add(cli)

        try:
            yield cli
        except Exception:  # pylint: disable=broad-except
            # Do not consider the client okay if an error happened (just in case).
            cli.close()
            # TODO?: potentially should close and re-open the `self._sentinel_cli` too.
        else:
            self._free_clients.append(cli)
        finally:
            self._busy_clients.remove(cli)


@attr.s(auto_attribs=True)
class Worker:

    redis_sentinel_cfg: dict
    nkeys: int = 100
    ntasks: int = 12
    duration_sec: float = 120.0
    lock_ttl_sec: float = 4.0
    data_ttl_sec: float = 12.0
    min_data_gen_time: float = 0.01
    max_data_gen_time: float = 8.0
    resource_tag: str = 'load_test'
    log_fln_tpl: str = '.log/results_{pid}.ndjson'

    _acm_stack: Optional[AsyncExitStack] = None
    _sentinel_pool: Optional[SentinelPool] = None
    _rcl: Optional[RedisCacheLock] = None
    _log_fobj: Optional[TextIO] = None
    _monotonic_offset: Optional[float] = None
    _hits: int = 0
    _misses: int = 0
    _errors: int = 0

    # Note: methods are ordered in the execution order.

    @classmethod
    def run(cls, nprocs: int = 24, **kwargs: Any) -> List[dict]:
        with multiprocessing.Pool(nprocs) as pool:
            results = list(pool.imap_unordered(
                cls._c_run_one,
                [kwargs for _ in range(nprocs)],
            ))
            assert results
        return results

    @classmethod
    def _c_run_one(cls, kwargs: Any) -> dict:
        worker = cls(**kwargs)
        return worker._run()  # pylint: disable=protected-access

    def _run(self) -> dict:
        return asyncio.run(self._arun())

    # async def self._sentinel_client.master_for(self._namespace)
    async def _arun(self) -> dict:
        assert self._acm_stack is None
        assert self._sentinel_pool is None
        assert self._rcl is None
        assert self._log_fobj is None

        acm_stack = contextlib.AsyncExitStack()
        self._acm_stack = acm_stack
        try:
            async with acm_stack:
                sentinel_pool = SentinelPool(**self.redis_sentinel_cfg)
                self._sentinel_pool = sentinel_pool
                await acm_stack.enter_async_context(sentinel_pool.manage())
                self._rcl = RedisCacheLock(
                    client_acm=sentinel_pool.cli_acm,
                    resource_tag=self.resource_tag,
                    lock_ttl_sec=self.lock_ttl_sec,
                    data_ttl_sec=self.data_ttl_sec,
                )
                log_fln = self.log_fln_tpl.format(
                    pid=os.getpid(),
                )
                os.makedirs(os.path.dirname(log_fln), exist_ok=True)
                log_fobj = acm_stack.enter_context(open(log_fln, 'a', 1))
                self._log_fobj = log_fobj
                result = await self._arun_all()
                return result
        finally:
            self._log_fobj = None
            self._rcl = None
            self._sentinel_pool = None
            self._acm_stack = None

        raise Exception('Should not end up here')

    async def _arun_all(self) -> dict:
        assert self._rcl is not None
        tasks = [
            asyncio.create_task(self._arun_one())
            for _ in range(self.ntasks)
        ]
        try:
            await asyncio.wait(tasks, timeout=self.duration_sec)
        finally:
            for task in tasks:
                task.cancel()
        return dict(
            pid=os.getpid(),
            hits=self._hits,
            misses=self._misses,
            errors=self._errors,
        )

    async def _arun_one(self) -> None:
        rcl = self._rcl
        assert rcl is not None
        while True:
            key = 'k{}'.format(random.randrange(self.nkeys))
            gen_time = (
                self.min_data_gen_time
                + random.random() * (self.max_data_gen_time - self.min_data_gen_time)
            )
            gen_func = self._make_gen_func(gen_time)
            result = None
            result_b = None
            result_exc = None
            t01 = time.monotonic()
            try:
                _result = await rcl.generate_with_lock(
                    key=key,
                    generate_func=gen_func,
                )
                result_b, result = _result
            except Exception as exc_:  # pylint: disable=broad-except
                result_exc = exc_
                self._errors += 1
                traceback.print_exc()
            else:
                if result is not None:
                    self._misses += 1
                else:
                    self._hits += 1
            t02 = time.monotonic()

            result_b_maybe = (
                result_b.decode('utf-8')
                if result is None and result_b is not None
                else None
            )
            await self._handle_result(
                ts=self._get_absolute_timestamp(),
                key=key,
                gen_time=gen_time,
                result=result,
                result_b=result_b_maybe,
                result_exc=result_exc,
                td=t02 - t01,
            )

    def _make_gen_func(self, duration: float) -> Callable[[], Awaitable[Tuple[bytes, dict]]]:

        async def gen_func(self: Worker = self, duration: float = duration) -> dict:
            await asyncio.sleep(duration)
            return dict(
                now=self._get_absolute_timestamp(),
                duration=duration,
            )

        return wrap_generate_func(gen_func)

    def _get_absolute_timestamp(self) -> float:
        """ Monotonic unix timestamp: less correct (but close enough), more monotonic """
        offset = self._monotonic_offset
        if offset is None:
            offset = time.time() - time.monotonic()
            self._monotonic_offset = offset
        return time.monotonic() + offset

    async def _handle_result(self, **kwargs: Any) -> None:
        assert self._log_fobj is not None
        data_s = json.dumps(kwargs, default=repr)
        data_s = data_s + '\n'
        self._log_fobj.write(data_s)
