from __future__ import annotations

import asyncio
import json
import math
import multiprocessing
import os
import random
import time
import traceback
from contextlib import AsyncExitStack, asynccontextmanager, contextmanager
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Generator,
    List,
    Optional,
    TextIO,
    Tuple,
)

import aioredis
import attr

from redis_cache_lock.main import RedisCacheLock
from redis_cache_lock.utils import (
    HistoryHolder,
    monotime,
    sentinel_client_acm,
    wrap_generate_func,
)

if TYPE_CHECKING:
    from aioredis import RedisSentinel

    from redis_cache_lock.types import TClientACM  # pylint: disable=ungrouped-imports


def random_geometric(p: float) -> int:
    return int(math.ceil(math.log(random.random()) / math.log(1 - p)))


@asynccontextmanager
async def sentinel_acm(**cfg: Any) -> AsyncGenerator[RedisSentinel, None]:
    sentinel_cli = await aioredis.create_sentinel(**cfg)
    try:
        yield sentinel_cli
    finally:
        sentinel_cli.close()
        await sentinel_cli.wait_closed()


@attr.s(auto_attribs=True)
class Worker:

    redis_sentinel_cfg: dict
    # Geometric distribution,
    # 0.999 of the keys will be within 10_000,
    # first few keys will be at 700 ppm each
    geom_p: float = 0.00069
    ntasks: int = 12
    duration_sec: float = 120.0
    lock_ttl_sec: float = 4.0
    data_ttl_sec: float = 12.0
    # Loop-yielding duration of the generating function.
    # Uniform distribution.
    min_data_gen_time: float = 0.01
    max_data_gen_time: float = 8.0
    # Loop-blocking duration of the generating function.
    # Exponential distribution,
    # scale is mean time (in seconds),
    # force-capped at `max_data_block_time` (for convenience).
    block_time_scale: float = 1.0
    max_data_block_time: float = 10.0
    resource_tag: str = "load_test"
    log_fln_tpl: str = ".log/results_{pid}.ndjson"

    _cm_stack: Optional[AsyncExitStack] = None
    _sentinel_cli: Optional[RedisSentinel] = None
    _client_acm: Optional[TClientACM] = None
    _rcls: List[RedisCacheLock] = attr.ib(factory=list)
    _log_fobj: Optional[TextIO] = None
    _hits: int = 0
    _misses: int = 0
    _errors: int = 0

    def _make_rcl(self, key: str = "") -> RedisCacheLock:
        client_acm = self._client_acm
        assert client_acm is not None
        history_holder = HistoryHolder()
        rcl = RedisCacheLock(
            key=key,
            client_acm=client_acm,
            resource_tag=self.resource_tag,
            lock_ttl_sec=self.lock_ttl_sec,
            data_ttl_sec=self.data_ttl_sec,
            enable_background_tasks=True,
            enable_slave_get=False,
            debug_log=history_holder,
        )
        setattr(rcl, "logs_", history_holder)
        self._rcls.append(rcl)
        return rcl

    def _make_gen_func(
        self,
        gen_time: float,
        block_time: float,
    ) -> Callable[[], Awaitable[Tuple[bytes, dict]]]:
        async def gen_func(
            # self: Worker = self,
            gen_time: float = gen_time,
            block_time: float = block_time,
        ) -> dict:
            time.sleep(block_time)
            await asyncio.sleep(gen_time)
            return dict(
                now=monotime(),
                gen_time=gen_time,
                block_time=block_time,
            )

        func = wrap_generate_func(gen_func)
        setattr(func, "gen_time_", gen_time)
        setattr(func, "block_time_", block_time)
        return func

    def _make_random_gen_func(self) -> Callable[[], Awaitable[Tuple[bytes, dict]]]:
        gen_time = random.uniform(
            self.min_data_gen_time,
            self.max_data_gen_time,
        )
        block_time = min(
            random.expovariate(1 / self.block_time_scale),
            self.max_data_block_time,
        )
        return self._make_gen_func(gen_time=gen_time, block_time=block_time)

    def _make_random_key(self) -> str:
        return "k{}".format(random_geometric(self.geom_p))

    async def _handle_result(self, **kwargs: Any) -> None:
        assert self._log_fobj is not None
        data_s = json.dumps(kwargs, default=repr)
        data_s = data_s + "\n"
        self._log_fobj.write(data_s)

    async def _arun_one(self) -> None:
        while True:
            key = self._make_random_key()
            gen_func = self._make_random_gen_func()

            result = None
            result_b = None
            result_exc = None
            t01 = time.monotonic_ns()
            rcl = self._make_rcl(key=key)
            try:
                _result = await rcl.generate_with_lock(gen_func)
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
            t02 = time.monotonic_ns()

            result_b_maybe = (
                result_b.decode("utf-8")
                if result is None and result_b is not None
                else None
            )
            await self._handle_result(
                ts=monotime(),
                key=key,
                gen_time=getattr(gen_func, "gen_time_"),
                block_time=getattr(gen_func, "block_time_"),
                history=getattr(rcl, "logs_").history,
                result=result,
                result_b=result_b_maybe,
                result_exc=result_exc,
                td=t02 - t01,
            )

    async def _arun_all(self) -> dict:
        tasks = [asyncio.create_task(self._arun_one()) for _ in range(self.ntasks)]
        try:
            await asyncio.wait(tasks, timeout=self.duration_sec)
        finally:
            for task in tasks:
                task.cancel()

        # TODO: gather bg tasks from all managers
        await asyncio.sleep(2)

        return dict(
            pid=os.getpid(),
            hits=self._hits,
            misses=self._misses,
            errors=self._errors,
        )

    async def _arun_i(self) -> dict:
        assert self._log_fobj is None
        cm_stack = self._cm_stack
        assert cm_stack is not None

        sentinel_cfg = self.redis_sentinel_cfg
        sentinel_cli = await cm_stack.enter_async_context(
            sentinel_acm(
                sentinels=sentinel_cfg["sentinels"],
                db=sentinel_cfg["db"],
                password=sentinel_cfg["password"],
            )
        )
        self._sentinel_cli = sentinel_cli
        self._client_acm = sentinel_client_acm(
            sentinel_cli, sentinel_cfg["service_name"]
        )

        log_fln = self.log_fln_tpl.format(
            pid=os.getpid(),
        )
        os.makedirs(os.path.dirname(log_fln), exist_ok=True)
        log_fobj = cm_stack.enter_context(open(log_fln, "a", 1))
        self._log_fobj = log_fobj

        return await self._arun_all()

    @contextmanager
    def _cleanup_cm(self) -> Generator[None, None, None]:
        try:
            yield None
        finally:
            self._log_fobj = None
            self._client_acm = None
            self._sentinel_cli = None

    async def _arun(self) -> dict:
        async with AsyncExitStack() as cm_stack:
            self._cm_stack = cm_stack
            cm_stack.enter_context(self._cleanup_cm())
            return await self._arun_i()
        raise Exception("Should have not reached here")

    def _run(self) -> dict:
        return asyncio.run(self._arun())

    @classmethod
    def _c_run_one(cls, kwargs: Any) -> dict:
        worker = cls(**kwargs)
        return worker._run()  # pylint: disable=protected-access

    @classmethod
    def run(cls, nprocs: int = 24, **kwargs: Any) -> List[dict]:
        with multiprocessing.Pool(nprocs) as pool:
            results = list(
                pool.imap_unordered(
                    cls._c_run_one,
                    [kwargs for _ in range(nprocs)],
                )
            )
            assert results
        return results
