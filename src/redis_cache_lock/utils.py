from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import socket
import threading
import uuid
from enum import IntEnum, unique
from typing import (
    Any, AsyncContextManager, AsyncGenerator, Awaitable,
    Callable, Dict, Optional, Set, Tuple, Type, TypeVar, Union,
)

import attr

LOGGER = logging.getLogger(__name__)


@unique
class CMState(IntEnum):
    initialized = 1
    entered = 2
    exited = 3


_PE_RET_TV = TypeVar('_PE_RET_TV')


class PreExitable:
    """ An AsyncContextManager wrapper that allows exiting before the `with` block is over """

    def __init__(self, cm: AsyncContextManager[_PE_RET_TV]) -> None:
        self._cm = cm
        self._state = CMState.initialized

    async def __aenter__(self) -> _PE_RET_TV:
        assert self._state == CMState.initialized
        self._state = CMState.entered
        return await self._cm.__aenter__()

    async def __aexit__(self, *exc_details: Any) -> Any:
        if self._state == CMState.exited:
            return None
        assert self._state == CMState.entered
        self._state = CMState.exited
        return await self._cm.__aexit__(*exc_details)

    async def exit(self) -> Any:
        return await self.__aexit__(None, None, None)


@contextlib.asynccontextmanager
async def task_cm(coro: Awaitable) -> AsyncGenerator[asyncio.Task, None]:
    """ Small helper to run an asyncio task for the duration of the context manager """
    # While it doesn't actually do any `await`, it would be weird to expect non-async here.
    task = asyncio.create_task(coro)
    try:
        yield task
    finally:
        task.cancel()


@attr.s(auto_attribs=True)
class CacheShareItem:
    key: str
    done: bool = attr.ib(default=False)
    result: Any = attr.ib(default=None)
    lock: asyncio.Lock = attr.ib(factory=asyncio.Lock)
    waiters: Set[Any] = attr.ib(factory=set)


def get_current_task_name() -> Optional[str]:
    current_task = asyncio.current_task()
    if current_task is not None:
        get_name = getattr(current_task, 'get_name', None)  # python 3.7 doesn't
        if get_name is not None and callable(get_name):
            return get_name()
    return None


def get_self_id() -> str:
    pieces = [
        'h_' + socket.gethostname(),
        'p_' + str(os.getpid()),
    ]
    thread_name = threading.current_thread().name
    if thread_name and thread_name != 'MainThread':
        pieces.append('t_' + thread_name)  # rare
    current_task_name = get_current_task_name()
    if current_task_name:
        pieces.append('a_' + current_task_name)
    pieces.append('r_' + str(uuid.uuid4()))
    return '_'.join(pieces)


_CSS_RES_TV = TypeVar('_CSS_RES_TV')


@attr.s
class CacheShareSingleton:
    """
    A helper for in-memory synchronization of cached value generation.

    Intended to be used as a wrapper around `RedisCacheLock` calls, for a bit
    more performance.
    """

    item_cls: Type[CacheShareItem] = CacheShareItem

    cache: Dict[str, CacheShareItem] = attr.ib(factory=dict)
    track_waiters: bool = attr.ib(default=True)
    debug: bool = attr.ib(default=False)

    def _debug(self, msg: str, *args: Any) -> None:
        if self.debug:
            LOGGER.debug(msg, *args)

    async def generate_with_cache(
            self,
            key: str,
            generate: Callable[[], Awaitable[_CSS_RES_TV]],
    ) -> _CSS_RES_TV:
        cache_item = self.cache.get(key)
        if cache_item is None:
            self._debug('Initializing: key=%r', key)
            cache_item = self.item_cls(key=key)
            self.cache[key] = cache_item

        # Not a normal case, actually:
        # should imply non-empty `cache_item.waiters`
        if cache_item.done:
            self._debug('Found ready: key=%r, item=%r', key, cache_item)  # rare
            return cache_item.result  # rare

        this_request = (object(), get_current_task_name() or str(uuid.uuid4()))
        assert this_request not in cache_item.waiters

        try:
            # This can be removed in favor of python's refcounting (using weakref in `self.cache`),
            # for more performance and less debuggability.
            cache_item.waiters.add(this_request)

            self._debug('Locking %r...', key)
            async with cache_item.lock:
                if cache_item.done:
                    self._debug('Locked %r, found it done.', key)
                    return cache_item.result

                self._debug('Generating %r...', key)
                result = await generate()
                cache_item.result = result
                cache_item.done = True
                return result

        finally:
            cache_item.waiters.remove(this_request)
            if not cache_item.waiters:
                self._debug('Clearing %r.', key)
                # Make it garbage-collectable soon enough:
                self.cache.pop(key)


_GF_RET_TV = TypeVar('_GF_RET_TV')


def wrap_generate_func(
        func: Callable[[], Awaitable[_GF_RET_TV]],
        serialize: Callable[[Any], Union[bytes, str]] = json.dumps,
        default_encoding: str = 'utf-8',
) -> Callable[[], Awaitable[Tuple[bytes, _GF_RET_TV]]]:
    """
    Given a function that returns some value, wrap it to also return the
    serialized version of the value for use in RedisCacheLock.
    """

    async def wrapped_generate_func() -> Tuple[bytes, _GF_RET_TV]:
        result = await func()
        result_b = serialize(result)
        if isinstance(result_b, str):
            result_b = result_b.encode(default_encoding)
        return result_b, result

    return wrapped_generate_func
