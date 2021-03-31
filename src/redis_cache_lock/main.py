from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack, asynccontextmanager
from typing import (
    TYPE_CHECKING, Any, AsyncContextManager, AsyncGenerator, Awaitable,
    Callable, ClassVar, Dict, Optional, Tuple, Type, TypeVar,
)

import attr

from .enums import ReqResultInternal, ReqScriptResult
# from .exc import CacheError, CacheRedisError, CacheLockLost
from .scripts import ALIVE_PREFIX, DATA_PREFIX, FAIL_PREFIX
from .scripts_support import ForceSaveScript, RenewScript, ReqScript, SaveScript
from .utils import PreExitable, get_self_id, task_cm

if TYPE_CHECKING:
    from aioredis import Channel, Redis


# `generate_func` types:
# `generate_func` must return a bytestring for saving to redis,
# but can also return a value to be returned directly,
# to skip an unnecessary deserialization (if there was no cache hit).
# The cache wrap can return either the full result, or only the bytestring part.
_GF_RET_TV = TypeVar('_GF_RET_TV')
TGenerateResult = Tuple[bytes, _GF_RET_TV]
TCacheResult = Tuple[bytes, Optional[_GF_RET_TV]]
TGenerateFunc = Callable[[], Awaitable[TGenerateResult]]


@attr.s(auto_attribs=True, frozen=True, slots=True)
class SubscriptionElements:
    """ Container for all objects required to manage a redis event subscription """
    cli: Redis
    cli_cm: PreExitable
    channel: Channel
    channels_cm: PreExitable

    @staticmethod
    @asynccontextmanager
    async def channels_acm(
            cli: Redis, channel_key: str,
    ) -> AsyncGenerator[Tuple[Channel, ...], None]:
        channels = await cli.psubscribe(channel_key)
        try:
            yield tuple(channels)
        finally:
            await cli.punsubscribe(*channels)

    @classmethod
    async def create(
            cls,
            cm_stack: AsyncExitStack,
            client_acm: Callable[[], AsyncContextManager[Redis]],
            channel_key: str,
    ) -> SubscriptionElements:
        cli_cm = PreExitable(client_acm())
        cli: Redis  # XXX: the type should've been autoidentified
        cli = await cm_stack.enter_async_context(cli_cm)
        channels_cm = PreExitable(cls.channels_acm(cli=cli, channel_key=channel_key))
        channels: Tuple[Channel, ...]  # XXX: the type should've been autoidentified
        channels = await cm_stack.enter_async_context(channels_cm)
        if len(channels) != 1:
            raise ValueError(
                f'Expected a single channel; '
                f'channel_key={channel_key!r}, channels={channels!r}')
        channel = channels[0]
        return cls(cli=cli, cli_cm=cli_cm, channel=channel, channels_cm=channels_cm)

    async def get_direct(self) -> bytes:
        # returns a `channel_key, message_data` tuple.
        _, message = await self.channel.get()
        return message

    async def get(self, timeout: float) -> Optional[bytes]:
        try:
            return await asyncio.wait_for(self.get_direct(), timeout=timeout)
        except asyncio.TimeoutError:
            return None

    async def close(self) -> None:
        """
        End the subscription and release the client.
        Can be called multiple times.
        """
        # This is done in order and the exceptions are passed through.
        # The fallback closing is through the `cm_stack` which is done fully
        # even if one CM raises.
        await self.channels_cm.exit()
        await self.cli_cm.exit()


@attr.s(auto_attribs=True)
class RedisCacheLock:
    # ACM that should return a redis instance for exclusive use for the duration of the ACM
    client_acm: Callable[[], AsyncContextManager[Redis]]

    resource_tag: str  # namespace for the keys
    lock_ttl_sec: float
    data_ttl_sec: float

    lock_renew_interval: Optional[float] = None
    channel_poll_timeout: Optional[float] = None
    channel_poll_timeout_situation: ReqResultInternal = ReqResultInternal.force_without_cache
    channel_fail_situation: ReqResultInternal = ReqResultInternal.force_without_cache

    debug_log: Optional[Callable[[str, Dict[str, Any]], None]] = None
    enable_background_tasks: bool = False

    req_script_cls: ClassVar[Type[ReqScript]] = ReqScript
    req_script_situation: ClassVar[Type[ReqScriptResult]] = ReqScriptResult
    req_situation: ClassVar[Type[ReqResultInternal]] = ReqResultInternal
    renew_script_cls: ClassVar[Type[RenewScript]] = RenewScript
    save_script_cls: ClassVar[Type[SaveScript]] = SaveScript
    force_save_script_cls: ClassVar[Type[ForceSaveScript]] = ForceSaveScript
    chan_data_prefix: ClassVar[bytes] = DATA_PREFIX
    chan_alive_prefix: ClassVar[bytes] = ALIVE_PREFIX
    chan_fail_prefix: ClassVar[bytes] = FAIL_PREFIX

    data_tag: str = '/data:'
    signal_tag: str = '/notif:'
    lock_tag: str = '/lock:'

    # def __attrs_post_init__(self):
    #     self.channel_pattern = self.resource_tag + self.signal_tag + '*'

    @staticmethod
    def make_self_id() -> str:
        return get_self_id()

    def _log(self, msg: str, *args: Any, **details: Any) -> None:
        if self.debug_log is not None:
            self.debug_log(msg % args, details)  # pylint: disable=not-callable

    def data_key(self, key: str) -> str:
        return self.resource_tag + self.data_tag + key

    def signal_key(self, key: str) -> str:
        return self.resource_tag + self.signal_tag + key

    def lock_key(self, key: str) -> str:
        return self.resource_tag + self.lock_tag + key

    async def maybe_in_background(self, coro: Awaitable) -> Tuple[bool, Any]:
        if not self.enable_background_tasks:
            return False, await coro
        # TODO: wrap coro in extra management (e.g. result logging)
        return True, asyncio.create_task(coro)

    async def get_data(
            self, cli: Redis, key: str, self_id: str,
            cm_stack: AsyncExitStack,
    ) -> Tuple[ReqResultInternal, Optional[bytes], Optional[SubscriptionElements]]:
        # TODO: support a timeout for this whole function, with
        # force_without_cache / force_without_lock situation result.

        subscription: Optional[SubscriptionElements] = None

        lock_key = self.lock_key(key)
        data_key = self.data_key(key)
        lock_ttl_sec = self.lock_ttl_sec

        req_script = self.req_script_cls(cli=cli)

        self._log('Calling req_script', key=key, self_id=self_id, lock_ttl_sec=lock_ttl_sec)
        situation, result = await req_script(
            lock_key=lock_key, data_key=data_key,
            self_id=self_id, lock_ttl_sec=lock_ttl_sec,
        )

        if situation == self.req_script_situation.lock_wait:
            self._log('Subscribing to notify channel (lock_wait)', key=key, self_id=self_id)
            signal_key = self.signal_key(key)
            subscription = await SubscriptionElements.create(
                cm_stack=cm_stack,
                client_acm=self.client_acm,
                channel_key=signal_key,
            )

            # In case the result appeared between first `get` and `psubscribe`, check for it again.
            situation, result = await req_script(
                lock_key=lock_key, data_key=data_key,
                self_id=self_id, lock_ttl_sec=lock_ttl_sec,
            )
            if situation != self.req_script_situation.lock_wait:
                self._log('Situation changed while subscribing', key=key, self_id=self_id)
                # Not waiting anymore, stop the subscription.
                await self.maybe_in_background(subscription.close())
                subscription = None

        internal_situation = self.req_situation(situation.value)
        self._log('Situation from get_data: %r', internal_situation, key=key, self_id=self_id)
        return internal_situation, result, subscription

    async def wait_for_result(
            self,
            key: str,
            sub: SubscriptionElements,
    ) -> Tuple[ReqResultInternal, Optional[bytes]]:
        # Lock should be renewed more often than `lock_ttl_sec`,
        # so waiting for the ttl duration should be sufficient.
        poll_timeout = self.channel_poll_timeout or self.lock_ttl_sec
        self._log('Waiting for signal', key=key, poll_timeout=poll_timeout)

        try:
            while True:
                message = await sub.get(timeout=poll_timeout)

                if message is None:
                    return self.channel_poll_timeout_situation, None

                if message.startswith(self.chan_data_prefix):
                    data = message[len(self.chan_data_prefix):]
                    return self.req_situation.cache_hit, data

                if message.startswith(self.chan_alive_prefix):
                    self._log(
                        'Alive signal: %r',
                        message, key=key, poll_timeout=poll_timeout)
                    continue

                if message.startswith(self.chan_fail_prefix):
                    self._log(
                        'Fail signal: %r',
                        message, key=key, poll_timeout=poll_timeout)
                    return self.channel_fail_situation, None

                self._log(
                    'Unexpected signal message: %r',
                    message, key=key, poll_timeout=poll_timeout)

        finally:
            await self.maybe_in_background(sub.close())

        raise Exception('Programming Error')

    async def get_data_full(
            self, cli: Redis, key: str, self_id: str,
            cm_stack: AsyncExitStack,
    ) -> Tuple[ReqResultInternal, Optional[bytes]]:
        # TODO: support `get_data_full` timeout.

        situation, result, subscription = await self.get_data(
            cli=cli, key=key, self_id=self_id, cm_stack=cm_stack,
        )

        if situation == self.req_situation.lock_wait:
            # At this point, the `sub_client` should already be subscribed
            # without any race conditions.
            assert subscription is not None
            situation, result = await self.wait_for_result(key=key, sub=subscription)

        # Can fall through to either `cache_hit` or whichever
        # `self.channel_poll_timeout_situation` defines.
        return situation, result

    async def renew_lock(self, cli: Redis, key: str, self_id: str) -> int:
        lock_ttl_sec = self.lock_ttl_sec
        self._log('Calling renew_script', key=key, self_id=self_id, lock_ttl_sec=lock_ttl_sec)
        renew_script = self.renew_script_cls(cli=cli)
        return await renew_script(
            lock_key=self.lock_key(key), signal_key=self.signal_key(key),
            self_id=self_id, lock_ttl_sec=lock_ttl_sec)

    async def lock_pinger(self, cli: Redis, key: str, self_id: str) -> None:
        lock_ttl_sec = self.lock_ttl_sec
        renew_interval = self.lock_renew_interval or lock_ttl_sec * 0.5
        while True:  # until cancelled, really
            await asyncio.sleep(renew_interval)
            try:
                renew_res = await self.renew_lock(cli=cli, key=key, self_id=self_id)
                # TODO: should cancel the parent task if the result is non-okay.
                self._log(
                    'renew script result: %r', renew_res,
                    key=key, self_id=self_id, renew_interval=renew_interval)
            except Exception as err:  # pylint: disable=broad-except
                self._log(
                    'lock_pinger error: %r', err,
                    key=key, self_id=self_id, renew_interval=renew_interval)

    async def generate_data_straight(self, generate_func: TGenerateFunc) -> TGenerateResult:
        """ Non-ping version """
        self._log('Calling generate_func without lock-ping')
        return await generate_func()

    async def generate_data_with_ping(
            self, generate_func: TGenerateFunc,
            cli: Redis, key: str, self_id: str,
    ) -> Tuple[bytes, _GF_RET_TV]:
        async with task_cm(self.lock_pinger(cli=cli, key=key, self_id=self_id)):
            return await generate_func()

    async def save_data(self, cli: Redis, key: str, self_id: str, data: bytes) -> None:
        self._log('Calling save_script', key=key, self_id=self_id, data_len=len(data))
        save_script = self.save_script_cls(cli=cli)
        await save_script(
            lock_key=self.lock_key(key),
            signal_key=self.signal_key(key),
            data_key=self.data_key(key),
            self_id=self_id,
            data=data,
            data_ttl_sec=self.data_ttl_sec,
        )

    async def force_save_data(self, cli: Redis, key: str, data: bytes) -> None:
        self._log('Calling force_save_script', key=key, data_len=len(data))
        force_save_script = self.force_save_script_cls(cli=cli)
        await force_save_script(
            signal_key=self.signal_key(key),
            data_key=self.data_key(key),
            data=data,
            data_ttl_sec=self.data_ttl_sec,
        )

    async def generate_with_lock(self, key: str, generate_func: TGenerateFunc) -> TCacheResult:
        self_id = self.make_self_id()

        cm_stack = AsyncExitStack()
        async with cm_stack:
            client = await cm_stack.enter_async_context(self.client_acm())
            situation, result = await self.get_data_full(
                cli=client, key=key, self_id=self_id, cm_stack=cm_stack,
            )

            if situation == self.req_situation.cache_hit:
                assert result is not None
                return result, None

            if situation == self.req_situation.force_without_cache:
                # special case for subclasses: allow to force data retrieval
                # ignoring the cache.
                assert result is None
                data, straight_data = await self.generate_data_straight(generate_func)
                return data, straight_data

            if situation == self.req_situation.force_without_lock:
                # special case for subclasses: allow to force data retrieval
                # ignoring the cache lock (but saving the cache).
                assert result is None
                data, straight_data = await self.generate_data_straight(generate_func)
                await self.maybe_in_background(self.force_save_data(
                    cli=client, key=key, data=data,
                ))
                return data, straight_data

            if situation == self.req_situation.successfully_locked:
                assert result is None
                data, straight_data = await self.generate_data_with_ping(
                    generate_func=generate_func,
                    cli=client, key=key, self_id=self_id,
                )
                await self.maybe_in_background(self.save_data(
                    cli=client, key=key, self_id=self_id, data=data,
                ))
                return data, straight_data

        raise Exception('RedisCacheLock error: Completely unexpected get_data outcome')
