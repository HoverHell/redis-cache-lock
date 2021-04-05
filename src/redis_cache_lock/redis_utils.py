from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, AsyncGenerator, Optional, Tuple

import attr

from .utils import PreExitable

if TYPE_CHECKING:
    from contextlib import AsyncExitStack  # pylint: disable=ungrouped-imports

    from aioredis import Channel, Redis

    from .types import TClientACM


@attr.s(auto_attribs=True, frozen=True, slots=True)
class SubscriptionManager:
    """ Helper to manage a redis channel subscription """

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
            try:
                await cli.punsubscribe(*channels)
            except Exception:  # pylint: disable=broad-except
                cli.close()  # mark as invalid if anything suspicious happens

    @classmethod
    async def create(
            cls,
            cm_stack: AsyncExitStack,
            client_acm: TClientACM,
            channel_key: str,
    ) -> SubscriptionManager:
        cli_cm = PreExitable(client_acm(master=True, exclusive=True))
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

    async def get_direct(self) -> Optional[bytes]:
        # returns a `channel_key, message_data` tuple.
        try:
            item = await self.channel.get()
        except Exception:  # pylint: disable=broad-except
            # doesn't really matter which exception,
            # although this can often be `aioredis.errors.ChannelClosedError`.
            item = None

        if item is None:
            return None

        _, message = item
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
