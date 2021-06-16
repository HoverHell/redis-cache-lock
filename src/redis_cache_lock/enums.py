from __future__ import annotations

from enum import IntEnum, unique


@unique
class ReqScriptResultBase(IntEnum):
    pass


@unique
class ReqScriptResult(ReqScriptResultBase):
    cache_hit = 130
    successfully_locked = 131
    lock_wait = 132


@unique
class ReqResultInternal(ReqScriptResultBase):
    # Special values for internal processing:
    starting = 1  # A lock request might be in flight.
    requesting = 2  # A lock request might be in flight.
    awaiting = 3  # A pubsub subscription is being awaited.

    # *MUST* have all values of the `ReqScriptResult`
    cache_hit = 130
    successfully_locked = 131
    lock_wait = 132

    # Special situations:
    network_call_timeout = 335
    cache_hit_after_wait = 336
    lock_wait_timeout = 337
    lock_wait_unexpected_message = 338
    failure_signal = 339


# @unique
# class FinalizeAction(IntEnum):
#     # Special values for behavior customization:
#     force_without_cache = 233  # ignore cache completely (no saving either)
#     force_without_lock = 234  # ignore lock, force-save cache


@unique
class RenewScriptResult(IntEnum):
    extended = 140
    expired = 141
    locked_by_another = 142


@unique
class SaveScriptResult(IntEnum):
    success = 150
    token_mismatch = 151
