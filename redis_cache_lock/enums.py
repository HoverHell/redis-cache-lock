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
    # *MUST* have all values of the `ReqScriptResult`
    cache_hit = 130
    successfully_locked = 131
    lock_wait = 132
    # ...
    force_without_cache = 233
    force_without_lock = 234


@unique
class RenewScriptResult(IntEnum):
    extended = 140
    expired = 141
    locked_by_another = 142


@unique
class SaveScriptResult(IntEnum):
    success = 150
    token_mismatch = 151
