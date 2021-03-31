from __future__ import annotations


class CacheError(Exception):
    """ Common base exception """


class CacheRedisError(CacheError):
    """ Redis communication failure """


class CacheLockLost(CacheError):
    """ Previously owned lock is not owned anymore, abort """


# class CacheTimeoutError(CacheError):
#     """ Some waiting failed within the time limit """
