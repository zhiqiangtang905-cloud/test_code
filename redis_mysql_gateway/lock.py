from __future__ import annotations
"""Cross-backend distributed lock and counting semaphore.

- DistributedLock: Context-managed lock that prefers Redis and falls back to MySQL.
- CountingSemaphore: Simple counting semaphore backed by Redis with MySQL fallback.

The MySQL fallback uses the shared table `redis_gateway_info` with the following
conventions:
- Lock rows: name = '__lock__:<lock_name>', key = 'token', value = JSON string of the token
- Semaphore rows: name = '__sema__:<sema_name>', key = 'count', value = JSON int string

Both use `expire_time` to implement TTL and auto-recovery of stale holders.
"""

import json
import secrets
import threading
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from redis import Redis
from redis.exceptions import RedisError
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .config import INSTANCE_ID
from .db import RedisGatewayInfo, get_session


_LOCK_RELEASE_LUA = """
-- Release lock only if value matches our token
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('del', KEYS[1])
else
  return 0
end
"""


@dataclass
class DistributedLock(AbstractContextManager):
    """A Redis-style distributed lock with MySQL fallback.

    Usage:
        with DistributedLock(redis_client, 'my-lock', ttl_seconds=30).acquire(blocking=True):
            ...

    This lock is process-safe and cross-instance when using the shared DB fallback.
    """

    redis_client: Optional[Redis]
    name: str
    ttl_seconds: int = 30
    blocking: bool = True
    timeout_seconds: Optional[float] = None

    def __post_init__(self) -> None:
        self._token = f"{INSTANCE_ID}:{secrets.token_urlsafe(16)}"
        self._acquired = False
        self._start_time: Optional[float] = None
        self._lock_key = f"__lock__:{self.name}"
        self._release_script = None
        if self.redis_client is not None:
            try:
                self._release_script = self.redis_client.register_script(_LOCK_RELEASE_LUA)
            except Exception:
                self._release_script = None
        self._local = threading.local()

    # Context manager API ---------------------------------------------------
    def __enter__(self):
        self.acquire(self.blocking, self.timeout_seconds)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._acquired:
            try:
                self.release()
            finally:
                self._acquired = False

    # Public methods --------------------------------------------------------
    def acquire(self, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        """Try to acquire the lock using Redis first, then MySQL fallback."""
        deadline = None if timeout is None else (datetime.now().timestamp() + timeout)
        while True:
            # Try Redis
            if self.redis_client is not None:
                try:
                    if self.redis_client.set(self._lock_key, self._token, nx=True, ex=self.ttl_seconds):
                        self._acquired = True
                        return True
                except RedisError:
                    # Redis error: ignore and try MySQL fallback
                    pass

            # MySQL fallback
            if self._try_acquire_mysql():
                self._acquired = True
                return True

            if not blocking:
                return False

            if deadline is not None and datetime.now().timestamp() >= deadline:
                return False

            # Short sleep to avoid hot spinning
            threading.Event().wait(0.1)

    def release(self) -> None:
        """Release the lock in Redis or MySQL, matching our token only."""
        # Redis release
        if self.redis_client is not None:
            try:
                if self._release_script is not None:
                    self._release_script(keys=[self._lock_key], args=[self._token])
                else:
                    # Fallback: non-atomic compare-and-del
                    current = self.redis_client.get(self._lock_key)
                    if current is not None and current.decode() == self._token:
                        self.redis_client.delete(self._lock_key)
            except RedisError:
                # Ignore on Redis error; we'll still clear MySQL fallback
                pass

        # MySQL release
        self._release_mysql()

    def renew(self, additional_ttl_seconds: Optional[int] = None) -> None:
        """Extend the lock TTL (best effort)."""
        ttl = additional_ttl_seconds or self.ttl_seconds
        # Redis renew
        if self.redis_client is not None:
            try:
                # Redis has no direct expire-if-value-equals; do best effort
                self.redis_client.expire(self._lock_key, ttl)
            except RedisError:
                pass
        # MySQL renew
        now = datetime.now(timezone.utc)
        with get_session() as session:
            row = (
                session.query(RedisGatewayInfo)
                .with_for_update()
                .filter(
                    RedisGatewayInfo.name == self._lock_key,
                    RedisGatewayInfo.key == "token",
                    RedisGatewayInfo.value == json.dumps(self._token),
                )
                .one_or_none()
            )
            if row is not None:
                row.expire_time = now + timedelta(seconds=ttl)
                row.last_update_time = now

    # Internal helpers ------------------------------------------------------
    def _try_acquire_mysql(self) -> bool:
        now = datetime.now(timezone.utc)
        expire_at = now + timedelta(seconds=self.ttl_seconds)
        token_json = json.dumps(self._token)
        with get_session() as session:
            # Try insert first (fast path)
            try:
                new_row = RedisGatewayInfo(
                    name=self._lock_key,
                    key="token",
                    value=token_json,
                    expire_time=expire_at,
                    last_update_time=now,
                )
                session.add(new_row)
                session.flush()  # may raise IntegrityError
                return True
            except IntegrityError:
                session.rollback()
                # Row exists -> check if expired and then try to take over
                row = (
                    session.query(RedisGatewayInfo)
                    .with_for_update()
                    .filter(
                        RedisGatewayInfo.name == self._lock_key,
                        RedisGatewayInfo.key == "token",
                    )
                    .one_or_none()
                )
                if row is None:
                    return False
                if row.expire_time is None or row.expire_time <= now:
                    row.value = token_json
                    row.expire_time = expire_at
                    row.last_update_time = now
                    return True
                return False

    def _release_mysql(self) -> None:
        now = datetime.now(timezone.utc)
        with get_session() as session:
            row = (
                session.query(RedisGatewayInfo)
                .with_for_update()
                .filter(
                    RedisGatewayInfo.name == self._lock_key,
                    RedisGatewayInfo.key == "token",
                )
                .one_or_none()
            )
            if row is not None:
                # Release only if we're the owner
                if row.value == json.dumps(self._token):
                    session.delete(row)
                else:
                    # If token mismatches, we do nothing (someone else took over)
                    pass


class CountingSemaphore:
    """A cross-backend counting semaphore.

    Redis implementation uses a Lua script for atomic check-decrement/increment-with-cap.
    MySQL fallback uses row-level locking with (name='__sema__:<name>', key='count').
    """

    def __init__(self, redis_client: Optional[Redis], name: str, max_permits: int):
        if max_permits <= 0:
            raise ValueError("max_permits must be > 0")
        self.redis_client = redis_client
        self.name = name
        self.max_permits = max_permits
        self._redis_key = f"__sema__:{name}:count"
        self._db_name = f"__sema__:{name}"
        self._acquire_lua = None
        self._release_lua = None
        if self.redis_client is not None:
            try:
                self._acquire_lua = self.redis_client.register_script(
                    """
                    local k = KEYS[1]
                    local maxp = tonumber(ARGV[1])
                    local v = redis.call('get', k)
                    if not v then
                        redis.call('set', k, tostring(maxp))
                        v = tostring(maxp)
                    end
                    local cur = tonumber(v)
                    if cur > 0 then
                        cur = cur - 1
                        redis.call('set', k, tostring(cur))
                        return 1
                    else
                        return 0
                    end
                    """
                )
                self._release_lua = self.redis_client.register_script(
                    """
                    local k = KEYS[1]
                    local maxp = tonumber(ARGV[1])
                    local v = redis.call('get', k)
                    if not v then
                        redis.call('set', k, '1')
                        return 1
                    end
                    local cur = tonumber(v)
                    if cur < maxp then
                        cur = cur + 1
                        redis.call('set', k, tostring(cur))
                        return 1
                    else
                        return 0
                    end
                    """
                )
            except Exception:
                self._acquire_lua = None
                self._release_lua = None

    def acquire(self, blocking: bool = True, timeout_seconds: Optional[float] = None) -> bool:
        deadline = None if timeout_seconds is None else (datetime.now().timestamp() + timeout_seconds)
        while True:
            # Try Redis
            if self.redis_client is not None:
                try:
                    if self._acquire_lua and self._acquire_lua(keys=[self._redis_key], args=[str(self.max_permits)]) == 1:
                        return True
                except RedisError:
                    pass

            # MySQL fallback
            if self._try_acquire_mysql():
                return True

            if not blocking:
                return False
            if deadline is not None and datetime.now().timestamp() >= deadline:
                return False
            threading.Event().wait(0.1)

    def release(self) -> bool:
        # Redis first
        if self.redis_client is not None:
            try:
                if self._release_lua and self._release_lua(keys=[self._redis_key], args=[str(self.max_permits)]) == 1:
                    return True
            except RedisError:
                pass
        # MySQL fallback
        return self._try_release_mysql()

    # MySQL helpers ---------------------------------------------------------
    def _try_acquire_mysql(self) -> bool:
        now = datetime.now(timezone.utc)
        with get_session() as session:
            row = (
                session.query(RedisGatewayInfo)
                .with_for_update()
                .filter(
                    RedisGatewayInfo.name == self._db_name,
                    RedisGatewayInfo.key == "count",
                )
                .one_or_none()
            )
            if row is None:
                row = RedisGatewayInfo(
                    name=self._db_name,
                    key="count",
                    value=json.dumps(self.max_permits - 1),
                    expire_time=None,
                    last_update_time=now,
                )
                session.add(row)
                return True
            else:
                cur = int(json.loads(row.value)) if row.value is not None else 0
                if cur > 0:
                    row.value = json.dumps(cur - 1)
                    row.last_update_time = now
                    return True
                return False

    def _try_release_mysql(self) -> bool:
        now = datetime.now(timezone.utc)
        with get_session() as session:
            row = (
                session.query(RedisGatewayInfo)
                .with_for_update()
                .filter(
                    RedisGatewayInfo.name == self._db_name,
                    RedisGatewayInfo.key == "count",
                )
                .one_or_none()
            )
            if row is None:
                # initialize at 1
                session.add(
                    RedisGatewayInfo(
                        name=self._db_name,
                        key="count",
                        value=json.dumps(1),
                        expire_time=None,
                        last_update_time=now,
                    )
                )
                return True
            cur = int(json.loads(row.value)) if row.value is not None else 0
            if cur < self.max_permits:
                row.value = json.dumps(cur + 1)
                row.last_update_time = now
                return True
            return False
