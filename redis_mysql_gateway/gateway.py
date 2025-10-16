from __future__ import annotations
"""Redis operations with MySQL fallback and async persistence.

Supported operations (write path writes Redis then asynchronously persists to MySQL):
- set_ex(key, value, ttl)
- set_ex(key, name, ttl=20) [alias: set_kv_ex]
- rpush(name, val)
- lpush(name, val)
- hget(name, key)
- hset(name, key, value)
- hdel(name, key)
- lrange(name, start=0, end=-1)
- exists(key)
- get(key)
- lrem(key, count, value)
- delete(key)
- expire(key, ttl)
- lpop(key, count)
- lindex(key, index)

Additionally includes:
- Redis context distributed lock wrapper via lock module
- Redis-backed counting semaphore via lock module

Data encoding:
- All payloads written to Redis and MySQL are json.dumps'ed strings
- Reads json.loads back to Python objects

MySQL storage mapping:
- KV: name='_kv_', key=<key>, value=<json string>
- Hash: name=<hash_name>, key=<field>, value=<json string>
- List: name=<list_name>, key='__list__', value=<json array string>

Concurrency/uniqueness across instances:
- Use DistributedLock to guard jobs that must run once across instances
"""

import json
import logging
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, List, Optional, Tuple

from redis import Redis
from redis.exceptions import RedisError

from .db import RedisGatewayInfo, get_session
from .lock import DistributedLock, CountingSemaphore

logger = logging.getLogger(__name__)


@dataclass
class RedisState:
    available: bool = True
    probing: bool = False


class RedisMySQLGateway:
    def __init__(self, redis_client: Redis, redis_state: Optional[RedisState] = None):
        self.redis = redis_client
        self.state = redis_state or RedisState()
        self._bg = _BackgroundPersistence()

    # ------------- Health hooks -------------
    def mark_redis_unavailable(self) -> None:
        # Mark Redis as unavailable and signal probing should be active
        self.state.available = False
        self.state.probing = True

    def mark_redis_available(self) -> None:
        # Mark Redis as available and stop probing
        self.state.available = True
        self.state.probing = False

    # ------------- Public API -------------
    # KV
    def set(self, key: str, value: Any) -> bool:
        """Set a KV without TTL. Writes Redis first (if available), then async persist to MySQL.

        All values are json.dumps'ed strings before storing.
        """
        payload = json.dumps(value)
        try:
            if self.state.available:
                self.redis.set(name=key, value=payload)
        except RedisError:
            self.mark_redis_unavailable()
        # Async persist without expire
        self._bg.persist_kv(key, payload, ttl=0)
        return True

    def set_ex(self, key: str, value: Any, ttl: int) -> bool:
        payload = json.dumps(value)
        try:
            if self.state.available:
                self.redis.set(name=key, value=payload, ex=ttl)
        except RedisError:
            self.mark_redis_unavailable()
        # persist async
        self._bg.persist_kv(key, payload, ttl)
        return True

    def set_kv_ex(self, key: str, name: str, ttl: int = 20) -> bool:
        # Interpret as: store name under key with TTL 20
        return self.set_ex(key, name, ttl)

    def get(self, key: str) -> Optional[Any]:
        try:
            if self.state.available:
                data = self.redis.get(key)
                if data is not None:
                    return json.loads(data)
        except RedisError:
            self.mark_redis_unavailable()
        # Fallback to MySQL
        self._bg.cleanup_expired_sync()
        with get_session() as session:
            row = (
                session.query(RedisGatewayInfo)
                .filter(
                    RedisGatewayInfo.name == "_kv_",
                    RedisGatewayInfo.key == key,
                )
                .one_or_none()
            )
            if row is None:
                return None
            return json.loads(row.value) if row.value is not None else None

    def exists(self, key: str) -> bool:
        try:
            if self.state.available:
                return bool(self.redis.exists(key))
        except RedisError:
            self.mark_redis_unavailable()
        # fallback
        self._bg.cleanup_expired_sync()
        with get_session() as session:
            # exists if KV row or any row with name == key (hash fields or list row)
            kv_exists = (
                session.query(RedisGatewayInfo)
                .filter(RedisGatewayInfo.name == "_kv_", RedisGatewayInfo.key == key)
                .count()
                > 0
            )
            if kv_exists:
                return True
            other_exists = (
                session.query(RedisGatewayInfo)
                .filter(RedisGatewayInfo.name == key)
                .count()
                > 0
            )
            return other_exists

    def delete(self, key: str) -> int:
        deleted = 0
        try:
            if self.state.available:
                deleted = int(self.redis.delete(key))
        except RedisError:
            self.mark_redis_unavailable()
        # persist across all types
        self._bg.persist_delete_key(key)
        return deleted

    def expire(self, key: str, ttl: int) -> bool:
        ok = True
        try:
            if self.state.available:
                ok = bool(self.redis.expire(key, ttl))
        except RedisError:
            self.mark_redis_unavailable()
        self._bg.persist_expire_key(key, ttl)
        return ok

    # Hash
    def hset(self, name: str, key: str, value: Any) -> int:
        payload = json.dumps(value)
        try:
            if self.state.available:
                return int(self.redis.hset(name=name, key=key, value=payload))
        except RedisError:
            self.mark_redis_unavailable()
        self._bg.persist_hash_field(name, key, payload)
        return 1

    def hget(self, name: str, key: str) -> Optional[Any]:
        try:
            if self.state.available:
                data = self.redis.hget(name=name, key=key)
                if data is not None:
                    return json.loads(data)
        except RedisError:
            self.mark_redis_unavailable()
        # Fallback to MySQL with cleanup
        self._bg.cleanup_expired_sync()
        with get_session() as session:
            row = (
                session.query(RedisGatewayInfo)
                .filter(
                    RedisGatewayInfo.name == name,
                    RedisGatewayInfo.key == key,
                )
                .one_or_none()
            )
            if row is None:
                return None
            return json.loads(row.value) if row.value is not None else None

    def hdel(self, name: str, key: str) -> int:
        deleted = 0
        try:
            if self.state.available:
                # redis-py: hdel(name, *keys)
                deleted = int(self.redis.hdel(name, key))
        except RedisError:
            self.mark_redis_unavailable()
        self._bg.persist_hash_delete(name, key)
        return deleted

    # List
    def _get_list_mysql(self, name: str) -> List[Any]:
        with get_session() as session:
            row = (
                session.query(RedisGatewayInfo)
                .filter(
                    RedisGatewayInfo.name == name,
                    RedisGatewayInfo.key == "__list__",
                )
                .one_or_none()
            )
            if row is None or row.value is None:
                return []
            try:
                return json.loads(row.value)
            except Exception:
                return []

    def _persist_list_mysql(self, name: str, items: List[Any], ttl: Optional[int] = None) -> None:
        now = datetime.now(timezone.utc)
        expire_time = None if ttl is None else now + timedelta(seconds=ttl)
        payload = json.dumps(items)
        with get_session() as session:
            row = (
                session.query(RedisGatewayInfo)
                .with_for_update()
                .filter(
                    RedisGatewayInfo.name == name,
                    RedisGatewayInfo.key == "__list__",
                )
                .one_or_none()
            )
            if row is None:
                session.add(
                    RedisGatewayInfo(
                        name=name,
                        key="__list__",
                        value=payload,
                        expire_time=expire_time,
                        last_update_time=now,
                    )
                )
            else:
                row.value = payload
                row.expire_time = expire_time
                row.last_update_time = now

    def rpush(self, name: str, val: Any) -> int:
        payload = json.dumps(val)
        new_len = None
        try:
            if self.state.available:
                new_len = int(self.redis.rpush(name, payload))
        except RedisError:
            self.mark_redis_unavailable()
        # Async persist
        self._bg.persist_list_append(name=name, value=payload, left=False)
        return new_len if new_len is not None else -1

    def lpush(self, name: str, val: Any) -> int:
        payload = json.dumps(val)
        new_len = None
        try:
            if self.state.available:
                new_len = int(self.redis.lpush(name, payload))
        except RedisError:
            self.mark_redis_unavailable()
        self._bg.persist_list_append(name=name, value=payload, left=True)
        return new_len if new_len is not None else -1

    def lrange(self, name: str, start: int = 0, end: int = -1) -> List[Any]:
        try:
            if self.state.available:
                data = self.redis.lrange(name, start, end)
                return [json.loads(x) for x in data]
        except RedisError:
            self.mark_redis_unavailable()
        # Fallback
        items = self._get_list_mysql(name)
        # emulate slicing semantics
        n = len(items)
        if end == -1:
            end = n - 1
        start = max(0, start)
        end = min(n - 1, end)
        if start > end:
            return []
        return items[start : end + 1]

    def lrem(self, name: str, count: int, value: Any) -> int:
        payload = json.dumps(value)
        removed = 0
        try:
            if self.state.available:
                removed = int(self.redis.lrem(name, count, payload))
        except RedisError:
            self.mark_redis_unavailable()
        self._bg.persist_list_remove(name=name, payload=payload, count=count)
        return removed

    def lpop(self, key: str, count: int = 1) -> List[Any]:
        try:
            if self.state.available:
                data = self.redis.lpop(key, count)
                if data is None:
                    return []
                return [json.loads(x) for x in data] if isinstance(data, list) else [json.loads(data)]
        except RedisError:
            self.mark_redis_unavailable()
        # Fallback
        items = self._get_list_mysql(key)
        popped = items[:count]
        self._bg.persist_list_set(name=key, items=items[count:])
        return popped

    def lindex(self, key: str, index: int) -> Optional[Any]:
        try:
            if self.state.available:
                data = self.redis.lindex(key, index)
                return json.loads(data) if data is not None else None
        except RedisError:
            self.mark_redis_unavailable()
        items = self._get_list_mysql(key)
        if -len(items) <= index < len(items):
            return items[index]
        return None

    # Helpers for MySQL cleanup --------------------------------------------
    def cleanup_expired(self) -> int:
        now = datetime.now(timezone.utc)
        with get_session() as session:
            q = (
                session.query(RedisGatewayInfo)
                .filter(RedisGatewayInfo.expire_time != None, RedisGatewayInfo.expire_time <= now)
            )
            # count before delete
            count = q.count()
            for row in q:
                session.delete(row)
            return count


# ---------------- Background persistence implementation -------------------
class _BackgroundPersistence:
    """Performs async MySQL persistence using a single background worker thread."""

    def __init__(self) -> None:
        self._queue = []  # list of callables
        self._cv = threading.Condition()
        self._worker = threading.Thread(target=self._run, name="mysql-persist-worker", daemon=True)
        self._worker.start()

    def _submit(self, fn) -> None:
        with self._cv:
            self._queue.append(fn)
            self._cv.notify()

    def _run(self) -> None:
        while True:
            with self._cv:
                while not self._queue:
                    self._cv.wait()
                fn = self._queue.pop(0)
            try:
                fn()
            except Exception as e:
                logger.exception("Async MySQL persist failed: %s", e)

    # --- Public enqueue helpers ---
    def persist_kv(self, key: str, payload: str, ttl: int) -> None:
        def _do():
            now = datetime.now(timezone.utc)
            expire_time = now + timedelta(seconds=ttl) if ttl else None
            with get_session() as session:
                row = (
                    session.query(RedisGatewayInfo)
                    .with_for_update()
                    .filter(RedisGatewayInfo.name == "_kv_", RedisGatewayInfo.key == key)
                    .one_or_none()
                )
                if row is None:
                    session.add(
                        RedisGatewayInfo(
                            name="_kv_",
                            key=key,
                            value=payload,
                            expire_time=expire_time,
                            last_update_time=now,
                        )
                    )
                else:
                    row.value = payload
                    row.expire_time = expire_time
                    row.last_update_time = now
        self._submit(_do)

    def persist_delete_kv(self, key: str) -> None:
        def _do():
            with get_session() as session:
                row = (
                    session.query(RedisGatewayInfo)
                    .with_for_update()
                    .filter(RedisGatewayInfo.name == "_kv_", RedisGatewayInfo.key == key)
                    .one_or_none()
                )
                if row is not None:
                    session.delete(row)
        self._submit(_do)

    def persist_delete_key(self, key: str) -> None:
        """Delete any rows associated with the given Redis key (KV, hash, list)."""
        def _do():
            with get_session() as session:
                # Delete KV row
                rows = (
                    session.query(RedisGatewayInfo)
                    .filter(
                        (RedisGatewayInfo.name == "_kv_") & (RedisGatewayInfo.key == key)
                        | (RedisGatewayInfo.name == key)
                    )
                    .all()
                )
                for r in rows:
                    session.delete(r)
        self._submit(_do)

    def persist_expire_kv(self, key: str, ttl: int) -> None:
        def _do():
            now = datetime.now(timezone.utc)
            expire_time = now + timedelta(seconds=ttl) if ttl else None
            with get_session() as session:
                row = (
                    session.query(RedisGatewayInfo)
                    .with_for_update()
                    .filter(RedisGatewayInfo.name == "_kv_", RedisGatewayInfo.key == key)
                    .one_or_none()
                )
                if row is not None:
                    row.expire_time = expire_time
                    row.last_update_time = now
        self._submit(_do)

    def persist_expire_key(self, key: str, ttl: int) -> None:
        """Set expire_time for any rows associated with the given Redis key.

        - KV mapping: (name='_kv_', key=<key>)
        - Hash mapping: (name=<key>, key=<field>) for all fields
        - List mapping: (name=<key>, key='__list__')
        """
        def _do():
            now = datetime.now(timezone.utc)
            expire_time = now + timedelta(seconds=ttl) if ttl else None
            with get_session() as session:
                rows = (
                    session.query(RedisGatewayInfo)
                    .with_for_update()
                    .filter(
                        (RedisGatewayInfo.name == "_kv_") & (RedisGatewayInfo.key == key)
                        | (RedisGatewayInfo.name == key)
                    )
                    .all()
                )
                for r in rows:
                    r.expire_time = expire_time
                    r.last_update_time = now
        self._submit(_do)

    def persist_hash_field(self, name: str, key: str, payload: str) -> None:
        def _do():
            now = datetime.now(timezone.utc)
            with get_session() as session:
                row = (
                    session.query(RedisGatewayInfo)
                    .with_for_update()
                    .filter(RedisGatewayInfo.name == name, RedisGatewayInfo.key == key)
                    .one_or_none()
                )
                if row is None:
                    session.add(
                        RedisGatewayInfo(
                            name=name,
                            key=key,
                            value=payload,
                            expire_time=None,
                            last_update_time=now,
                        )
                    )
                else:
                    row.value = payload
                    row.last_update_time = now
        self._submit(_do)

    def persist_hash_delete(self, name: str, key: str) -> None:
        def _do():
            with get_session() as session:
                row = (
                    session.query(RedisGatewayInfo)
                    .with_for_update()
                    .filter(RedisGatewayInfo.name == name, RedisGatewayInfo.key == key)
                    .one_or_none()
                )
                if row is not None:
                    session.delete(row)
        self._submit(_do)

    def persist_list_append(self, name: str, value: str, left: bool) -> None:
        def _do():
            # Perform read-modify-write within a single transaction for correctness
            now = datetime.now(timezone.utc)
            with get_session() as session:
                row = (
                    session.query(RedisGatewayInfo)
                    .with_for_update()
                    .filter(RedisGatewayInfo.name == name, RedisGatewayInfo.key == "__list__")
                    .one_or_none()
                )
                if row is None or row.value is None:
                    items: List[Any] = []
                    row = row  # no-op for clarity
                else:
                    try:
                        items = json.loads(row.value)
                    except Exception:
                        items = []
                obj = json.loads(value)
                if left:
                    items.insert(0, obj)
                else:
                    items.append(obj)
                payload = json.dumps(items)
                if row is None:
                    session.add(
                        RedisGatewayInfo(
                            name=name,
                            key="__list__",
                            value=payload,
                            expire_time=None,
                            last_update_time=now,
                        )
                    )
                else:
                    row.value = payload
                    row.last_update_time = now
        self._submit(_do)

    def persist_list_remove(self, name: str, payload: str, count: int) -> None:
        def _do():
            target = json.loads(payload)
            now = datetime.now(timezone.utc)
            with get_session() as session:
                row = (
                    session.query(RedisGatewayInfo)
                    .with_for_update()
                    .filter(RedisGatewayInfo.name == name, RedisGatewayInfo.key == "__list__")
                    .one_or_none()
                )
                if row is None or row.value is None:
                    items: List[Any] = []
                else:
                    try:
                        items = json.loads(row.value)
                    except Exception:
                        items = []
                removed = 0
                if count == 0:
                    items = [x for x in items if x != target]
                elif count > 0:
                    new_items = []
                    for x in items:
                        if x == target and removed < count:
                            removed += 1
                            continue
                        new_items.append(x)
                    items = new_items
                else:  # count < 0 remove from tail
                    reversed_items = []
                    for x in reversed(items):
                        if x == target and removed < -count:
                            removed += 1
                            continue
                        reversed_items.append(x)
                    items = list(reversed(reversed_items))
                payload_out = json.dumps(items)
                if row is None:
                    session.add(
                        RedisGatewayInfo(
                            name=name,
                            key="__list__",
                            value=payload_out,
                            expire_time=None,
                            last_update_time=now,
                        )
                    )
                else:
                    row.value = payload_out
                    row.last_update_time = now
        self._submit(_do)

    def persist_list_set(self, name: str, items: List[Any]) -> None:
        def _do():
            now = datetime.now(timezone.utc)
            payload = json.dumps(items)
            with get_session() as session:
                row = (
                    session.query(RedisGatewayInfo)
                    .with_for_update()
                    .filter(RedisGatewayInfo.name == name, RedisGatewayInfo.key == "__list__")
                    .one_or_none()
                )
                if row is None:
                    session.add(
                        RedisGatewayInfo(
                            name=name,
                            key="__list__",
                            value=payload,
                            expire_time=None,
                            last_update_time=now,
                        )
                    )
                else:
                    row.value = payload
                    row.last_update_time = now
        self._submit(_do)

    # Local helpers (sync within worker)
    def _get_list_mysql(self, name: str) -> List[Any]:
        with get_session() as session:
            row = (
                session.query(RedisGatewayInfo)
                .with_for_update()
                .filter(RedisGatewayInfo.name == name, RedisGatewayInfo.key == "__list__")
                .one_or_none()
            )
            if row is None or row.value is None:
                return []
            try:
                return json.loads(row.value)
            except Exception:
                return []

    def _persist_list_mysql(self, name: str, items: List[Any]) -> None:
        now = datetime.now(timezone.utc)
        payload = json.dumps(items)
        with get_session() as session:
            row = (
                session.query(RedisGatewayInfo)
                .with_for_update()
                .filter(RedisGatewayInfo.name == name, RedisGatewayInfo.key == "__list__")
                .one_or_none()
            )
            if row is None:
                session.add(
                    RedisGatewayInfo(
                        name=name,
                        key="__list__",
                        value=payload,
                        expire_time=None,
                        last_update_time=now,
                    )
                )
            else:
                row.value = payload
                row.last_update_time = now

    # Cleanup helper exposed to gateway
    def cleanup_expired_sync(self) -> int:
        now = datetime.now(timezone.utc)
        with get_session() as session:
            q = (
                session.query(RedisGatewayInfo)
                .filter(RedisGatewayInfo.expire_time != None, RedisGatewayInfo.expire_time <= now)
            )
            count = q.count()
            for row in q:
                session.delete(row)
            return count
