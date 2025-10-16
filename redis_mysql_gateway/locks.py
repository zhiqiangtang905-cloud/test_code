# -*- coding: utf-8 -*-
import json
import threading
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import timedelta
from typing import Optional

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .db import utc_now
from .models import RedisGatewayInfo


@dataclass
class LockToken:
    name: str
    token: str


class DistributedLock:
    """
    分布式锁，基于 redis 优先，降级到 MySQL：
    - Redis 使用 SET NX EX 语义，值为 token
    - MySQL 使用 redis_gateway_info 表，行：key='__lock__', name=lock_name, value={'token': token}
    """

    def __init__(self, redis_client, get_db_session, instance_id: Optional[str] = None):
        self.redis = redis_client
        self._get_db_session = get_db_session
        self.instance_id = instance_id or str(uuid.uuid4())

    def acquire(self, name: str, ttl_seconds: int, blocking: bool = True, retry_interval: float = 0.2) -> Optional[LockToken]:
        import time
        token = str(uuid.uuid4())
        lock_key = f"lock:{name}"

        while True:
            if self.redis.is_available:
                try:
                    ok = self.redis._client.set(lock_key, token, nx=True, ex=ttl_seconds)
                    if ok:
                        return LockToken(name=name, token=token)
                except Exception:
                    # 标记不可用，后续降级到 MySQL
                    self.redis.mark_unavailable()

            # MySQL 降级
            got = self._acquire_mysql(name, token, ttl_seconds)
            if got:
                return LockToken(name=name, token=token)

            if not blocking:
                return None
            time.sleep(retry_interval)

    def _acquire_mysql(self, name: str, token: str, ttl_seconds: int) -> bool:
        now = utc_now()
        expire_time = now + timedelta(seconds=ttl_seconds)
        with self._get_db_session() as session:
            # 尝试插入（不存在则成功）
            try:
                row = RedisGatewayInfo(
                    key="__lock__",
                    name=name,
                    value=json.dumps({"token": token}),
                    expire_time=expire_time,
                )
                session.add(row)
                session.flush()
                return True
            except IntegrityError:
                session.rollback()
                # 存在则尝试抢占过期锁
                return self._steal_expired_mysql(session, name, token, expire_time)

    @staticmethod
    def _steal_expired_mysql(session: Session, name: str, token: str, new_expire_time) -> bool:
        # for update 锁住行，检查是否过期
        row = session.execute(
            select(RedisGatewayInfo).where(
                (RedisGatewayInfo.key == "__lock__") & (RedisGatewayInfo.name == name)
            ).with_for_update()
        ).scalar_one_or_none()
        now = utc_now()
        if row is None:
            # 再次插入
            try:
                row = RedisGatewayInfo(
                    key="__lock__",
                    name=name,
                    value=json.dumps({"token": token}),
                    expire_time=new_expire_time,
                )
                session.add(row)
                session.flush()
                return True
            except IntegrityError:
                session.rollback()
                return False
        # 判断是否过期
        if row.expire_time is None or row.expire_time > now:
            return False
        # 抢占
        row.value = json.dumps({"token": token})
        row.expire_time = new_expire_time
        session.flush()
        return True

    def release(self, lock: LockToken) -> bool:
        lock_key = f"lock:{lock.name}"
        # Redis 优先，需比较 token
        if self.redis.is_available:
            try:
                # 简单保证：只有持有者才能释放
                cur = self.redis._client.get(lock_key)
                if cur and cur == lock.token:
                    return bool(self.redis._client.delete(lock_key))
            except Exception:
                self.redis.mark_unavailable()
        # MySQL 降级
        with self._get_db_session() as session:
            row = session.execute(
                select(RedisGatewayInfo).where(
                    (RedisGatewayInfo.key == "__lock__") & (RedisGatewayInfo.name == lock.name)
                ).with_for_update()
            ).scalar_one_or_none()
            if row is None:
                return False
            try:
                cur = json.loads(row.value) if row.value else {}
            except Exception:
                cur = {}
            if cur.get("token") != lock.token:
                return False
            session.delete(row)
            session.flush()
            return True

    @contextmanager
    def context(self, name: str, ttl_seconds: int, blocking: bool = True, retry_interval: float = 0.2):
        """上下文锁：with lock.context("job", 30): ..."""
        lk = self.acquire(name, ttl_seconds, blocking=blocking, retry_interval=retry_interval)
        if lk is None:
            raise TimeoutError(f"Failed to acquire lock: {name}")
        try:
            yield lk
        finally:
            self.release(lk)


class DistributedSemaphore:
    """
    分布式信号量（计数信号量），Redis 优先，MySQL 降级：
    - Redis：使用 ZSET 的常见实现（token = uuid, score = 过期时间戳）
    - MySQL：使用单行 key='__semaphore__', name=sem_name，value= {max, holders:[{token, expire_ts}]}
    """

    def __init__(self, redis_client, get_db_session, instance_id: Optional[str] = None):
        self.redis = redis_client
        self._get_db_session = get_db_session
        self.instance_id = instance_id or str(uuid.uuid4())

    def acquire(self, name: str, limit: int, ttl_seconds: int, token: Optional[str] = None) -> Optional[str]:
        token = token or str(uuid.uuid4())
        if self.redis.is_available:
            try:
                key = f"sema:{name}"
                pipe = self.redis._client.pipeline(True)
                now_ms = int(utc_now().timestamp() * 1000)
                expire_score = now_ms + ttl_seconds * 1000
                # 清理过期
                pipe.zremrangebyscore(key, 0, now_ms)
                # 添加当前 token
                pipe.zadd(key, {token: expire_score})
                # 检查排名/持有数量
                pipe.zcard(key)
                pipe.expire(key, ttl_seconds)
                _, _, cnt, _ = pipe.execute()
                if cnt <= limit:
                    return token
                else:
                    # 超限，移除自己
                    self.redis._client.zrem(key, token)
            except Exception:
                self.redis.mark_unavailable()
        # MySQL 降级
        return self._acquire_mysql(name, limit, ttl_seconds, token)

    def _acquire_mysql(self, name: str, limit: int, ttl_seconds: int, token: str) -> Optional[str]:
        from sqlalchemy import select
        now_ts = int(utc_now().timestamp())
        expire_ts = now_ts + ttl_seconds
        with self._get_db_session() as session:
            row = session.execute(
                select(RedisGatewayInfo).where(
                    (RedisGatewayInfo.key == "__semaphore__") & (RedisGatewayInfo.name == name)
                ).with_for_update()
            ).scalar_one_or_none()
            if row is None:
                holders = [{"token": token, "expire_ts": expire_ts}]
                row = RedisGatewayInfo(
                    key="__semaphore__",
                    name=name,
                    value=json.dumps({"max": limit, "holders": holders}),
                    expire_time=None,
                )
                session.add(row)
                session.flush()
                return token
            try:
                data = json.loads(row.value or "{}")
            except Exception:
                data = {}
            max_permits = int(data.get("max", limit))
            holders = [h for h in data.get("holders", []) if int(h.get("expire_ts", 0)) > now_ts]
            if len(holders) < max_permits:
                holders.append({"token": token, "expire_ts": expire_ts})
                row.value = json.dumps({"max": max_permits, "holders": holders})
                session.flush()
                return token
            return None

    def release(self, name: str, token: str) -> bool:
        if self.redis.is_available:
            try:
                key = f"sema:{name}"
                return bool(self.redis._client.zrem(key, token))
            except Exception:
                self.redis.mark_unavailable()
        # MySQL 降级
        with self._get_db_session() as session:
            row = session.execute(
                select(RedisGatewayInfo).where(
                    (RedisGatewayInfo.key == "__semaphore__") & (RedisGatewayInfo.name == name)
                ).with_for_update()
            ).scalar_one_or_none()
            if row is None:
                return False
            try:
                data = json.loads(row.value or "{}")
            except Exception:
                data = {}
            holders = [h for h in data.get("holders", []) if h.get("token") != token]
            data["holders"] = holders
            row.value = json.dumps(data)
            session.flush()
            return True
