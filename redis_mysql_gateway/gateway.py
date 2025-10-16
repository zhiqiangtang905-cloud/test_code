# -*- coding: utf-8 -*-
import json
import os
import threading
import uuid
from dataclasses import dataclass
from datetime import timedelta
from queue import Queue, Empty
from typing import Any, Optional

from sqlalchemy import select, delete, and_, or_, func

from .db import get_session, utc_now, ensure_database_initialized
from .models import RedisGatewayInfo
from .redis_wrapper import RedisJSONClient
from .locks import DistributedLock, DistributedSemaphore


# 元数据行：用于标注某个顶层 key 的类型，便于回灌
# key='__meta__', name=top_key, value={'type': 'kv'|'list'|'hash'|'lock'|'semaphore'}


@dataclass
class WriteTask:
    op: str
    payload: dict


class RedisMySQLGateway:
    """
    Redis 优先、MySQL 降级的网关实现：
    - 写：先写 Redis（若可用），再异步落 MySQL
    - 读：优先读 Redis，异常时清理过期后读 MySQL
    - 仅使用一个表 redis_gateway_info（含 meta 行）
    - 提供分布式锁和信号量（Redis -> MySQL 降级）
    - 通过 HealthMonitor 实现健康拨测、回灌、周期清理
    """

    def __init__(self, redis_url: Optional[str] = None, instance_id: Optional[str] = None):
        ensure_database_initialized()
        self.instance_id = instance_id or os.getenv("INSTANCE_ID") or str(uuid.uuid4())
        self.redis = RedisJSONClient(redis_url)
        self.lock = DistributedLock(self.redis, instance_id=self.instance_id)
        self.semaphore = DistributedSemaphore(self.redis, instance_id=self.instance_id)
        self._write_queue: "Queue[WriteTask]" = Queue(maxsize=10000)
        self._writer_thread = threading.Thread(target=self._writer_loop, name="mysql-writer", daemon=True)
        self._writer_thread.start()

        # HealthMonitor 在外部构造后调用 start()
        self.health: Optional["HealthMonitor"] = None

    # --------------- 公共工具方法 ---------------
    def attach_health_monitor(self, health: "HealthMonitor"):
        self.health = health

    def _on_redis_error(self):
        self.redis.mark_unavailable()
        if self.health:
            self.health.on_redis_error()

    def _enqueue(self, op: str, **payload):
        try:
            self._write_queue.put_nowait(WriteTask(op=op, payload=payload))
        except Exception:
            # 队列满/异常，避免阻塞业务，改为同步兜底
            self._apply_task_sync(WriteTask(op=op, payload=payload))

    # --------------- 写入实现（异步消费者） ---------------
    def _writer_loop(self):
        while True:
            try:
                task = self._write_queue.get(timeout=1)
            except Empty:
                continue
            try:
                self._apply_task_sync(task)
            except Exception:
                # 单任务失败不终止线程
                pass
            finally:
                self._write_queue.task_done()

    def _apply_task_sync(self, task: WriteTask):
        op = task.op
        p = task.payload
        with get_session() as session:
            if op == "set_kv":
                self._mysql_upsert_kv(session, p["key"], p["value"], p.get("ttl"))
            elif op == "expire":
                self._mysql_expire(session, p["key"], p["ttl"]) 
            elif op == "delete":
                self._mysql_delete_key(session, p["key"]) 
            elif op == "hset":
                self._mysql_hset(session, p["name"], p["key"], p["value"], p.get("ttl"))
            elif op == "hdel":
                self._mysql_hdel(session, p["name"], p["key"]) 
            elif op == "list_write":
                self._mysql_list_store(session, p["name"], p["list_value"], p.get("ttl"))
            elif op == "meta":
                self._mysql_upsert_meta(session, p["top_key"], p["type"], p.get("ttl"))

    # --------------- MySQL 存取方法 ---------------
    def _mysql_upsert_meta(self, session, top_key: str, typ: str, ttl: Optional[int]):
        expire_time = (utc_now() + timedelta(seconds=ttl)) if ttl else None
        row = session.get(RedisGatewayInfo, {"key": "__meta__", "name": top_key})
        if row is None:
            row = RedisGatewayInfo(key="__meta__", name=top_key, value=json.dumps({"type": typ}), expire_time=expire_time)
            session.add(row)
        else:
            row.value = json.dumps({"type": typ})
            row.expire_time = expire_time
        session.flush()

    def _mysql_upsert_kv(self, session, key: str, value: Any, ttl: Optional[int]):
        expire_time = (utc_now() + timedelta(seconds=ttl)) if ttl else None
        # 主行：key=key, name=''
        row = session.get(RedisGatewayInfo, {"key": key, "name": ""})
        if row is None:
            row = RedisGatewayInfo(key=key, name="", value=json.dumps(value), expire_time=expire_time)
            session.add(row)
        else:
            row.value = json.dumps(value)
            row.expire_time = expire_time
        # 元数据
        self._mysql_upsert_meta(session, key, "list" if isinstance(value, list) else "kv", ttl)
        session.flush()

    def _mysql_expire(self, session, key: str, ttl: int):
        expire_time = utc_now() + timedelta(seconds=ttl)
        # 如果是 KV/List 行
        row = session.get(RedisGatewayInfo, {"key": key, "name": ""})
        if row:
            row.expire_time = expire_time
        # Hash 全量字段
        session.execute(
            RedisGatewayInfo.__table__.update()
            .where(RedisGatewayInfo.name == key)
            .values(expire_time=expire_time)
        )
        # Meta
        meta = session.get(RedisGatewayInfo, {"key": "__meta__", "name": key})
        if meta:
            meta.expire_time = expire_time
        session.flush()

    def _mysql_delete_key(self, session, key: str):
        session.execute(delete(RedisGatewayInfo).where(and_(RedisGatewayInfo.key == key, RedisGatewayInfo.name == "")))
        session.execute(delete(RedisGatewayInfo).where(RedisGatewayInfo.name == key))
        session.execute(delete(RedisGatewayInfo).where(and_(RedisGatewayInfo.key == "__meta__", RedisGatewayInfo.name == key)))
        session.flush()

    def _mysql_hset(self, session, name: str, key: str, value: Any, ttl: Optional[int]):
        expire_time = (utc_now() + timedelta(seconds=ttl)) if ttl else None
        row = session.get(RedisGatewayInfo, {"key": key, "name": name})
        if row is None:
            row = RedisGatewayInfo(key=key, name=name, value=json.dumps(value), expire_time=expire_time)
            session.add(row)
        else:
            row.value = json.dumps(value)
            # 仅当传入了 ttl 才更新字段过期
            if ttl is not None:
                row.expire_time = expire_time
        # 维护 meta（Hash 容器）
        self._mysql_upsert_meta(session, name, "hash", ttl)
        session.flush()

    def _mysql_hdel(self, session, name: str, key: str):
        session.execute(delete(RedisGatewayInfo).where(and_(RedisGatewayInfo.key == key, RedisGatewayInfo.name == name)))
        session.flush()

    def _mysql_list_store(self, session, name: str, list_value: list[Any], ttl: Optional[int]):
        expire_time = (utc_now() + timedelta(seconds=ttl)) if ttl else None
        row = session.get(RedisGatewayInfo, {"key": name, "name": ""})
        if row is None:
            row = RedisGatewayInfo(key=name, name="", value=json.dumps(list_value), expire_time=expire_time)
            session.add(row)
        else:
            row.value = json.dumps(list_value)
            row.expire_time = expire_time
        self._mysql_upsert_meta(session, name, "list", ttl)
        session.flush()

    # --------------- 对外 API：写入（先 Redis 后入队） ---------------
    def set(self, key: str, value: Any):
        try:
            if self.redis.is_available:
                self.redis.set(key, value)
        except Exception:
            self._on_redis_error()
        self._enqueue("set_kv", key=key, value=value, ttl=None)

    def set_ex(self, key: str, value: Any, ttl: int):
        try:
            if self.redis.is_available:
                self.redis.setex(key, ttl, value)
        except Exception:
            self._on_redis_error()
        self._enqueue("set_kv", key=key, value=value, ttl=ttl)

    def expire(self, key: str, ttl: int):
        try:
            if self.redis.is_available:
                self.redis.expire(key, ttl)
        except Exception:
            self._on_redis_error()
        self._enqueue("expire", key=key, ttl=ttl)

    def delete(self, key: str):
        try:
            if self.redis.is_available:
                self.redis.delete(key)
        except Exception:
            self._on_redis_error()
        self._enqueue("delete", key=key)

    def hset(self, name: str, key: str, value: Any, ttl: Optional[int] = None):
        try:
            if self.redis.is_available:
                self.redis.hset(name, key, value)
        except Exception:
            self._on_redis_error()
        self._enqueue("hset", name=name, key=key, value=value, ttl=ttl)
        if ttl is not None:
            # 同步设置容器 ttl
            self.expire(name, ttl)

    def hdel(self, name: str, key: str):
        try:
            if self.redis.is_available:
                self.redis.hdel(name, key)
        except Exception:
            self._on_redis_error()
        self._enqueue("hdel", name=name, key=key)

    def rpush(self, name: str, *vals: Any, ttl: Optional[int] = None):
        try:
            if self.redis.is_available and vals:
                self.redis.rpush(name, *vals)
                if ttl is not None:
                    self.redis.expire(name, ttl)
        except Exception:
            self._on_redis_error()
        # MySQL：获取当前 list，更新
        lst = self._mysql_list_read(name)
        lst.extend(list(vals))
        self._enqueue("list_write", name=name, list_value=lst, ttl=ttl)

    def lpush(self, name: str, *vals: Any, ttl: Optional[int] = None):
        try:
            if self.redis.is_available and vals:
                self.redis.lpush(name, *vals)
                if ttl is not None:
                    self.redis.expire(name, ttl)
        except Exception:
            self._on_redis_error()
        lst = self._mysql_list_read(name)
        for v in reversed(vals):
            lst.insert(0, v)
        self._enqueue("list_write", name=name, list_value=lst, ttl=ttl)

    def lrem(self, name: str, count: int, value: Any) -> int:
        removed = 0
        try:
            if self.redis.is_available:
                removed = self.redis.lrem(name, count, value)
        except Exception:
            self._on_redis_error()
        lst = self._mysql_list_read(name)
        original = len(lst)
        if count == 0:
            lst = [v for v in lst if v != value]
        elif count > 0:
            c = count
            new = []
            for v in lst:
                if v == value and c > 0:
                    c -= 1
                    continue
                new.append(v)
            lst = new
        else:  # count < 0，从尾部开始
            c = -count
            i = len(lst) - 1
            while i >= 0 and c > 0:
                if lst[i] == value:
                    del lst[i]
                    c -= 1
                i -= 1
        self._enqueue("list_write", name=name, list_value=lst, ttl=None)
        return removed if removed else (original - len(lst))

    # --------------- 对外 API：读取（Redis 优先，异常走 MySQL） ---------------
    def get(self, key: str) -> Any:
        try:
            if self.redis.is_available:
                return self.redis.get(key)
        except Exception:
            self._on_redis_error()
        # MySQL 读取前先清理过期
        with get_session() as session:
            from .db import cleanup_expired
            cleanup_expired(session)
            row = session.get(RedisGatewayInfo, {"key": key, "name": ""})
            if row is None:
                return None
            if row.expire_time and row.expire_time <= utc_now():
                return None
            return json.loads(row.value) if row.value is not None else None

    def hget(self, name: str, key: str) -> Any:
        try:
            if self.redis.is_available:
                return self.redis.hget(name, key)
        except Exception:
            self._on_redis_error()
        with get_session() as session:
            from .db import cleanup_expired
            cleanup_expired(session)
            row = session.get(RedisGatewayInfo, {"key": key, "name": name})
            if row is None:
                return None
            if row.expire_time and row.expire_time <= utc_now():
                return None
            return json.loads(row.value) if row.value is not None else None

    def hgetall(self, name: str) -> dict[str, Any]:
        try:
            if self.redis.is_available:
                return self.redis.hgetall(name)
        except Exception:
            self._on_redis_error()
        with get_session() as session:
            from .db import cleanup_expired
            cleanup_expired(session)
            rows = session.execute(
                select(RedisGatewayInfo).where(and_(RedisGatewayInfo.name == name, RedisGatewayInfo.key != "__meta__"))
            ).scalars().all()
            now = utc_now()
            result = {}
            for r in rows:
                if r.expire_time and r.expire_time <= now:
                    continue
                result[r.key] = json.loads(r.value) if r.value else None
            return result

    def exists(self, key: str) -> bool:
        try:
            if self.redis.is_available:
                if self.redis.exists(key):
                    return True
                # 尝试判断是否为 hash 容器
                # 如果是 hash，exists(key) 在 Redis 返回 1，我们这里辅助判断
                if self.redis._client.hlen(key) > 0:
                    return True
                return False
        except Exception:
            self._on_redis_error()
        with get_session() as session:
            from .db import cleanup_expired
            cleanup_expired(session)
            now = utc_now()
            # KV/List 行存在
            row = session.get(RedisGatewayInfo, {"key": key, "name": ""})
            if row and (row.expire_time is None or row.expire_time > now):
                return True
            # Hash 子项存在
            cnt = session.execute(
                select(func.count()).select_from(RedisGatewayInfo).where(
                    and_(RedisGatewayInfo.name == key, or_(RedisGatewayInfo.expire_time.is_(None), RedisGatewayInfo.expire_time > now))
                )
            ).scalar() or 0
            return cnt > 0

    def lrange(self, name: str, start: int = 0, end: int = -1) -> list[Any]:
        try:
            if self.redis.is_available:
                return self.redis.lrange(name, start, end)
        except Exception:
            self._on_redis_error()
        lst = self._mysql_list_read(name)
        # Redis 的 lrange end 包含端点，-1 表示到末尾
        if end == -1:
            end = len(lst) - 1
        if start < 0:
            start = len(lst) + start
        if end < 0:
            end = len(lst) + end
        start = max(0, start)
        end = min(len(lst) - 1, end)
        if start > end or not lst:
            return []
        return lst[start : end + 1]

    def Lrange(self, name: str, start: int = 0, end: int = -1) -> list[Any]:
        return self.lrange(name, start, end)

    def lpop(self, name: str, count: Optional[int] = None) -> Any:
        try:
            if self.redis.is_available:
                return self.redis.lpop(name, count=count)
        except Exception:
            self._on_redis_error()
        lst = self._mysql_list_read(name)
        if not lst:
            return None
        if count is None:
            v = lst.pop(0)
            self._enqueue("list_write", name=name, list_value=lst, ttl=None)
            return v
        n = min(count, len(lst))
        out = lst[:n]
        lst = lst[n:]
        self._enqueue("list_write", name=name, list_value=lst, ttl=None)
        return out

    def lindex(self, name: str, index: int) -> Any:
        try:
            if self.redis.is_available:
                return self.redis.lindex(name, index)
        except Exception:
            self._on_redis_error()
        lst = self._mysql_list_read(name)
        if index < 0:
            index = len(lst) + index
        if index < 0 or index >= len(lst):
            return None
        return lst[index]

    def llen(self, name: str) -> int:
        try:
            if self.redis.is_available:
                return self.redis.llen(name)
        except Exception:
            self._on_redis_error()
        lst = self._mysql_list_read(name)
        return len(lst)

    # --------------- 内部：MySQL List 读取 ---------------
    def _mysql_list_read(self, name: str) -> list[Any]:
        with get_session() as session:
            from .db import cleanup_expired
            cleanup_expired(session)
            row = session.get(RedisGatewayInfo, {"key": name, "name": ""})
            if row is None:
                return []
            if row.expire_time and row.expire_time <= utc_now():
                return []
            try:
                arr = json.loads(row.value or "[]")
            except Exception:
                arr = []
            # 标注为 list 的 meta
            self._mysql_upsert_meta(session, name, "list", ttl=None)
            return arr

    # --------------- 回灌逻辑 ---------------
    def rehydrate_from_mysql(self):
        """读取 MySQL 中未过期的数据，写回 Redis。"""
        with get_session() as session:
            now = utc_now()
            # 遍历所有 meta 行
            metas = session.execute(
                select(RedisGatewayInfo).where(and_(RedisGatewayInfo.key == "__meta__", or_(RedisGatewayInfo.expire_time.is_(None), RedisGatewayInfo.expire_time > now)))
            ).scalars().all()
            for meta in metas:
                try:
                    info = json.loads(meta.value or "{}")
                except Exception:
                    info = {}
                typ = info.get("type")
                top = meta.name
                ttl = None
                if meta.expire_time:
                    ttl = max(1, int((meta.expire_time - now).total_seconds())) if meta.expire_time > now else None
                if typ == "kv":
                    row = session.get(RedisGatewayInfo, {"key": top, "name": ""})
                    if not row or (row.expire_time and row.expire_time <= now):
                        continue
                    val = json.loads(row.value) if row.value else None
                    # 写回 Redis
                    try:
                        if ttl:
                            self.redis.setex(top, ttl, val)
                        else:
                            self.redis.set(top, val)
                    except Exception:
                        self._on_redis_error()
                        return
                elif typ == "list":
                    row = session.get(RedisGatewayInfo, {"key": top, "name": ""})
                    if not row or (row.expire_time and row.expire_time <= now):
                        continue
                    arr = json.loads(row.value or "[]")
                    try:
                        # 先删除再重建，保持一致性
                        self.redis.delete(top)
                        if arr:
                            self.redis.rpush(top, *arr)
                        if ttl:
                            self.redis.expire(top, ttl)
                    except Exception:
                        self._on_redis_error()
                        return
                elif typ == "hash":
                    rows = session.execute(
                        select(RedisGatewayInfo).where(and_(RedisGatewayInfo.name == top, or_(RedisGatewayInfo.expire_time.is_(None), RedisGatewayInfo.expire_time > now)))
                    ).scalars().all()
                    mapping = {}
                    for r in rows:
                        mapping[r.key] = json.loads(r.value) if r.value else None
                    try:
                        # 清空并回填
                        self.redis.delete(top)
                        if mapping:
                            for k, v in mapping.items():
                                self.redis.hset(top, k, v)
                        if ttl:
                            self.redis.expire(top, ttl)
                    except Exception:
                        self._on_redis_error()
                        return
                else:
                    # 锁/信号量等跳过
                    continue
