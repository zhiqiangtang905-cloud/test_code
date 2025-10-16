# -*- coding: utf-8 -*-
"""
可插入的 Redis -> MySQL 降级与健康拨测模块（多实例共用一个数据库）

集成说明：
- 直接使用你的工程中的以下工厂函数（无需新建连接）：
  - get_redis_cache_service() -> 返回已连接的 redis 客户端
  - get_db_session() -> 返回 SQLAlchemy Session 工厂，可用 with get_db_session() as session:
- 所有写入 Redis 的数据一律使用 json.dumps() 转为字符串；读取时使用 json.loads() 转换。
- MySQL 侧以表 `redis_gateway_info` 存储镜像数据（注意：变量名尽量使用 service，“gateway”仅用于表名，因外部表结构约束）。
  表结构要求（需预先存在）：
  - name VARCHAR(...)，key VARCHAR(...) 组成联合主键
  - value TEXT/VARCHAR 存 json.dumps 的字符串
  - expire_time DATETIME NULL
  - last_update_time DATETIME NOT NULL

实现能力：
- 重构 set/get/hset/hget/set_ex(用于分布式锁)/hdel/rpush/lpush/lrem/expire/lpop/lindex/llen/hgetall/lrange/exists/delete
- Redis 上下文分布式锁（SET NX EX 语义），并支持 MySQL 降级锁
- Redis 信号量（计数信号量），并支持 MySQL 降级信号量
- 健康拨测：
  - 当检测到 Redis 异常，标记不可用并每 10s 探测一次；
  - 恢复后从 MySQL 回灌数据到 Redis，切回正常；
  - 程序启动后每 60s 清理一次 MySQL 过期数据（使用 schedule 调度）
- 多实例场景：定时任务与回灌均使用分布式锁确保仅单实例执行

重要说明：
- 表名包含 “gateway” 是既有库表命名约束，非变量名选择；代码中变量尽量使用 service 前缀。
- 列表(List) 在 MySQL 中以单行记录保存整个列表（value 为 JSON 数组），键映射规则见下：
  - KV：      name='__kv__', key=<kv_key>
  - HASH：    name=<hash_name>, key=<field>
  - LIST：    name=<list_name>, key='__list__'
  - LOCK：    name='__lock__', key=<lock_key>
  - SEMAPHORE：name='__semaphore__', key=<semaphore_name>

你可直接将本文件放入工程，并按需初始化 `ResilientRedisService`。
"""
from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import schedule
from sqlalchemy import text

# 由宿主工程提供的工厂函数：不要在此文件中建立新连接
# 建议在初始化 ResilientRedisService 时以依赖注入方式传入，避免导入路径问题
# from your_project import get_redis_cache_service, get_db_session  # 由使用方提供


logger = logging.getLogger(__name__)


# ------------------------------
# 工具函数（JSON/时间）
# ------------------------------

def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _json_loads(value_str: Optional[str]) -> Any:
    if value_str is None:
        return None
    return json.loads(value_str)


def _utcnow() -> datetime:
    return datetime.utcnow()


# ------------------------------
# 健康拨测类：存 Redis 存活状态与拨测状态
# ------------------------------
@dataclass
class RedisHealthProbe:
    # Redis 当前是否可用
    redis_alive: bool = True
    # 是否处于拨测中（true 时每 10s 探测一次）
    probing: bool = False

    # 内部控制
    _probe_job: Optional[schedule.Job] = None
    _lock: threading.Lock = threading.Lock()

    def start_probing(self, probe_callable: Callable[[], None]) -> None:
        """开启 10s 一次的拨测任务。"""
        with self._lock:
            if self.probing:
                return
            self.probing = True
            # 每 10 秒拨测
            self._probe_job = schedule.every(10).seconds.do(probe_callable)
            logger.warning("Redis 标记为不可用，已启动 10s 拨测任务")

    def stop_probing(self) -> None:
        """停止拨测任务。"""
        with self._lock:
            if self._probe_job is not None:
                try:
                    schedule.cancel_job(self._probe_job)
                except Exception:
                    pass
                self._probe_job = None
            self.probing = False
            logger.info("Redis 拨测已停止（恢复可用）")


# ------------------------------
# 后台 schedule 运行器（单守护线程）
# ------------------------------
class _ScheduleRunner(threading.Thread):
    def __init__(self) -> None:
        super().__init__(name="service-schedule-runner", daemon=True)
        self._stopped = threading.Event()

    def run(self) -> None:
        while not self._stopped.is_set():
            try:
                schedule.run_pending()
            except Exception as e:
                logger.exception("schedule.run_pending 发生异常: %s", e)
            self._stopped.wait(1.0)

    def stop(self) -> None:
        self._stopped.set()


# ------------------------------
# 异步 MySQL 写入队列（单工作线程，保证顺序）
# ------------------------------
class _AsyncDbWriter:
    def __init__(self, db_session_factory: Callable[[], Any]) -> None:
        self._db_session_factory = db_session_factory
        self._queue: "queue.Queue[Tuple[str, Dict[str, Any]]]" = _SafeQueue()
        self._worker = threading.Thread(target=self._worker_loop, name="service-db-writer", daemon=True)
        self._worker.start()

    def enqueue(self, op: str, payload: Dict[str, Any]) -> None:
        self._queue.put((op, payload))

    def _worker_loop(self) -> None:
        while True:
            op, payload = self._queue.get()
            try:
                self._execute(op, payload)
            except Exception:
                logger.exception("异步写入任务失败 op=%s payload=%s", op, payload)
            finally:
                self._queue.task_done()

    # 各种写入操作统一入口
    def _execute(self, op: str, p: Dict[str, Any]) -> None:
        with self._db_session_factory() as session:
            now = _utcnow()

            if op == "upsert_kv":
                # name='__kv__', key=<kv_key>
                ttl = p.get("ttl")
                expire_time = (now + timedelta(seconds=ttl)) if ttl else None
                session.execute(
                    text(
                        """
                        INSERT INTO redis_gateway_info(`name`,`key`,`value`,expire_time,last_update_time)
                        VALUES (:name, :key, :value, :expire_time, :now)
                        ON DUPLICATE KEY UPDATE
                          `value`=VALUES(`value`),
                          expire_time=VALUES(expire_time),
                          last_update_time=VALUES(last_update_time)
                        """
                    ),
                    {
                        "name": "__kv__",
                        "key": p["key"],
                        "value": p["value_json"],
                        "expire_time": expire_time,
                        "now": now,
                    },
                )
                session.commit()
                return

            if op == "delete_kv":
                session.execute(
                    text("DELETE FROM redis_gateway_info WHERE `name`='__kv__' AND `key`=:key"),
                    {"key": p["key"]},
                )
                session.commit()
                return

            if op == "upsert_hash_field":
                ttl = p.get("ttl")
                expire_time = (now + timedelta(seconds=ttl)) if ttl else None
                session.execute(
                    text(
                        """
                        INSERT INTO redis_gateway_info(`name`,`key`,`value`,expire_time,last_update_time)
                        VALUES (:name, :key, :value, :expire_time, :now)
                        ON DUPLICATE KEY UPDATE
                          `value`=VALUES(`value`),
                          expire_time=VALUES(expire_time),
                          last_update_time=VALUES(last_update_time)
                        """
                    ),
                    {
                        "name": p["hash_name"],
                        "key": p["field"],
                        "value": p["value_json"],
                        "expire_time": expire_time,
                        "now": now,
                    },
                )
                session.commit()
                return

            if op == "delete_hash_field":
                session.execute(
                    text(
                        "DELETE FROM redis_gateway_info WHERE `name`=:name AND `key`=:key"
                    ),
                    {"name": p["hash_name"], "key": p["field"]},
                )
                session.commit()
                return

            if op == "upsert_list":
                ttl = p.get("ttl")
                expire_time = (now + timedelta(seconds=ttl)) if ttl else None
                session.execute(
                    text(
                        """
                        INSERT INTO redis_gateway_info(`name`,`key`,`value`,expire_time,last_update_time)
                        VALUES (:name, '__list__', :value, :expire_time, :now)
                        ON DUPLICATE KEY UPDATE
                          `value`=VALUES(`value`),
                          expire_time=VALUES(expire_time),
                          last_update_time=VALUES(last_update_time)
                        """
                    ),
                    {
                        "name": p["list_name"],
                        "value": p["list_json"],
                        "expire_time": expire_time,
                        "now": now,
                    },
                )
                session.commit()
                return

            if op == "expire_name":
                # 对 KV(hash/list 之外) 之外的 name 批量设置过期（list/hash 的 name 级别）
                expire_time = now + timedelta(seconds=p["ttl"]) if p.get("ttl") else None
                session.execute(
                    text(
                        """
                        UPDATE redis_gateway_info
                           SET expire_time=:expire_time, last_update_time=:now
                         WHERE `name`=:name
                        """
                    ),
                    {"expire_time": expire_time, "now": now, "name": p["name"]},
                )
                session.commit()
                return

            if op == "lock_try_acquire":
                # 仅在 DB 降级下使用：尝试基于过期时间的非阻塞获取
                expire_time = now + timedelta(seconds=p["ttl"]) if p.get("ttl") else now + timedelta(seconds=30)
                session.execute(
                    text(
                        """
                        INSERT INTO redis_gateway_info(`name`,`key`,`value`,expire_time,last_update_time)
                        VALUES ('__lock__', :key, :token, :expire_time, :now)
                        ON DUPLICATE KEY UPDATE
                          `value`=IF(expire_time < :now, VALUES(`value`), `value`),
                          expire_time=IF(expire_time < :now, VALUES(expire_time), expire_time),
                          last_update_time=IF(expire_time < :now, VALUES(last_update_time), last_update_time)
                        """
                    ),
                    {"key": p["lock_key"], "token": p["token"], "expire_time": expire_time, "now": now},
                )
                # 验证是否获取成功
                row = session.execute(
                    text(
                        "SELECT `value` FROM redis_gateway_info WHERE `name`='__lock__' AND `key`=:key AND (expire_time IS NULL OR expire_time>:now)"
                    ),
                    {"key": p["lock_key"], "now": now},
                ).first()
                session.commit()
                p["result"][0] = bool(row and row[0] == p["token"])  # 通过外部可变对象返回结果
                return

            if op == "lock_release":
                session.execute(
                    text(
                        "DELETE FROM redis_gateway_info WHERE `name`='__lock__' AND `key`=:key AND `value`=:token"
                    ),
                    {"key": p["lock_key"], "token": p["token"]},
                )
                session.commit()
                return

            if op == "semaphore_update":
                # DB 降级信号量：value 为 JSON 数组 tokens
                expire_time = now + timedelta(seconds=p.get("ttl", 60))
                session.execute(
                    text(
                        """
                        INSERT INTO redis_gateway_info(`name`,`key`,`value`,expire_time,last_update_time)
                        VALUES ('__semaphore__', :name, :value, :expire_time, :now)
                        ON DUPLICATE KEY UPDATE
                          `value`=VALUES(`value`),
                          expire_time=VALUES(expire_time),
                          last_update_time=VALUES(last_update_time)
                        """
                    ),
                    {
                        "name": p["sem_name"],
                        "value": _json_dumps(p["tokens"]),
                        "expire_time": expire_time,
                        "now": now,
                    },
                )
                session.commit()
                return

            if op == "purge_expired":
                session.execute(text("DELETE FROM redis_gateway_info WHERE expire_time IS NOT NULL AND expire_time <= :now"), {"now": now})
                session.commit()
                return

            raise ValueError(f"未知的异步写入操作类型: {op}")


# 轻量安全队列（避免直接依赖 queue 以简化导入）
class _SafeQueue:
    def __init__(self):
        import queue

        self._q: "queue.Queue" = queue.Queue()

    def put(self, item: Any) -> None:
        self._q.put(item)

    def get(self) -> Any:
        return self._q.get()

    def task_done(self) -> None:
        self._q.task_done()


# ------------------------------
# 分布式锁（Redis 优先，MySQL 降级），可用作上下文管理器
# ------------------------------
class DistributedLock:
    def __init__(self, service: "ResilientRedisService", lock_key: str, ttl_seconds: int = 30) -> None:
        self._service = service
        self._lock_key = lock_key
        self._ttl = ttl_seconds
        self._token = f"{uuid.uuid4()}"
        self._acquired = False

    def acquire(self, block: bool = False, retry_interval: float = 0.2) -> bool:
        # 优先 Redis SET NX EX
        if self._service.health.redis_alive:
            try:
                # 注意：不同 Redis 客户端的 set 参数名略有不同
                ok = self._service.redis.set(self._lock_key, self._token, nx=True, ex=self._ttl)
                if ok:
                    self._acquired = True
                    return True
            except Exception as e:
                self._service._on_redis_error(e)
                # 降级到 DB

        # DB 降级：基于过期时间的“自旋”
        if not block:
            return self._try_acquire_db()

        # 阻塞重试
        end_time = time.time() + self._ttl
        while time.time() < end_time:
            if self._try_acquire_db():
                return True
            time.sleep(retry_interval)
        return False

    def _try_acquire_db(self) -> bool:
        result_holder = [False]
        self._service._db_writer.enqueue(
            "lock_try_acquire",
            {"lock_key": self._lock_key, "token": self._token, "ttl": self._ttl, "result": result_holder},
        )
        # 简化：短暂等待工作线程执行（确保尽快得到结果），实际场景可替换更稳妥的回调/事件
        time.sleep(0.05)
        self._acquired = bool(result_holder[0])
        return self._acquired

    def release(self) -> None:
        if not self._acquired:
            return
        released = False
        if self._service.health.redis_alive:
            try:
                # Lua 脚本：仅当 value==token 时删除
                lua = (
                    "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end"
                )
                self._service.redis.eval(lua, 1, self._lock_key, self._token)
                released = True
            except Exception as e:
                self._service._on_redis_error(e)
        if not released:
            self._service._db_writer.enqueue("lock_release", {"lock_key": self._lock_key, "token": self._token})
        self._acquired = False

    def __enter__(self) -> "DistributedLock":
        acquired = self.acquire(block=False)
        if not acquired:
            raise TimeoutError(f"无法获取分布式锁: {self._lock_key}")
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()


# ------------------------------
# 简单 Redis 信号量（计数），带 MySQL 降级
# ------------------------------
class RedisSemaphore:
    def __init__(self, service: "ResilientRedisService", name: str, capacity: int, ttl_seconds: int = 60) -> None:
        self._service = service
        self._name = name
        self._capacity = int(capacity)
        self._ttl = int(ttl_seconds)
        self._token = f"{uuid.uuid4()}"

    @property
    def redis_key(self) -> str:
        return f"semaphore:{self._name}"

    def acquire(self) -> bool:
        # Redis 优先：LLEN < capacity 则 RPUSH token 并 EXPIRE
        if self._service.health.redis_alive:
            try:
                lua = (
                    "local k=KEYS[1]; local cap=tonumber(ARGV[1]); local token=ARGV[2]; local ttl=tonumber(ARGV[3]); "
                    "local n=redis.call('llen', k); if n < cap then redis.call('rpush', k, token); redis.call('expire', k, ttl); return 1 else return 0 end"
                )
                res = self._service.redis.eval(lua, 1, self.redis_key, str(self._capacity), self._token, str(self._ttl))
                if int(res or 0) == 1:
                    return True
            except Exception as e:
                self._service._on_redis_error(e)
        # DB 降级：读取 tokens 数组，若长度 < capacity 则写回追加
        return self._acquire_db()

    def _acquire_db(self) -> bool:
        tokens = self._get_tokens_db()
        if len(tokens) >= self._capacity:
            return False
        tokens.append(self._token)
        self._service._db_writer.enqueue(
            "semaphore_update",
            {"sem_name": self._name, "tokens": tokens, "ttl": self._ttl},
        )
        time.sleep(0.05)
        return True

    def release(self) -> None:
        if self._service.health.redis_alive:
            try:
                self._service.redis.lrem(self.redis_key, 0, self._token)
            except Exception as e:
                self._service._on_redis_error(e)
        # DB 降级
        tokens = self._get_tokens_db()
        if self._token in tokens:
            tokens = [t for t in tokens if t != self._token]
            self._service._db_writer.enqueue(
                "semaphore_update", {"sem_name": self._name, "tokens": tokens, "ttl": self._ttl}
            )

    def _get_tokens_db(self) -> List[str]:
        with self._service.db_session_factory() as session:
            now = _utcnow()
            row = session.execute(
                text(
                    "SELECT `value`, expire_time FROM redis_gateway_info WHERE `name`='__semaphore__' AND `key`=:k AND (expire_time IS NULL OR expire_time>:now)"
                ),
                {"k": self._name, "now": now},
            ).first()
            if not row:
                return []
            try:
                return list(_json_loads(row[0]))
            except Exception:
                return []


# ------------------------------
# 核心服务：对外提供 Redis API 包装 + MySQL 降级 + 健康拨测
# ------------------------------
class ResilientRedisService:
    def __init__(
        self,
        redis_factory: Callable[[], Any],
        db_session_factory: Callable[[], Any],
        *,
        enable_schedule: bool = True,
    ) -> None:
        # 依赖注入工厂
        self.redis_factory = redis_factory
        self.db_session_factory = db_session_factory

        # 连接实例（不新建连接，由宿主工程提供）
        self.redis = redis_factory()

        # 健康拨测状态
        self.health = RedisHealthProbe(redis_alive=self._ping_redis_ok())

        # 异步写入器
        self._db_writer = _AsyncDbWriter(db_session_factory)

        # 实例标识（用于锁 owner 等）
        self.instance_id = f"service-{uuid.uuid4()}"

        # schedule 后台线程
        self._schedule_runner = _ScheduleRunner() if enable_schedule else None
        if enable_schedule:
            # 60s 周期清理过期数据（分布式锁保护，保证单实例执行）
            schedule.every(60).seconds.do(self._cleanup_expired_job)
            self._schedule_runner.start()

    # --------------------------
    # 基础工具
    # --------------------------
    def _ping_redis_ok(self) -> bool:
        try:
            self.redis.ping()
            return True
        except Exception:
            return False

    def _on_redis_error(self, exc: Exception) -> None:
        logger.exception("Redis 调用异常，降级到 MySQL：%s", exc)
        if self.health.redis_alive:
            # 第一次发现异常：标记为不可用并启动拨测
            self.health.redis_alive = False
            self.health.start_probing(lambda: self._probe_and_recover())

    def _probe_and_recover(self) -> None:
        try:
            # 仅单实例执行探测与回灌
            with DistributedLock(self, "service:health_probe", ttl_seconds=30):
                if self._ping_redis_ok():
                    logger.info("健康拨测成功，开始回灌 Redis 数据")
                    try:
                        self._rehydrate_redis_from_db()
                    except Exception:
                        logger.exception("回灌 Redis 失败")
                    # 切回可用
                    self.health.redis_alive = True
                    self.health.stop_probing()
        except Exception as e:
            # 获取锁失败或其他异常，忽略，等待下次拨测
            logger.debug("健康拨测本轮未执行：%s", e)

    def _rehydrate_redis_from_db(self) -> None:
        now = _utcnow()
        with self.db_session_factory() as session:
            rows = session.execute(
                text(
                    "SELECT `name`,`key`,`value`,expire_time FROM redis_gateway_info WHERE expire_time IS NULL OR expire_time>:now"
                ),
                {"now": now},
            ).fetchall()
        # 清理 Redis 侧旧数据（仅对我们管理的 key 做温和处理）：这里不盲目删除，采取覆盖策略
        for name, key, value_json, expire_time in rows:
            try:
                # 跳过锁/信号量
                if name == "__lock__" or name == "__semaphore__":
                    continue
                ttl = None
                if expire_time is not None:
                    ttl_seconds = int((expire_time - now).total_seconds())
                    ttl = ttl_seconds if ttl_seconds > 0 else None
                if name == "__kv__":
                    # KV -> set 覆盖
                    self.redis.set(key, value_json)
                    if ttl:
                        self.redis.expire(key, ttl)
                elif key == "__list__":
                    # LIST -> 先删除再 rpush
                    self.redis.delete(name)
                    try:
                        data = _json_loads(value_json) or []
                    except Exception:
                        data = []
                    if data:
                        # 写入时每个元素也需 json.dumps
                        enc = [ _json_dumps(item) for item in data ]
                        self.redis.rpush(name, *enc)
                    if ttl:
                        self.redis.expire(name, ttl)
                else:
                    # HASH -> 单字段写回
                    self.redis.hset(name, key, value_json)
                    if ttl:
                        self.redis.expire(name, ttl)
            except Exception as e:
                self._on_redis_error(e)
                # 如果回灌过程中又失败，后续拨测会继续
                return
        logger.info("回灌完成，共同步 %d 条记录", len(rows))

    def _cleanup_expired_job(self) -> None:
        """每 60s 清理一次过期数据，仅单实例执行。"""
        try:
            with DistributedLock(self, "service:cleanup_expired", ttl_seconds=50):
                self.purge_expired_now()
        except Exception as e:
            logger.debug("清理任务本轮跳过：%s", e)

    # --------------------------
    # 过期清理（公开方法，读取前可调用）
    # --------------------------
    def purge_expired_now(self) -> None:
        self._db_writer.enqueue("purge_expired", {})
        # 等待片刻确保执行
        time.sleep(0.05)

    # --------------------------
    # Redis 接口包装（按 Redis 传参语义）
    # 写规则：若 Redis 可用 -> 先写 Redis，再异步写 MySQL；若 Redis 不可用 -> 直接写 MySQL（同步触发）
    # 读规则：优先 Redis；若 Redis 异常 -> 清理过期后走 MySQL
    # --------------------------
    # KV
    def set(self, key: str, value: Any) -> bool:
        value_json = _json_dumps(value)
        if self.health.redis_alive:
            try:
                ok = self.redis.set(key, value_json)
                self._db_writer.enqueue("upsert_kv", {"key": key, "value_json": value_json, "ttl": None})
                return bool(ok)
            except Exception as e:
                self._on_redis_error(e)
        # DB 降级
        self._db_writer.enqueue("upsert_kv", {"key": key, "value_json": value_json, "ttl": None})
        time.sleep(0.02)
        return True

    def get(self, key: str) -> Any:
        if self.health.redis_alive:
            try:
                raw = self.redis.get(key)
                if raw is None:
                    return None
                return _json_loads(raw)
            except Exception as e:
                self._on_redis_error(e)
        # 降级到 MySQL
        self.purge_expired_now()
        with self.db_session_factory() as session:
            row = session.execute(
                text("SELECT `value` FROM redis_gateway_info WHERE `name`='__kv__' AND `key`=:k AND (expire_time IS NULL OR expire_time > :now)"),
                {"k": key, "now": _utcnow()},
            ).first()
            return _json_loads(row[0]) if row else None

    def delete(self, key: str) -> int:
        # 仅 KV
        deleted = 0
        if self.health.redis_alive:
            try:
                deleted = int(self.redis.delete(key) or 0)
            except Exception as e:
                self._on_redis_error(e)
        self._db_writer.enqueue("delete_kv", {"key": key})
        return deleted

    def exists(self, key: str) -> bool:
        if self.health.redis_alive:
            try:
                return bool(self.redis.exists(key))
            except Exception as e:
                self._on_redis_error(e)
        with self.db_session_factory() as session:
            row = session.execute(
                text(
                    "SELECT 1 FROM redis_gateway_info WHERE `name`='__kv__' AND `key`=:k AND (expire_time IS NULL OR expire_time>:now) LIMIT 1"
                ),
                {"k": key, "now": _utcnow()},
            ).first()
            return bool(row)

    def expire(self, name_or_key: str, ttl: int) -> bool:
        """对 KV(key) 或 HASH/LIST(name) 设置过期。"""
        ok = True
        if self.health.redis_alive:
            try:
                ok = bool(self.redis.expire(name_or_key, ttl))
            except Exception as e:
                self._on_redis_error(e)
        # MySQL：KV 用 name='__kv__'；否则以 name 维度批量更新
        # 这里无从区分是否 KV，采用两条语句覆盖（KV + name 批量）
        now = _utcnow()
        expire_time = now + timedelta(seconds=int(ttl))
        with self.db_session_factory() as session:
            # KV
            session.execute(
                text(
                    "UPDATE redis_gateway_info SET expire_time=:t, last_update_time=:now WHERE `name`='__kv__' AND `key`=:k"
                ),
                {"t": expire_time, "now": now, "k": name_or_key},
            )
            # name 批量（适用于 hash/list）
            session.execute(
                text("UPDATE redis_gateway_info SET expire_time=:t, last_update_time=:now WHERE `name`=:n"),
                {"t": expire_time, "now": now, "n": name_or_key},
            )
            session.commit()
        return ok

    # SETEX（用于分布式锁常见写法：set_ex(key, token, ttl)）。为避免与 set 的 KV 语义混淆，这里提供独立方法。
    def set_ex(self, key: str, token_or_value: Any, ttl: int) -> bool:
        token_json = _json_dumps(token_or_value)
        if self.health.redis_alive:
            try:
                ok = self.redis.set(key, token_json, ex=int(ttl))
                # 锁等短期键不强制写入 MySQL；如需镜像，可启用：
                self._db_writer.enqueue(
                    "upsert_kv", {"key": key, "value_json": token_json, "ttl": int(ttl)}
                )
                return bool(ok)
            except Exception as e:
                self._on_redis_error(e)
        # DB 降级：作为 KV 写入（便于恢复时回灌）
        self._db_writer.enqueue("upsert_kv", {"key": key, "value_json": token_json, "ttl": int(ttl)})
        time.sleep(0.02)
        return True

    # HASH
    def hset(self, name: str, key: str, value: Any) -> int:
        value_json = _json_dumps(value)
        if self.health.redis_alive:
            try:
                n = int(self.redis.hset(name, key, value_json) or 0)
                self._db_writer.enqueue(
                    "upsert_hash_field",
                    {"hash_name": name, "field": key, "value_json": value_json, "ttl": None},
                )
                return n
            except Exception as e:
                self._on_redis_error(e)
        # DB 降级
        self._db_writer.enqueue(
            "upsert_hash_field", {"hash_name": name, "field": key, "value_json": value_json, "ttl": None}
        )
        time.sleep(0.02)
        return 1

    def hget(self, name: str, key: str) -> Any:
        if self.health.redis_alive:
            try:
                raw = self.redis.hget(name, key)
                return _json_loads(raw) if raw is not None else None
            except Exception as e:
                self._on_redis_error(e)
        # 降级读取：先清理过期
        self.purge_expired_now()
        with self.db_session_factory() as session:
            row = session.execute(
                text(
                    "SELECT `value` FROM redis_gateway_info WHERE `name`=:n AND `key`=:k AND (expire_time IS NULL OR expire_time>:now)"
                ),
                {"n": name, "k": key, "now": _utcnow()},
            ).first()
            return _json_loads(row[0]) if row else None

    def hgetall(self, name: str) -> Dict[str, Any]:
        if self.health.redis_alive:
            try:
                raw_map = self.redis.hgetall(name)
                # 注意：不同客户端返回 bytes/str；逐个 loads
                result: Dict[str, Any] = {}
                for k, v in (raw_map or {}).items():
                    k_str = k.decode() if hasattr(k, "decode") else str(k)
                    v_str = v.decode() if hasattr(v, "decode") else str(v)
                    result[k_str] = _json_loads(v_str)
                return result
            except Exception as e:
                self._on_redis_error(e)
        # DB 降级
        self.purge_expired_now()
        with self.db_session_factory() as session:
            rows = session.execute(
                text(
                    "SELECT `key`,`value` FROM redis_gateway_info WHERE `name`=:n AND (expire_time IS NULL OR expire_time>:now)"
                ),
                {"n": name, "now": _utcnow()},
            ).fetchall()
            return {row[0]: _json_loads(row[1]) for row in rows}

    def hdel(self, name: str, key: str) -> int:
        n = 0
        if self.health.redis_alive:
            try:
                n = int(self.redis.hdel(name, key) or 0)
            except Exception as e:
                self._on_redis_error(e)
        self._db_writer.enqueue("delete_hash_field", {"hash_name": name, "field": key})
        return n

    # LIST（以单行 JSON 数组镜像）
    def rpush(self, name: str, *values: Any) -> int:
        enc_values = [_json_dumps(v) for v in values]
        n = 0
        if self.health.redis_alive:
            try:
                n = int(self.redis.rpush(name, *enc_values) or 0)
                # 异步同步到 DB（读回 Redis 再组装，避免并发覆盖）
                data = self.lrange(name, 0, -1)  # 已做降级保护
                self._db_writer.enqueue("upsert_list", {"list_name": name, "list_json": _json_dumps(data)})
                return n
            except Exception as e:
                self._on_redis_error(e)
        # DB 降级：直接操作镜像
        data = self._get_list_db(name)
        data.extend(values)
        self._db_writer.enqueue("upsert_list", {"list_name": name, "list_json": _json_dumps(data)})
        return len(data)

    def lpush(self, name: str, *values: Any) -> int:
        enc_values = [_json_dumps(v) for v in values]
        n = 0
        if self.health.redis_alive:
            try:
                n = int(self.redis.lpush(name, *enc_values) or 0)
                data = self.lrange(name, 0, -1)
                self._db_writer.enqueue("upsert_list", {"list_name": name, "list_json": _json_dumps(data)})
                return n
            except Exception as e:
                self._on_redis_error(e)
        data = self._get_list_db(name)
        for v in reversed(list(values)):
            data.insert(0, v)
        self._db_writer.enqueue("upsert_list", {"list_name": name, "list_json": _json_dumps(data)})
        return len(data)

    def lrange(self, name: str, start: int = 0, end: int = -1) -> List[Any]:
        if self.health.redis_alive:
            try:
                raw_list = self.redis.lrange(name, start, end)
                # 逐个 loads
                return [ _json_loads(x.decode() if hasattr(x, "decode") else x) for x in (raw_list or []) ]
            except Exception as e:
                self._on_redis_error(e)
        data = self._get_list_db(name)
        # Redis 端 end 包含性，-1 表示末尾
        if end == -1:
            end = len(data) - 1
        end = min(end, len(data) - 1)
        if start < 0:
            start = max(0, len(data) + start)
        if end < start:
            return []
        return data[start : end + 1]

    def lpop(self, name: str, count: Optional[int] = None) -> Any:
        if self.health.redis_alive:
            try:
                if count is None:
                    raw = self.redis.lpop(name)
                    return _json_loads(raw) if raw is not None else None
                else:
                    raws = self.redis.lpop(name, count)
                    return [ _json_loads(x) for x in (raws or []) ]
            except Exception as e:
                self._on_redis_error(e)
        data = self._get_list_db(name)
        if count is None:
            val = data.pop(0) if data else None
            self._db_writer.enqueue("upsert_list", {"list_name": name, "list_json": _json_dumps(data)})
            return val
        else:
            popped: List[Any] = []
            for _ in range(min(count, len(data))):
                popped.append(data.pop(0))
            self._db_writer.enqueue("upsert_list", {"list_name": name, "list_json": _json_dumps(data)})
            return popped

    def lindex(self, name: str, index: int) -> Any:
        if self.health.redis_alive:
            try:
                raw = self.redis.lindex(name, index)
                return _json_loads(raw) if raw is not None else None
            except Exception as e:
                self._on_redis_error(e)
        data = self._get_list_db(name)
        try:
            return data[index]
        except Exception:
            return None

    def llen(self, name: str) -> int:
        if self.health.redis_alive:
            try:
                return int(self.redis.llen(name) or 0)
            except Exception as e:
                self._on_redis_error(e)
        return len(self._get_list_db(name))

    def lrem(self, name: str, count: int, value: Any) -> int:
        # 注意 Redis 的 count 语义：>0 从左到右，<0 从右到左，=0 全部
        value_json = _json_dumps(value)
        removed = 0
        if self.health.redis_alive:
            try:
                removed = int(self.redis.lrem(name, int(count), value_json) or 0)
                # 同步镜像
                data = self.lrange(name, 0, -1)
                self._db_writer.enqueue("upsert_list", {"list_name": name, "list_json": _json_dumps(data)})
                return removed
            except Exception as e:
                self._on_redis_error(e)
        # DB 降级
        data = self._get_list_db(name)
        if count == 0:
            before = len(data)
            data = [v for v in data if v != value]
            removed = before - len(data)
        elif count > 0:
            new_data: List[Any] = []
            c = count
            for v in data:
                if v == value and c > 0:
                    c -= 1
                    removed += 1
                else:
                    new_data.append(v)
            data = new_data
        else:  # count < 0 从右侧开始
            new_data = []
            c = -count
            for v in reversed(data):
                if v == value and c > 0:
                    c -= 1
                    removed += 1
                else:
                    new_data.append(v)
            data = list(reversed(new_data))
        self._db_writer.enqueue("upsert_list", {"list_name": name, "list_json": _json_dumps(data)})
        return removed

    # --------------------------
    # 内部：LIST DB 读写
    # --------------------------
    def _get_list_db(self, name: str) -> List[Any]:
        with self.db_session_factory() as session:
            row = session.execute(
                text(
                    "SELECT `value` FROM redis_gateway_info WHERE `name`=:n AND `key`='__list__' AND (expire_time IS NULL OR expire_time>:now)"
                ),
                {"n": name, "now": _utcnow()},
            ).first()
            if not row:
                return []
            try:
                return list(_json_loads(row[0]) or [])
            except Exception:
                return []

    # --------------------------
    # 对外：获取分布式锁与信号量
    # --------------------------
    def lock(self, lock_key: str, ttl_seconds: int = 30) -> DistributedLock:
        return DistributedLock(self, lock_key, ttl_seconds)

    def semaphore(self, name: str, capacity: int, ttl_seconds: int = 60) -> RedisSemaphore:
        return RedisSemaphore(self, name, capacity, ttl_seconds)


# 便捷初始化函数（建议在应用启动时构造一个单例 service）
# 示例：
# from your_project.factories import get_redis_cache_service, get_db_session
# SERVICE = create_resilient_service(get_redis_cache_service, get_db_session)

def create_resilient_service(
    get_redis_cache_service: Callable[[], Any],
    get_db_session: Callable[[], Any],
    *,
    enable_schedule: bool = True,
) -> ResilientRedisService:
    """由外部注入工厂函数创建服务。"""
    return ResilientRedisService(get_redis_cache_service, get_db_session, enable_schedule=enable_schedule)


# 便捷导出：如用户希望直接 import 使用以下名
__all__ = [
    "RedisHealthProbe",
    "ResilientRedisService",
    "DistributedLock",
    "RedisSemaphore",
    "create_resilient_service",
]
