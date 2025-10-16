# -*- coding: utf-8 -*-
"""
Redis-MySQL 降级服务与健康拨测、分布式锁/信号量实现

集成功能：
- 对常用 Redis 操作（setex/get/hset/hget/hdel/rpush/lpush/lpop/lrem/lrange/lindex/llen/exists/delete/expire/hgetall）
  进行降级：优先 Redis，Redis 异常时降级 MySQL；写操作始终异步落库 MySQL。
- MySQL 表结构使用 redis_gateway_info（注意：此处仅为兼容既有表名，变量命名避免使用 gateway）。
  字段：key(varchar)、name(varchar)、value(text JSON 字符串)、expire_time(datetime)、last_update_time(datetime)
  复合主键：(key, name)
- 健康拨测：
  - 当 Redis 读写报错时，标记可用状态为 False，并每 10 秒拨测一次。
  - 拨测成功后，将 MySQL 未过期数据回写到 Redis，并将 Redis 状态置 True，停止拨测。
  - 主程序启动后，每 60 秒清理一次 MySQL 过期数据。使用 schedule 调度器实现。
- 分布式能力：
  - 提供 Redis 上下文分布式锁（Redis SET NX EX + Lua 解锁），并在 Redis 不可用时使用 MySQL 行级锁降级。
  - 提供 Redis 信号量（ZSET 计数 + 过期清理），MySQL 降级使用单行存储令牌列表并在事务中串行化。

约定：
- 所有写入 Redis 的数据均使用 json.dumps() 转为字符串，读取时使用 json.loads() 还原。
- MySQL 中与 Redis 结构的映射：
  - String: (name='__STRING__', key=redis_key)
  - Hash:   (name=hash_name, key=field)
  - List:   (name=list_name, key='__LIST__', value=整表 JSON 列表字符串)
  - Lock:   (name='__LOCK__',   key=lock_name, value=令牌字符串)
  - Sem:    (name=f'__SEMAPHORE__:{sem_name}', key='__SEMAPHORE__', value=JSON 列表令牌)

注意：
- 代码中仅在表名处使用 "gateway"（原因：与既有数据库表名 redis_gateway_info 对齐），其余命名尽量使用 service。
- 不在此文件中初始化 Redis/MySQL 连接；请从现有项目中 import get_redis_cache_service, get_db_session。

使用方式（示例）：

from services.redis_fallback_service import get_fallback_service, start_background_tasks

service = get_fallback_service()
start_background_tasks(service)  # 主程序启动时调用：启动清理与拨测调度

service.hset('user:1', 'name', '张三')
name = service.hget('user:1', 'name')

service.rpush('queue:jobs', {'id': 1})
items = service.lrange('queue:jobs', 0, -1)

with service.distributed_lock('lock:job:1', ttl_seconds=30):
    # 只有一个实例能进入此块
    ...

sem = service.semaphore('sem:heavy-task', max_leases=3, ttl_seconds=60)
with sem.acquire(timeout_seconds=5):
    # 同时最多 3 个实例进入
    ...
"""
from __future__ import annotations

import json
import threading
import time
import uuid
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Iterable, ContextManager

# schedule 用于低开销定时任务
try:
    import schedule
except Exception:  # pragma: no cover - 运行环境可能暂未安装
    schedule = None  # type: ignore

# 引用项目内的连接工厂；此处不创建连接，只是引用
try:
    from get_redis_cache_service import get_redis_cache_service  # 用户项目中应提供
except Exception:  # pragma: no cover - 由用户项目提供
    get_redis_cache_service = None  # type: ignore

try:
    from get_db_session import get_db_session  # 用户项目中应提供
except Exception:  # pragma: no cover - 由用户项目提供
    get_db_session = None  # type: ignore

# SQLAlchemy ORM 定义（不创建 Engine，仅使用外部 Session）
from sqlalchemy import Column, String, Text, DateTime, select, func, and_, or_, delete as sa_delete
from sqlalchemy.orm import declarative_base
from sqlalchemy.exc import IntegrityError

Base = declarative_base()


class RedisGatewayInfo(Base):
    """与既有表名称对齐，仅此处使用 “gateway”。"""
    __tablename__ = 'redis_gateway_info'

    key = Column(String(255), primary_key=True)
    name = Column(String(255), primary_key=True)
    value = Column(Text, nullable=True)
    expire_time = Column(DateTime, nullable=True)
    last_update_time = Column(DateTime, nullable=True)


# 常量约定
TYPE_STRING = '__STRING__'
TYPE_LIST = '__LIST__'
TYPE_LOCK = '__LOCK__'
TYPE_SEMAPHORE = '__SEMAPHORE__'

# JSON 工具

def _dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


def _loads(value: Optional[str]) -> Any:
    if value is None:
        return None
    return json.loads(value)


# 时间工具

def _now() -> datetime:
    return datetime.utcnow()


def _ttl_to_expire(ttl_seconds: Optional[int]) -> Optional[datetime]:
    if ttl_seconds is None:
        return None
    return _now() + timedelta(seconds=int(ttl_seconds))


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class RedisHealthProbe:
    """健康拨测类，存储 Redis 存活状态与拨测状态，并驱动回填与清理任务。

    属性：
    - is_redis_alive: Redis 当前是否可用
    - is_probing: 是否正在进行 10s 间隔拨测
    """

    def __init__(self, redis_provider):
        self._redis_provider = redis_provider
        self.is_redis_alive: bool = True
        self.is_probing: bool = False
        self._lock = threading.RLock()

    def mark_redis_error_and_start_probe(self) -> None:
        """在捕获到 Redis 异常时调用：标记不可用并开启拨测。"""
        with self._lock:
            if not self.is_redis_alive:
                # 已不可用，无需重复
                if not self.is_probing:
                    self._start_probe_job()
                return
            self.is_redis_alive = False
            self._start_probe_job()

    def _start_probe_job(self) -> None:
        if schedule is None:
            logger.warning("schedule 未安装，无法进行健康拨测调度")
            return
        if self.is_probing:
            return
        self.is_probing = True

        def _job():
            # 使用一个轻量 ping：优先使用 ping，不可用时用 setex 探测
            try:
                redis_cli = self._redis_provider()
                try:
                    # 兼容不同客户端
                    if hasattr(redis_cli, 'ping'):
                        redis_cli.ping()
                    else:
                        redis_cli.setex('service:health:probe', 5, _dumps({'ts': time.time()}))
                except Exception:
                    # 一次失败不代表不恢复，继续等待下次
                    return
                # 成功：回填后恢复状态
                try:
                    # 使用 Redis NX 锁避免多实例同时回填
                    backfill_lock_key = 'service:health:backfill_lock'
                    token = str(uuid.uuid4())
                    should_backfill = False
                    try:
                        if hasattr(redis_cli, 'set'):
                            should_backfill = bool(redis_cli.set(backfill_lock_key, token, nx=True, ex=120))
                    except Exception:
                        should_backfill = False
                    if should_backfill:
                        try:
                            backfilled = backfill_all_to_redis(self._redis_provider)
                        finally:
                            # 释放锁（尽力而为）
                            try:
                                val = redis_cli.get(backfill_lock_key)
                                if val and (val.decode() if hasattr(val, 'decode') else val) == token:
                                    redis_cli.delete(backfill_lock_key)
                            except Exception:
                                pass
                    else:
                        backfilled = 0
                    logger.info("健康拨测成功，已回填 %s 条记录到 Redis", backfilled)
                except Exception as e:  # 回填失败不影响恢复，但记录日志
                    logger.exception("回填 Redis 失败：%s", e)
                with self._lock:
                    self.is_redis_alive = True
                    self.is_probing = False
                # 取消该拨测任务
                from schedule import CancelJob  # type: ignore
                return CancelJob
            except Exception:
                # 仍不可用，等待下次
                return

        schedule.every(10).seconds.do(_job)


class DistributedLock(ContextManager[None]):
    """分布式锁上下文（Redis 优先，MySQL 降级）。"""

    def __init__(
        self,
        name: str,
        ttl_seconds: int,
        redis_provider,
        db_session_provider,
        health_probe: RedisHealthProbe,
        block: bool = True,
        acquire_timeout_seconds: Optional[int] = None,
    ) -> None:
        self._name = name
        self._ttl = ttl_seconds
        self._redis_provider = redis_provider
        self._db_session_provider = db_session_provider
        self._health_probe = health_probe
        self._block = block
        self._timeout = acquire_timeout_seconds
        self._token = str(uuid.uuid4())

    # Redis 释放用 Lua，确保仅持有者释放
    _RELEASE_LUA = (
        "if redis.call('get', KEYS[1]) == ARGV[1] then "
        "return redis.call('del', KEYS[1]) else return 0 end"
    )

    def __enter__(self) -> None:
        deadline = time.time() + (self._timeout or 0)
        while True:
            if self._try_acquire_redis():
                return None
            if self._try_acquire_mysql():
                return None
            if not self._block:
                raise TimeoutError(f"无法获取分布式锁: {self._name}")
            if self._timeout is not None and time.time() >= deadline:
                raise TimeoutError(f"获取分布式锁超时: {self._name}")
            time.sleep(0.2)

    def __exit__(self, exc_type, exc, tb) -> None:
        if not self._try_release_redis():
            self._try_release_mysql()

    def _try_acquire_redis(self) -> bool:
        if not self._health_probe.is_redis_alive:
            return False
        try:
            cli = self._redis_provider()
            # 兼容 redis-py: set(name, value, nx=True, ex=ttl)
            ok = cli.set(self._lock_key(), self._token, nx=True, ex=self._ttl)
            return bool(ok)
        except Exception as e:
            logger.warning("Redis 加锁失败，降级 MySQL：%s", e)
            self._health_probe.mark_redis_error_and_start_probe()
            return False

    def _try_release_redis(self) -> bool:
        if not self._health_probe.is_redis_alive:
            return False
        try:
            cli = self._redis_provider()
            # 使用 Lua 保证只有持有者删除
            if hasattr(cli, 'eval'):
                cli.eval(self._RELEASE_LUA, 1, self._lock_key(), self._token)
                return True
            # 兜底：检查值再删除（非原子，尽量避免）
            val = cli.get(self._lock_key())
            if val and (val.decode() if hasattr(val, 'decode') else val) == self._token:
                cli.delete(self._lock_key())
            return True
        except Exception as e:
            logger.warning("Redis 解锁失败，尝试 MySQL 解锁：%s", e)
            self._health_probe.mark_redis_error_and_start_probe()
            return False

    def _try_acquire_mysql(self) -> bool:
        if self._db_session_provider is None:
            return False
        with self._db_session_provider() as session:
            now = _now()
            expire = now + timedelta(seconds=self._ttl)
            # 先尝试插入
            row = RedisGatewayInfo(key=self._name, name=TYPE_LOCK, value=self._token, expire_time=expire, last_update_time=now)
            session.add(row)
            try:
                session.commit()
                return True
            except IntegrityError:
                session.rollback()
            # 存在则检查是否过期，过期则夺取
            # 使用 SELECT ... FOR UPDATE 锁行
            row_db = (
                session.execute(
                    select(RedisGatewayInfo).where(
                        and_(RedisGatewayInfo.key == self._name, RedisGatewayInfo.name == TYPE_LOCK)
                    ).with_for_update()
                ).scalar_one_or_none()
            )
            if row_db is None:
                # 行被删除，重试由外层循环处理
                return False
            if row_db.expire_time is None or row_db.expire_time <= now:
                row_db.value = self._token
                row_db.expire_time = expire
                row_db.last_update_time = now
                session.add(row_db)
                try:
                    session.commit()
                    return True
                except IntegrityError:
                    session.rollback()
            return False

    def _try_release_mysql(self) -> None:
        if self._db_session_provider is None:
            return
        with self._db_session_provider() as session:
            row_db = (
                session.execute(
                    select(RedisGatewayInfo).where(
                        and_(RedisGatewayInfo.key == self._name, RedisGatewayInfo.name == TYPE_LOCK)
                    ).with_for_update()
                ).scalar_one_or_none()
            )
            if row_db and row_db.value == self._token:
                session.execute(
                    sa_delete(RedisGatewayInfo).where(
                        and_(RedisGatewayInfo.key == self._name, RedisGatewayInfo.name == TYPE_LOCK)
                    )
                )
                session.commit()

    def _lock_key(self) -> str:
        return f"service:lock:{self._name}"


class RedisSemaphore:
    """Redis 信号量（Redis 优先，MySQL 降级）。支持上下文协议。"""

    def __init__(
        self,
        name: str,
        max_leases: int,
        ttl_seconds: int,
        redis_provider,
        db_session_provider,
        health_probe: RedisHealthProbe,
    ) -> None:
        self._name = name
        self._key = f"service:semaphore:{name}"
        self._max = int(max_leases)
        self._ttl = int(ttl_seconds)
        self._redis_provider = redis_provider
        self._db_session_provider = db_session_provider
        self._health_probe = health_probe

    def acquire(self, timeout_seconds: Optional[int] = None) -> ContextManager[None]:
        token = str(uuid.uuid4())
        return _SemaphoreContext(self, token, timeout_seconds)

    # 内部方法由 _SemaphoreContext 调用
    def _try_acquire(self, token: str) -> bool:
        if self._try_acquire_redis(token):
            return True
        return self._try_acquire_mysql(token)

    def _release(self, token: str) -> None:
        if not self._try_release_redis(token):
            self._try_release_mysql(token)

    # Redis 实现：ZSET + 过期清理
    def _try_acquire_redis(self, token: str) -> bool:
        if not self._health_probe.is_redis_alive:
            return False
        try:
            cli = self._redis_provider()
            now = int(time.time())
            # 清理过期
            cli.zremrangebyscore(self._key, 0, now - self._ttl)
            # 检查容量
            if cli.zcard(self._key) < self._max:
                cli.zadd(self._key, {token: now})
                cli.expire(self._key, self._ttl)
                return True
            return False
        except Exception as e:
            logger.warning("Redis 信号量获取失败，降级 MySQL：%s", e)
            self._health_probe.mark_redis_error_and_start_probe()
            return False

    def _try_release_redis(self, token: str) -> bool:
        if not self._health_probe.is_redis_alive:
            return False
        try:
            cli = self._redis_provider()
            cli.zrem(self._key, token)
            return True
        except Exception as e:
            logger.warning("Redis 信号量释放失败，尝试 MySQL：%s", e)
            self._health_probe.mark_redis_error_and_start_probe()
            return False

    # MySQL 降级：单行保存令牌列表 [{"token": str, "ts": int}]
    def _try_acquire_mysql(self, token: str) -> bool:
        if self._db_session_provider is None:
            return False
        with self._db_session_provider() as session:
            now = int(time.time())
            expire_at = now + self._ttl
            row = (
                session.execute(
                    select(RedisGatewayInfo).where(
                        and_(RedisGatewayInfo.name == f"{TYPE_SEMAPHORE}:{self._name}",
                             RedisGatewayInfo.key == TYPE_SEMAPHORE)
                    ).with_for_update()
                ).scalar_one_or_none()
            )
            tokens: List[Dict[str, Any]] = []
            if row:
                try:
                    tokens = _loads(row.value) or []
                except Exception:
                    tokens = []
            # 清理过期
            tokens = [t for t in tokens if int(t.get('ts', 0)) > now - self._ttl]
            if len(tokens) >= self._max:
                session.rollback()
                return False
            tokens.append({"token": token, "ts": now})
            value = _dumps(tokens)
            now_dt = _now()
            if row is None:
                row = RedisGatewayInfo(
                    key=TYPE_SEMAPHORE,
                    name=f"{TYPE_SEMAPHORE}:{self._name}",
                    value=value,
                    expire_time=_ttl_to_expire(self._ttl),
                    last_update_time=now_dt,
                )
                session.add(row)
            else:
                row.value = value
                row.expire_time = _ttl_to_expire(self._ttl)
                row.last_update_time = now_dt
                session.add(row)
            session.commit()
            return True

    def _try_release_mysql(self, token: str) -> None:
        if self._db_session_provider is None:
            return
        with self._db_session_provider() as session:
            row = (
                session.execute(
                    select(RedisGatewayInfo).where(
                        and_(RedisGatewayInfo.name == f"{TYPE_SEMAPHORE}:{self._name}",
                             RedisGatewayInfo.key == TYPE_SEMAPHORE)
                    ).with_for_update()
                ).scalar_one_or_none()
            )
            if not row:
                session.rollback()
                return
            try:
                tokens = _loads(row.value) or []
            except Exception:
                tokens = []
            tokens = [t for t in tokens if t.get('token') != token]
            row.value = _dumps(tokens)
            row.last_update_time = _now()
            session.add(row)
            session.commit()


class _SemaphoreContext(ContextManager[None]):
    def __init__(self, sem: RedisSemaphore, token: str, timeout_seconds: Optional[int]):
        self._sem = sem
        self._token = token
        self._timeout = timeout_seconds

    def __enter__(self) -> None:
        deadline = time.time() + (self._timeout or 0)
        while True:
            if self._sem._try_acquire(self._token):
                return None
            if self._timeout is not None and time.time() >= deadline:
                raise TimeoutError("获取信号量超时")
            time.sleep(0.2)

    def __exit__(self, exc_type, exc, tb) -> None:
        self._sem._release(self._token)


# 异步 MySQL 写入队列
from queue import Queue, Empty

class _MySQLWriteTask:
    def __init__(self, op: str, args: Tuple[Any, ...]):
        self.op = op
        self.args = args


class RedisMySQLFallbackService:
    """Redis-MySQL 降级服务主体。"""

    def __init__(self, redis_provider=None, db_session_provider=None):
        if redis_provider is None:
            if get_redis_cache_service is None:
                raise RuntimeError("请提供 redis_provider 或在项目中实现 get_redis_cache_service")
            redis_provider = get_redis_cache_service
        if db_session_provider is None:
            if get_db_session is None:
                raise RuntimeError("请提供 db_session_provider 或在项目中实现 get_db_session")
            db_session_provider = get_db_session

        self._redis_provider = redis_provider
        self._db_session_provider = db_session_provider
        self._health = RedisHealthProbe(redis_provider)

        self._write_queue: "Queue[_MySQLWriteTask]" = Queue()
        self._writer_thread = threading.Thread(target=self._mysql_writer_loop, name="mysql-writer", daemon=True)
        self._writer_thread.start()

    # ---------------------------- 对外 API：分布式能力 ----------------------------
    def distributed_lock(
        self, name: str, ttl_seconds: int = 30, block: bool = True, acquire_timeout_seconds: Optional[int] = None
    ) -> DistributedLock:
        return DistributedLock(
            name=name,
            ttl_seconds=ttl_seconds,
            redis_provider=self._redis_provider,
            db_session_provider=self._db_session_provider,
            health_probe=self._health,
            block=block,
            acquire_timeout_seconds=acquire_timeout_seconds,
        )

    def semaphore(self, name: str, max_leases: int, ttl_seconds: int = 60) -> RedisSemaphore:
        return RedisSemaphore(
            name=name,
            max_leases=max_leases,
            ttl_seconds=ttl_seconds,
            redis_provider=self._redis_provider,
            db_session_provider=self._db_session_provider,
            health_probe=self._health,
        )

    # ---------------------------- 对外 API：写入（先判可用 -> 写 Redis -> 异步写 MySQL） ----------------------------
    def set(
        self,
        key: str,
        value: Any,
        ex: Optional[int] = None,
        px: Optional[int] = None,
        nx: Optional[bool] = None,
        xx: Optional[bool] = None,
        keepttl: Optional[bool] = None,
    ) -> bool:
        """兼容 redis-py 的 set 接口：
        - 参数：key, value, ex(秒), px(毫秒), nx(True), xx(True), keepttl(True)
        - 返回：True/False（参照 redis 行为）
        写入规则：
        1) 先判断 redis 可用 -> 写 redis；
        2) 无论是否成功，异步写入 MySQL（带条件）；
        3) MySQL 的 nx/xx 语义尽可能模拟（仅用于镜像，不影响上层成功判定）。
        """
        payload = _dumps(value)
        ttl_sec: Optional[int] = None
        if ex is not None:
            ttl_sec = int(ex)
        elif px is not None:
            ttl_sec = max(1, int(px) // 1000)
        wrote = False
        if self._health.is_redis_alive:
            try:
                cli = self._redis_provider()
                if hasattr(cli, 'set'):
                    kwargs: Dict[str, Any] = {}
                    if ttl_sec is not None:
                        kwargs['ex'] = ttl_sec
                    if nx:
                        kwargs['nx'] = True
                    if xx:
                        kwargs['xx'] = True
                    # keepttl: 某些客户端支持
                    if keepttl:
                        kwargs['keepttl'] = True
                    res = cli.set(key, payload, **kwargs)
                    wrote = bool(res)
                else:
                    # 兜底：仅写值
                    cli.set(key, payload)
                    if ttl_sec is not None:
                        cli.expire(key, ttl_sec)
                    wrote = True
            except Exception as e:
                logger.warning("Redis set 失败：%s", e)
                self._health.mark_redis_error_and_start_probe()
        # 异步 MySQL 镜像（条件 set）
        self._enqueue('set_string_conditional', (key, payload, ttl_sec, bool(nx), bool(xx), bool(keepttl)))
        return wrote
    # setex 支持两种调用方式：set_ex(key, ttl, value) 或 set_ex(key, value, ttl)
    def set_ex(self, key: str, *args: Any, **kwargs: Any) -> None:
        ttl: Optional[int]
        value: Any
        if len(args) == 2:
            # 可能是 (ttl, value) 或 (value, ttl)
            a, b = args
            if isinstance(a, int) and not isinstance(b, int):
                ttl, value = a, b
            elif isinstance(b, int) and not isinstance(a, int):
                value, ttl = a, b
            else:
                raise TypeError("set_ex 参数不明确，应为 (ttl:int, value:any) 或 (value:any, ttl:int)")
        else:
            ttl = kwargs.get('ttl')
            value = kwargs.get('value')
            if ttl is None or value is None:
                raise TypeError("set_ex 需要提供 ttl 与 value")
        payload = _dumps(value)
        # Redis 写入
        if self._health.is_redis_alive:
            try:
                cli = self._redis_provider()
                # 兼容 redis-py: setex(name, time, value)
                if hasattr(cli, 'setex'):
                    cli.setex(key, int(ttl), payload)
                else:
                    cli.set(key, payload, ex=int(ttl))
            except Exception as e:
                logger.warning("Redis setex 失败：%s", e)
                self._health.mark_redis_error_and_start_probe()
        # 异步写入 MySQL
        self._enqueue('set_string', (key, payload, int(ttl)))

    def hset(self, name: str, key: str, value: Any, ttl: Optional[int] = None) -> None:
        payload = _dumps(value)
        if self._health.is_redis_alive:
            try:
                cli = self._redis_provider()
                cli.hset(name, key, payload)
                if ttl is not None:
                    cli.expire(name, int(ttl))
            except Exception as e:
                logger.warning("Redis hset 失败：%s", e)
                self._health.mark_redis_error_and_start_probe()
        self._enqueue('hset', (name, key, payload, ttl))

    def hdel(self, name: str, key: str) -> None:
        if self._health.is_redis_alive:
            try:
                cli = self._redis_provider()
                cli.hdel(name, key)
            except Exception as e:
                logger.warning("Redis hdel 失败：%s", e)
                self._health.mark_redis_error_and_start_probe()
        self._enqueue('hdel', (name, key))

    def rpush(self, name: str, *values: Any, ttl: Optional[int] = None) -> None:
        if not values:
            return
        payloads = [_dumps(v) for v in values]
        if self._health.is_redis_alive:
            try:
                cli = self._redis_provider()
                cli.rpush(name, *payloads)
                if ttl is not None:
                    cli.expire(name, int(ttl))
            except Exception as e:
                logger.warning("Redis rpush 失败：%s", e)
                self._health.mark_redis_error_and_start_probe()
        self._enqueue('rpush_multi', (name, payloads, ttl))

    def lpush(self, name: str, *values: Any, ttl: Optional[int] = None) -> None:
        if not values:
            return
        payloads = [_dumps(v) for v in values]
        if self._health.is_redis_alive:
            try:
                cli = self._redis_provider()
                cli.lpush(name, *payloads)
                if ttl is not None:
                    cli.expire(name, int(ttl))
            except Exception as e:
                logger.warning("Redis lpush 失败：%s", e)
                self._health.mark_redis_error_and_start_probe()
        self._enqueue('lpush_multi', (name, payloads, ttl))

    def lrem(self, name: str, count: int, value: Any) -> None:
        payload = _dumps(value)
        if self._health.is_redis_alive:
            try:
                cli = self._redis_provider()
                cli.lrem(name, int(count), payload)
            except Exception as e:
                logger.warning("Redis lrem 失败：%s", e)
                self._health.mark_redis_error_and_start_probe()
        self._enqueue('lrem', (name, count, payload))

    def lpop(self, name: str, count: Optional[int] = None) -> List[Any]:
        # Redis 优先返回数据
        popped: List[Any] = []
        if self._health.is_redis_alive:
            try:
                cli = self._redis_provider()
                if count is None:
                    item = cli.lpop(name)
                    if item is not None:
                        popped = [_loads(item)]
                else:
                    items = cli.lpop(name, count)
                    if items:
                        popped = [_loads(x) for x in items]
            except Exception as e:
                logger.warning("Redis lpop 失败：%s", e)
                self._health.mark_redis_error_and_start_probe()
        # 异步落库（根据 pop 结果调整 DB 列表）
        if popped:
            self._enqueue('lpop', (name, len(popped)))
            return popped
        # Redis 不可用或无元素，降级 MySQL 同步执行并返回
        with self._db_session_provider() as session:
            lst = _db_list_get(session, name)
            n = len(lst) if count is None else min(len(lst), int(count))
            result = lst[:n]
            _db_list_set(session, name, lst[n:], None)
            session.commit()
            return result

    def expire(self, key: str, ttl: int) -> None:
        if self._health.is_redis_alive:
            try:
                cli = self._redis_provider()
                cli.expire(key, int(ttl))
            except Exception as e:
                logger.warning("Redis expire 失败：%s", e)
                self._health.mark_redis_error_and_start_probe()
        self._enqueue('expire', (key, int(ttl)))

    def delete(self, key: str) -> None:
        if self._health.is_redis_alive:
            try:
                cli = self._redis_provider()
                cli.delete(key)
            except Exception as e:
                logger.warning("Redis delete 失败：%s", e)
                self._health.mark_redis_error_and_start_probe()
        self._enqueue('delete', (key,))

    # ---------------------------- 对外 API：读取（Redis 优先，异常时降级 MySQL 并先清理过期） ----------------------------
    def get(self, key: str) -> Any:
        if self._health.is_redis_alive:
            try:
                cli = self._redis_provider()
                val = cli.get(key)
                return _loads(val)
            except Exception as e:
                logger.warning("Redis get 失败：%s", e)
                self._health.mark_redis_error_and_start_probe()
        # 降级 MySQL
        with self._db_session_provider() as session:
            cleanup_expired_records(session)
            row = (
                session.execute(
                    select(RedisGatewayInfo).where(
                        and_(
                            RedisGatewayInfo.name == TYPE_STRING,
                            RedisGatewayInfo.key == key,
                            or_(RedisGatewayInfo.expire_time.is_(None), (RedisGatewayInfo.expire_time > _now())),
                        )
                    )
                ).scalar_one_or_none()
            )
            return _loads(row.value) if row else None

    def hget(self, name: str, key: str) -> Any:
        if self._health.is_redis_alive:
            try:
                cli = self._redis_provider()
                val = cli.hget(name, key)
                return _loads(val)
            except Exception as e:
                logger.warning("Redis hget 失败：%s", e)
                self._health.mark_redis_error_and_start_probe()
        # 降级 MySQL：先清理过期
        with self._db_session_provider() as session:
            cleanup_expired_records(session)
            row = (
                session.execute(
                    select(RedisGatewayInfo).where(
                        and_(
                            RedisGatewayInfo.name == name,
                            RedisGatewayInfo.key == key,
                            or_(RedisGatewayInfo.expire_time.is_(None), (RedisGatewayInfo.expire_time > _now())),
                        )
                    )
                ).scalar_one_or_none()
            )
            return _loads(row.value) if row else None

    def hgetall(self, name: str) -> Dict[str, Any]:
        if self._health.is_redis_alive:
            try:
                cli = self._redis_provider()
                raw = cli.hgetall(name)
                # 兼容 redis-py 返回 bytes
                return {(
                    k.decode() if hasattr(k, 'decode') else k
                ): _loads(v) for k, v in raw.items()}
            except Exception as e:
                logger.warning("Redis hgetall 失败：%s", e)
                self._health.mark_redis_error_and_start_probe()
        with self._db_session_provider() as session:
            cleanup_expired_records(session)
            rows = (
                session.execute(
                    select(RedisGatewayInfo).where(
                        and_(
                            RedisGatewayInfo.name == name,
                            or_(RedisGatewayInfo.expire_time.is_(None), (RedisGatewayInfo.expire_time > _now())),
                        )
                    )
                ).scalars().all()
            )
            return {r.key: _loads(r.value) for r in rows}

    def lrange(self, name: str, start: int = 0, end: int = -1) -> List[Any]:
        if self._health.is_redis_alive:
            try:
                cli = self._redis_provider()
                raw = cli.lrange(name, int(start), int(end))
                return [_loads(x) for x in raw]
            except Exception as e:
                logger.warning("Redis lrange 失败：%s", e)
                self._health.mark_redis_error_and_start_probe()
        with self._db_session_provider() as session:
            cleanup_expired_records(session)
            lst = _db_list_get(session, name)
            # Redis 端 end=-1 表示末尾
            if end == -1:
                end = len(lst) - 1
            end = min(end, len(lst) - 1)
            if start < 0:
                start = max(0, len(lst) + start)
            if end < start:
                return []
            return lst[start:end + 1]

    # 为兼容用户列出的大小写写法
    def Lrange(self, name: str, start: int = 0, end: int = -1) -> List[Any]:  # noqa: N802
        return self.lrange(name, start, end)

    def lindex(self, name: str, index: int) -> Any:
        if self._health.is_redis_alive:
            try:
                cli = self._redis_provider()
                raw = cli.lindex(name, int(index))
                return _loads(raw)
            except Exception as e:
                logger.warning("Redis lindex 失败：%s", e)
                self._health.mark_redis_error_and_start_probe()
        with self._db_session_provider() as session:
            cleanup_expired_records(session)
            lst = _db_list_get(session, name)
            if index < 0:
                index = len(lst) + index
            if index < 0 or index >= len(lst):
                return None
            return lst[index]

    def llen(self, name: str) -> int:
        if self._health.is_redis_alive:
            try:
                cli = self._redis_provider()
                return int(cli.llen(name))
            except Exception as e:
                logger.warning("Redis llen 失败：%s", e)
                self._health.mark_redis_error_and_start_probe()
        with self._db_session_provider() as session:
            cleanup_expired_records(session)
            lst = _db_list_get(session, name)
            return len(lst)

    def exists(self, key: str) -> bool:
        if self._health.is_redis_alive:
            try:
                cli = self._redis_provider()
                return bool(cli.exists(key))
            except Exception as e:
                logger.warning("Redis exists 失败：%s", e)
                self._health.mark_redis_error_and_start_probe()
        with self._db_session_provider() as session:
            cleanup_expired_records(session)
            # 兼容三类：string、list、hash（任意存在即 True）
            count = (
                session.execute(
                    select(func.count()).where(
                        and_(
                            RedisGatewayInfo.name == TYPE_STRING,
                            RedisGatewayInfo.key == key,
                            or_(RedisGatewayInfo.expire_time.is_(None), (RedisGatewayInfo.expire_time > _now())),
                        )
                    )
                ).scalar_one()
            )
            if count:
                return True
            # list/hash 名称在 name 列
            count2 = (
                session.execute(
                    select(func.count()).where(
                        and_(
                            RedisGatewayInfo.name == key,
                            or_(RedisGatewayInfo.expire_time.is_(None), (RedisGatewayInfo.expire_time > _now())),
                        )
                    )
                ).scalar_one()
            )
            return bool(count2)

    # ---------------------------- 内部：异步 MySQL 写入实现 ----------------------------
    def _enqueue(self, op: str, args: Tuple[Any, ...]) -> None:
        self._write_queue.put(_MySQLWriteTask(op, args))

    def _mysql_writer_loop(self) -> None:
        while True:
            try:
                task = self._write_queue.get(timeout=1.0)
            except Empty:
                continue
            try:
                with self._db_session_provider() as session:
                    if task.op == 'set_string':
                        _db_set_string(session, key=task.args[0], payload=task.args[1], ttl=task.args[2])
                    elif task.op == 'hset':
                        _db_hset(session, name=task.args[0], key=task.args[1], payload=task.args[2], ttl=task.args[3])
                    elif task.op == 'hdel':
                        _db_hdel(session, name=task.args[0], key=task.args[1])
                    elif task.op == 'set_string_conditional':
                        _db_set_string_conditional(
                            session,
                            key=task.args[0],
                            payload=task.args[1],
                            ttl=task.args[2],
                            nx=bool(task.args[3]),
                            xx=bool(task.args[4]),
                            keepttl=bool(task.args[5]),
                        )
                    elif task.op == 'rpush_multi':
                        _db_list_multi_push(session, name=task.args[0], payloads=list(task.args[1]), left=False, ttl=task.args[2])
                    elif task.op == 'lpush_multi':
                        _db_list_multi_push(session, name=task.args[0], payloads=list(task.args[1]), left=True, ttl=task.args[2])
                    elif task.op == 'lrem':
                        _db_list_lrem(session, name=task.args[0], count=int(task.args[1]), payload=task.args[2])
                    elif task.op == 'lpop':
                        _db_list_pop_n(session, name=task.args[0], n=int(task.args[1]))
                    elif task.op == 'expire':
                        _db_expire_key(session, key=task.args[0], ttl=int(task.args[1]))
                    elif task.op == 'delete':
                        _db_delete_key(session, key=task.args[0])
                    session.commit()
            except Exception as e:
                logger.exception("MySQL 异步写入失败：op=%s args=%s err=%s", task.op, task.args, e)
            finally:
                self._write_queue.task_done()


# ---------------------------- MySQL 工具函数 ----------------------------

def cleanup_expired_records(session) -> int:
    """清理过期数据，返回删除条数。"""
    now = _now()
    # 由于 SQLAlchemy Core 删除无法直接返回影响数（取决于方言），这里先找后删
    rows = (
        session.execute(
            select(RedisGatewayInfo).where(
                RedisGatewayInfo.expire_time.is_not(None),
            )
        ).scalars().all()
    )
    expired_ids: List[Tuple[str, str]] = []
    for r in rows:
        if r.expire_time and r.expire_time <= now:
            expired_ids.append((r.key, r.name))
    if expired_ids:
        for k, n in expired_ids:
            session.execute(
                sa_delete(RedisGatewayInfo).where(
                    and_(RedisGatewayInfo.key == k, RedisGatewayInfo.name == n)
                )
            )
        session.commit()
    return len(expired_ids)


def _db_set_string(session, key: str, payload: str, ttl: Optional[int]) -> None:
    now = _now()
    row = (
        session.execute(
            select(RedisGatewayInfo).where(
                and_(RedisGatewayInfo.key == key, RedisGatewayInfo.name == TYPE_STRING)
            ).with_for_update()
        ).scalar_one_or_none()
    )
    expire = _ttl_to_expire(ttl)
    if row is None:
        row = RedisGatewayInfo(key=key, name=TYPE_STRING, value=payload, expire_time=expire, last_update_time=now)
        session.add(row)
    else:
        row.value = payload
        row.expire_time = expire
        row.last_update_time = now
        session.add(row)


def _db_set_string_conditional(
    session,
    key: str,
    payload: str,
    ttl: Optional[int],
    nx: bool,
    xx: bool,
    keepttl: bool,
) -> None:
    """MySQL 镜像 set 条件语义：
    - nx: 仅当不存在时写入
    - xx: 仅当存在时写入
    - keepttl: 保持过期时间（若存在），否则使用 ttl
    两者同时为 True 时按 redis 语义通常为无效，这里直接不写入。
    """
    now = _now()
    row = (
        session.execute(
            select(RedisGatewayInfo).where(
                and_(RedisGatewayInfo.key == key, RedisGatewayInfo.name == TYPE_STRING)
            ).with_for_update()
        ).scalar_one_or_none()
    )
    if nx and xx:
        return
    if nx and row is not None:
        return
    if xx and row is None:
        return
    if row is None:
        expire = _ttl_to_expire(ttl)
        row = RedisGatewayInfo(key=key, name=TYPE_STRING, value=payload, expire_time=expire, last_update_time=now)
        session.add(row)
        return
    # 存在
    row.value = payload
    if keepttl:
        # 保持现有 expire_time，不修改
        pass
    else:
        row.expire_time = _ttl_to_expire(ttl)
    row.last_update_time = now
    session.add(row)


def _db_hset(session, name: str, key: str, payload: str, ttl: Optional[int]) -> None:
    now = _now()
    row = (
        session.execute(
            select(RedisGatewayInfo).where(
                and_(RedisGatewayInfo.key == key, RedisGatewayInfo.name == name)
            ).with_for_update()
        ).scalar_one_or_none()
    )
    expire = _ttl_to_expire(ttl)
    if row is None:
        row = RedisGatewayInfo(key=key, name=name, value=payload, expire_time=expire, last_update_time=now)
        session.add(row)
    else:
        row.value = payload
        row.expire_time = expire
        row.last_update_time = now
        session.add(row)


def _db_hdel(session, name: str, key: str) -> None:
    session.execute(
        sa_delete(RedisGatewayInfo).where(
            and_(RedisGatewayInfo.key == key, RedisGatewayInfo.name == name)
        )
    )


def _db_list_get(session, name: str) -> List[Any]:
    row = (
        session.execute(
            select(RedisGatewayInfo).where(
                and_(RedisGatewayInfo.key == TYPE_LIST, RedisGatewayInfo.name == name,
                     (RedisGatewayInfo.expire_time.is_(None) | (RedisGatewayInfo.expire_time > _now())))
            )
        ).scalar_one_or_none()
    )
    if not row or not row.value:
        return []
    try:
        lst = _loads(row.value)
        return lst if isinstance(lst, list) else []
    except Exception:
        return []


def _db_list_set(session, name: str, lst: List[Any], ttl: Optional[int]) -> None:
    now = _now()
    row = (
        session.execute(
            select(RedisGatewayInfo).where(
                and_(RedisGatewayInfo.key == TYPE_LIST, RedisGatewayInfo.name == name)
            ).with_for_update()
        ).scalar_one_or_none()
    )
    payload = _dumps(lst)
    expire = _ttl_to_expire(ttl)
    if row is None:
        row = RedisGatewayInfo(key=TYPE_LIST, name=name, value=payload, expire_time=expire, last_update_time=now)
        session.add(row)
    else:
        row.value = payload
        row.expire_time = expire
        row.last_update_time = now
        session.add(row)


def _db_list_push(session, name: str, payload: str, left: bool, ttl: Optional[int]) -> None:
    lst = _db_list_get(session, name)
    item = _loads(payload)
    if left:
        lst.insert(0, item)
    else:
        lst.append(item)
    _db_list_set(session, name, lst, ttl)


def _db_list_multi_push(session, name: str, payloads: List[str], left: bool, ttl: Optional[int]) -> None:
    lst = _db_list_get(session, name)
    items = [_loads(p) for p in payloads]
    if left:
        lst = items + lst
    else:
        lst.extend(items)
    _db_list_set(session, name, lst, ttl)


def _db_list_lrem(session, name: str, count: int, payload: str) -> None:
    lst = _db_list_get(session, name)
    target = _loads(payload)
    removed = 0
    if count == 0:
        lst = [x for x in lst if not _json_equal(x, target)]
    elif count > 0:
        new_lst: List[Any] = []
        for x in lst:
            if removed < count and _json_equal(x, target):
                removed += 1
                continue
            new_lst.append(x)
        lst = new_lst
    else:  # count < 0 从尾部开始
        count = -count
        new_lst_rev: List[Any] = []
        for x in reversed(lst):
            if removed < count and _json_equal(x, target):
                removed += 1
                continue
            new_lst_rev.append(x)
        lst = list(reversed(new_lst_rev))
    _db_list_set(session, name, lst, None)


def _db_list_pop_n(session, name: str, n: int) -> None:
    lst = _db_list_get(session, name)
    lst = lst[n:]
    _db_list_set(session, name, lst, None)


def _db_expire_key(session, key: str, ttl: int) -> None:
    expire = _ttl_to_expire(ttl)
    now = _now()
    # string
    row = (
        session.execute(
            select(RedisGatewayInfo).where(
                and_(RedisGatewayInfo.key == key, RedisGatewayInfo.name == TYPE_STRING)
            ).with_for_update()
        ).scalar_one_or_none()
    )
    if row:
        row.expire_time = expire
        row.last_update_time = now
        session.add(row)
    # list/hash：按 name 匹配
    rows = (
        session.execute(
            select(RedisGatewayInfo).where(RedisGatewayInfo.name == key).with_for_update()
        ).scalars().all()
    )
    for r in rows:
        r.expire_time = expire
        r.last_update_time = now
        session.add(r)


def _db_delete_key(session, key: str) -> None:
    # 删除 string
    session.execute(
        sa_delete(RedisGatewayInfo).where(
            and_(RedisGatewayInfo.key == key, RedisGatewayInfo.name == TYPE_STRING)
        )
    )
    # 删除 list/hash（name=key）
    session.execute(
        sa_delete(RedisGatewayInfo).where(
            RedisGatewayInfo.name == key
        )
    )


def _json_equal(a: Any, b: Any) -> bool:
    try:
        return _dumps(a) == _dumps(b)
    except Exception:
        return a == b


# ---------------------------- 回填到 Redis ----------------------------

def backfill_all_to_redis(redis_provider) -> int:
    """将 MySQL 中未过期的数据全部回写到 Redis。返回回填条数。

    注意：为避免多实例重复回填，建议调用方在外层使用分布式锁保护（见 start_background_tasks）。
    """
    if get_db_session is None:
        return 0
    total = 0
    with get_db_session() as session:
        now = _now()
        rows = (
            session.execute(
                select(RedisGatewayInfo).where(
                    or_(RedisGatewayInfo.expire_time.is_(None), (RedisGatewayInfo.expire_time > now))
                )
            ).scalars().all()
        )
    if not rows:
        return 0
    cli = redis_provider()
    for r in rows:
        try:
            if r.name == TYPE_STRING:
                ttl = _expire_to_ttl(r.expire_time)
                if hasattr(cli, 'setex') and ttl is not None:
                    cli.setex(r.key, ttl, r.value)
                else:
                    if ttl is not None:
                        cli.set(r.key, r.value, ex=ttl)
                    else:
                        cli.set(r.key, r.value)
                total += 1
            elif r.key == TYPE_LIST:
                # 列表整表重建
                lst = _loads(r.value) or []
                cli.delete(r.name)
                if lst:
                    cli.rpush(r.name, *[_dumps(x) for x in lst])
                ttl = _expire_to_ttl(r.expire_time)
                if ttl is not None:
                    cli.expire(r.name, ttl)
                total += 1
            elif r.name.startswith(f"{TYPE_SEMAPHORE}:") or r.name == TYPE_LOCK:
                # 锁/信号量不需要回填（运行态资源），跳过
                continue
            else:
                # 其余视作 hash 的一个 field
                cli.hset(r.name, r.key, r.value)
                ttl = _expire_to_ttl(r.expire_time)
                if ttl is not None:
                    cli.expire(r.name, ttl)
                total += 1
        except Exception as e:
            logger.warning("回填单条失败 name=%s key=%s: %s", r.name, r.key, e)
    return total


def _expire_to_ttl(expire_time: Optional[datetime]) -> Optional[int]:
    if expire_time is None:
        return None
    delta = int((expire_time - _now()).total_seconds())
    return delta if delta > 0 else None


# ---------------------------- 后台调度 ----------------------------
_scheduler_started = False
_scheduler_lock = threading.Lock()


def start_background_tasks(service: RedisMySQLFallbackService) -> None:
    """在主程序启动时调用：
    - 每 60 秒清理一次 MySQL 过期数据（分布式锁保护，确保仅一个实例执行）。
    - 允许健康拨测（10 秒一次）在 Redis 异常时自动启动（由 RedisHealthProbe 管理）。
    """
    global _scheduler_started
    if schedule is None:
        logger.warning("schedule 未安装，无法启动后台定时任务")
        return
    with _scheduler_lock:
        if _scheduler_started:
            return
        _scheduler_started = True

        def _cleanup_job():
            try:
                with service.distributed_lock('service:cleanup', ttl_seconds=30):
                    with service._db_session_provider() as session:
                        removed = cleanup_expired_records(session)
                        if removed:
                            logger.info("清理过期数据 %s 条", removed)
            except Exception as e:
                logger.exception("清理任务失败：%s", e)

        schedule.every(60).seconds.do(_cleanup_job)

        # 调度线程（轻量循环）
        t = threading.Thread(target=_schedule_runner, name="schedule-runner", daemon=True)
        t.start()


def _schedule_runner() -> None:
    while True:
        try:
            schedule.run_pending()
        except Exception as e:  # pragma: no cover
            logger.exception("schedule 运行失败：%s", e)
        time.sleep(1)


# ---------------------------- 工厂方法 ----------------------------
_service_singleton_lock = threading.Lock()
_service_singleton: Optional[RedisMySQLFallbackService] = None


def get_fallback_service() -> RedisMySQLFallbackService:
    """获取（或创建）单例服务实例。"""
    global _service_singleton
    with _service_singleton_lock:
        if _service_singleton is None:
            _service_singleton = RedisMySQLFallbackService()
        return _service_singleton

