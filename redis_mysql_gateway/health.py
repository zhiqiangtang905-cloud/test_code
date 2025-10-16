from __future__ import annotations
"""Health monitor class for Redis with MySQL cleanup and restore.

- Keeps a shared state of Redis availability and probing status
- When Redis operations fail, mark unavailable and start probing every 10s
- On recovery, backfill data from MySQL to Redis and stop probing
- Also runs a periodic cleanup of expired rows every 60s using schedule

Only one instance should perform cleanup/backfill jobs across multiple
application instances. We ensure single-execution via a `DistributedLock`.
"""

import json
import logging
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import schedule
from redis import Redis
from redis.exceptions import RedisError

from .db import RedisGatewayInfo, get_session
from .gateway import RedisState
from .lock import DistributedLock

logger = logging.getLogger(__name__)


@dataclass
class RedisHealthMonitor:
    redis: Redis
    state: RedisState
    lock_name: str = "redis-gateway-health-lock"

    def __post_init__(self) -> None:
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._run, name="redis-health-monitor", daemon=True)
        self._thread.start()

    # Main loop drives schedule
    def _run(self) -> None:
        # Schedule periodic cleanup every 60s
        schedule.every(60).seconds.do(self._periodic_cleanup)
        # Probe every 10s when in probing mode
        schedule.every(10).seconds.do(self._maybe_probe)
        while not self._stop_event.is_set():
            schedule.run_pending()
            self._stop_event.wait(1.0)

    def stop(self) -> None:
        self._stop_event.set()

    # Scheduled jobs --------------------------------------------------------
    def _periodic_cleanup(self) -> None:
        # Ensure only one instance performs cleanup
        lock = DistributedLock(self.redis, self.lock_name, ttl_seconds=30, blocking=False)
        if not lock.acquire(blocking=False):
            return
        try:
            self._cleanup_mysql_expired()
        finally:
            lock.release()

    def _maybe_probe(self) -> None:
        # probe only if marked unavailable or explicitly probing
        if not self.state.available or self.state.probing:
            self._probe_and_maybe_restore()

    # Implementation details -----------------------------------------------
    def _cleanup_mysql_expired(self) -> int:
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

    def _probe_and_maybe_restore(self) -> None:
        # Ensure only one instance probes/restore at a time
        lock = DistributedLock(self.redis, f"{self.lock_name}:probe", ttl_seconds=30, blocking=False)
        if not lock.acquire(blocking=False):
            return
        try:
            try:
                self.redis.ping()
                # If successful, restore and mark available
                self._restore_from_mysql()
                self.state.available = True
                self.state.probing = False
            except RedisError:
                # Still down; remain unavailable
                self.state.available = False
                self.state.probing = True
        finally:
            lock.release()

    def _restore_from_mysql(self) -> None:
        # Load all rows and write back to Redis
        with get_session() as session:
            rows = session.query(RedisGatewayInfo).all()
            pipe = self.redis.pipeline(transaction=True)
            for row in rows:
                if row.name == "_kv_":
                    # KV
                    if row.expire_time is not None:
                        ttl = int(max(0, (row.expire_time - datetime.now(timezone.utc)).total_seconds()))
                    else:
                        ttl = None
                    if ttl and ttl > 0:
                        pipe.set(row.key, row.value, ex=ttl)
                    else:
                        pipe.set(row.key, row.value)
                elif row.key == "__list__":
                    try:
                        items = json.loads(row.value) if row.value else []
                    except Exception:
                        items = []
                    # reset list by deleting then rpush
                    pipe.delete(row.name)
                    if items:
                        for item in items:
                            pipe.rpush(row.name, json.dumps(item))
                else:
                    # Hash field
                    pipe.hset(row.name, row.key, row.value)
            try:
                pipe.execute()
            except RedisError:
                # If restore fails mid-way, we'll try again next probe
                logger.exception("Redis restore failed; will retry later")
