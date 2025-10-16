# -*- coding: utf-8 -*-
import threading
import time
from typing import Optional

import schedule

from .locks import DistributedLock
from .db import cleanup_expired


class HealthMonitor:
    """
    健康拨测类：
    - 维护 redis 是否可用（通过网关传入）
    - 当 redis 不可用时，每 10 秒拨测一次，恢复后回灌 MySQL 数据到 Redis
    - 永久任务：每 60 秒清理一次 MySQL 过期数据
    - 所有定时任务在执行前，均用分布式锁保证只有一个实例在执行
    """

    def __init__(self, service, lock: Optional[DistributedLock] = None):
        # 兼容：HealthMonitor 现在绑定 Service
        self.service = service
        self.redis = service.redis
        self.lock = lock or DistributedLock(self.redis, service.get_db_session)
        self._probing = False
        self._stop_event = threading.Event()
        self._schedule_thread = threading.Thread(target=self._schedule_loop, name="health-schedule", daemon=True)

        # 固定任务：每 60 秒清理一次过期数据
        schedule.every(60).seconds.do(self._cleanup_job)

    @property
    def probing(self) -> bool:
        return self._probing

    def start(self):
        if not self._schedule_thread.is_alive():
            self._schedule_thread.start()

    def stop(self):
        self._stop_event.set()

    def on_redis_error(self):
        """当网关发现 Redis 异常时调用，开启 10s 拨测。"""
        if self._probing:
            return
        self._probing = True
        schedule.every(10).seconds.do(self._probe_job)

    # ----------------- 定时任务 -----------------
    def _schedule_loop(self):
        while not self._stop_event.is_set():
            try:
                schedule.run_pending()
            except Exception:
                # 不让调度线程因异常停止
                pass
            time.sleep(1)

    def _cleanup_job(self):
        # 尝试获取分布式锁，避免多实例重复清理
        try:
            with self.lock.context("mysql_cleanup", ttl_seconds=55, blocking=False) as _lk:  # noqa: F841
                with self.service.get_db_session() as session:
                    cleanup_expired(session)
        except TimeoutError:
            # 未拿到锁，跳过本轮
            return
        except Exception:
            # 清理失败不影响主流程
            return

    def _probe_job(self):
        # 只允许一个实例探测
        try:
            with self.lock.context("redis_probe", ttl_seconds=9, blocking=False) as _lk:  # noqa: F841
                ok = self.redis.ping()
                if ok:
                    # 恢复：回灌
                    self._rehydrate()
                    # 标记可用，停止拨测
                    self.redis.mark_available()
                    self._probing = False
                    # 取消此任务：通过返回 schedule.CancelJob
                    return schedule.CancelJob
        except TimeoutError:
            return
        except Exception:
            return

    def _rehydrate(self):
        """从 MySQL 将未过期数据回灌到 Redis。"""
        try:
            self.service.rehydrate_from_mysql()
        except Exception:
            # 回灌失败不致命，等待下次
            pass
