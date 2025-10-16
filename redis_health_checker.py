"""
Redis健康检查类
用于监控Redis的可用状态，并在Redis不可用时进行健康拨测
"""
import threading
import time
import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


class RedisHealthChecker:
    """
    Redis健康检查器
    负责维护Redis的存活状态，并在Redis故障时定期进行健康拨测
    """
    
    def __init__(self):
        # Redis存活状态
        self._is_redis_alive = True
        
        # 当前是否正在拨测
        self._is_probing = False
        
        # 拨测线程
        self._probe_thread: Optional[threading.Thread] = None
        
        # 线程锁，保证状态修改的线程安全
        self._lock = threading.Lock()
        
        # 拨测停止标志
        self._stop_probe = False
        
    @property
    def is_redis_alive(self) -> bool:
        """获取Redis存活状态"""
        with self._lock:
            return self._is_redis_alive
    
    @property
    def is_probing(self) -> bool:
        """获取当前拨测状态"""
        with self._lock:
            return self._is_probing
    
    def mark_redis_down(self):
        """
        标记Redis为不可用状态
        当Redis操作失败时调用此方法
        """
        with self._lock:
            if self._is_redis_alive:
                logger.warning(f"[{datetime.now()}] Redis服务标记为不可用，准备启动健康拨测")
                self._is_redis_alive = False
    
    def mark_redis_up(self):
        """
        标记Redis为可用状态
        当Redis拨测成功时调用此方法
        """
        with self._lock:
            if not self._is_redis_alive:
                logger.info(f"[{datetime.now()}] Redis服务恢复可用")
                self._is_redis_alive = True
    
    def start_probing(self, probe_func, interval: int = 10):
        """
        启动健康拨测
        
        Args:
            probe_func: 拨测函数，应返回True表示Redis可用，False表示不可用
            interval: 拨测间隔时间（秒），默认10秒
        """
        with self._lock:
            # 如果已经在拨测中，不重复启动
            if self._is_probing:
                logger.debug("健康拨测已在运行中，跳过启动")
                return
            
            self._is_probing = True
            self._stop_probe = False
        
        # 在新线程中启动拨测
        self._probe_thread = threading.Thread(
            target=self._probe_loop,
            args=(probe_func, interval),
            daemon=True,
            name="RedisHealthProbe"
        )
        self._probe_thread.start()
        logger.info(f"启动Redis健康拨测线程，间隔{interval}秒")
    
    def stop_probing(self):
        """停止健康拨测"""
        with self._lock:
            if not self._is_probing:
                return
            
            self._stop_probe = True
            self._is_probing = False
        
        # 等待拨测线程结束
        if self._probe_thread and self._probe_thread.is_alive():
            self._probe_thread.join(timeout=5)
        
        logger.info("Redis健康拨测已停止")
    
    def _probe_loop(self, probe_func, interval: int):
        """
        拨测循环
        每隔interval秒执行一次拨测
        """
        logger.info("Redis健康拨测循环开始")
        
        while True:
            # 检查停止标志
            if self._stop_probe:
                break
            
            # 只有在Redis不可用时才继续拨测
            if not self.is_redis_alive:
                try:
                    # 执行拨测
                    logger.debug(f"[{datetime.now()}] 执行Redis健康拨测")
                    is_healthy = probe_func()
                    
                    if is_healthy:
                        logger.info(f"[{datetime.now()}] Redis健康拨测成功，服务已恢复")
                        self.mark_redis_up()
                        # Redis恢复后停止拨测
                        break
                    else:
                        logger.debug(f"[{datetime.now()}] Redis健康拨测失败，继续等待")
                        
                except Exception as e:
                    logger.error(f"[{datetime.now()}] Redis健康拨测异常: {e}")
            else:
                # Redis已恢复，退出拨测循环
                break
            
            # 等待下一次拨测
            time.sleep(interval)
        
        # 拨测结束，重置状态
        with self._lock:
            self._is_probing = False
        
        logger.info("Redis健康拨测循环结束")
