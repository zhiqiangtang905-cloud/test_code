"""
Redis健康检查服务
负责监控Redis状态、执行健康拨测、清理过期数据
使用分布式锁确保多实例场景下只有一个实例执行任务
"""
import time
import schedule
import threading
import logging
from datetime import datetime, timedelta
from typing import Optional
import json

logger = logging.getLogger(__name__)


class RedisHealthCheck:
    """
    Redis健康检查类
    
    功能：
    1. 监控Redis存活状态
    2. Redis不可用时启动健康拨测（每10秒）
    3. Redis恢复后将MySQL数据回写到Redis
    4. 定期清理过期数据（每60秒）
    5. 使用分布式锁确保多实例只有一个执行任务
    """
    
    # 使用类变量存储单例实例
    _instance: Optional['RedisHealthCheck'] = None
    _lock = threading.Lock()
    
    def __new__(cls, *args, **kwargs):
        """单例模式"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, get_redis_service_func, get_db_session_func):
        """
        初始化健康检查服务
        
        Args:
            get_redis_service_func: 获取Redis服务的函数
            get_db_session_func: 获取数据库会话的函数
        """
        # 防止重复初始化
        if hasattr(self, '_initialized'):
            return
            
        self._initialized = True
        self.get_redis_service = get_redis_service_func
        self.get_db_session = get_db_session_func
        
        # Redis存活状态
        self.redis_alive = True
        
        # 当前拨测状态
        self.is_probing = False
        
        # 拨测线程
        self._probe_thread: Optional[threading.Thread] = None
        
        # 定时任务线程
        self._schedule_thread: Optional[threading.Thread] = None
        self._schedule_running = False
        
        # 分布式锁的key
        self.HEALTH_CHECK_LOCK_KEY = "redis_service:health_check_lock"
        self.CLEANUP_LOCK_KEY = "redis_service:cleanup_lock"
        
        # 锁的过期时间（秒）
        self.LOCK_TTL = 30
        
        logger.info("Redis健康检查服务初始化完成")
    
    def start_schedule(self):
        """
        启动定时任务
        每60秒清理一次过期数据
        """
        if self._schedule_running:
            logger.warning("定时任务已在运行中")
            return
        
        self._schedule_running = True
        
        # 配置定时任务：每60秒清理一次过期数据
        schedule.every(60).seconds.do(self._cleanup_expired_data_with_lock)
        
        # 启动调度器线程
        self._schedule_thread = threading.Thread(target=self._run_schedule, daemon=True)
        self._schedule_thread.start()
        
        logger.info("定时任务已启动：每60秒清理一次过期数据")
    
    def _run_schedule(self):
        """运行调度器"""
        while self._schedule_running:
            schedule.run_pending()
            time.sleep(1)
    
    def stop_schedule(self):
        """停止定时任务"""
        self._schedule_running = False
        schedule.clear()
        logger.info("定时任务已停止")
    
    def mark_redis_down(self):
        """
        标记Redis为不可用状态
        启动健康拨测
        """
        if not self.redis_alive:
            # 已经是不可用状态，无需重复处理
            return
        
        logger.warning("Redis标记为不可用，启动健康拨测")
        self.redis_alive = False
        
        # 启动健康拨测
        if not self.is_probing:
            self._start_health_probe()
    
    def _start_health_probe(self):
        """启动健康拨测线程"""
        if self.is_probing:
            return
        
        self.is_probing = True
        self._probe_thread = threading.Thread(target=self._health_probe_loop, daemon=True)
        self._probe_thread.start()
        logger.info("健康拨测线程已启动")
    
    def _health_probe_loop(self):
        """
        健康拨测循环
        每10秒检测一次Redis是否恢复
        """
        while self.is_probing and not self.redis_alive:
            try:
                # 使用分布式锁确保只有一个实例执行拨测
                if self._acquire_lock(self.HEALTH_CHECK_LOCK_KEY):
                    try:
                        # 检测Redis是否可用
                        if self._check_redis_alive():
                            logger.info("Redis已恢复可用")
                            # 回写MySQL数据到Redis
                            self._sync_mysql_to_redis()
                            # 标记Redis为可用
                            self.redis_alive = True
                            self.is_probing = False
                            logger.info("健康拨测停止")
                    finally:
                        self._release_lock(self.HEALTH_CHECK_LOCK_KEY)
                
            except Exception as e:
                logger.error(f"健康拨测异常: {e}", exc_info=True)
            
            # 等待10秒后再次拨测
            time.sleep(10)
    
    def _check_redis_alive(self) -> bool:
        """
        检查Redis是否可用
        
        Returns:
            bool: Redis是否可用
        """
        try:
            redis_service = self.get_redis_service()
            # 使用ping命令检测Redis
            redis_service.ping()
            return True
        except Exception as e:
            logger.debug(f"Redis拨测失败: {e}")
            return False
    
    def _sync_mysql_to_redis(self):
        """
        将MySQL中的数据回写到Redis
        只回写未过期的数据
        """
        try:
            from models import RedisGatewayInfo
            
            with self.get_db_session() as session:
                # 查询所有未过期的数据
                now = datetime.now()
                records = session.query(RedisGatewayInfo).filter(
                    (RedisGatewayInfo.expire_time == None) | 
                    (RedisGatewayInfo.expire_time > now)
                ).all()
                
                redis_service = self.get_redis_service()
                sync_count = 0
                
                for record in records:
                    try:
                        # 根据name判断数据类型并回写
                        if record.name == '':
                            # 普通的key-value
                            redis_service.set(record.key, record.value)
                            
                            # 设置过期时间
                            if record.expire_time:
                                ttl = int((record.expire_time - now).total_seconds())
                                if ttl > 0:
                                    redis_service.expire(record.key, ttl)
                        else:
                            # hash类型
                            redis_service.hset(record.name, record.key, record.value)
                        
                        sync_count += 1
                    except Exception as e:
                        logger.error(f"回写数据失败 key={record.key}, name={record.name}: {e}")
                
                logger.info(f"MySQL数据回写到Redis完成，共回写{sync_count}条数据")
                
        except Exception as e:
            logger.error(f"MySQL数据回写Redis异常: {e}", exc_info=True)
    
    def _cleanup_expired_data_with_lock(self):
        """
        使用分布式锁清理过期数据
        确保多实例场景下只有一个实例执行清理任务
        """
        # 尝试获取清理锁
        if self._acquire_lock(self.CLEANUP_LOCK_KEY):
            try:
                self._cleanup_expired_data()
            finally:
                self._release_lock(self.CLEANUP_LOCK_KEY)
        else:
            logger.debug("其他实例正在执行清理任务，跳过")
    
    def _cleanup_expired_data(self):
        """
        清理MySQL中的过期数据
        """
        try:
            from models import RedisGatewayInfo
            
            with self.get_db_session() as session:
                now = datetime.now()
                
                # 删除过期数据
                deleted_count = session.query(RedisGatewayInfo).filter(
                    RedisGatewayInfo.expire_time != None,
                    RedisGatewayInfo.expire_time <= now
                ).delete(synchronize_session=False)
                
                session.commit()
                
                if deleted_count > 0:
                    logger.info(f"清理过期数据完成，共删除{deleted_count}条记录")
                
        except Exception as e:
            logger.error(f"清理过期数据异常: {e}", exc_info=True)
    
    def _acquire_lock(self, lock_key: str) -> bool:
        """
        获取分布式锁
        
        Args:
            lock_key: 锁的key
            
        Returns:
            bool: 是否成功获取锁
        """
        try:
            # 如果Redis可用，使用Redis锁
            if self.redis_alive:
                redis_service = self.get_redis_service()
                # 使用SET NX EX实现分布式锁
                result = redis_service.set(
                    lock_key,
                    "locked",
                    nx=True,
                    ex=self.LOCK_TTL
                )
                return result is not None and result
            else:
                # Redis不可用时使用MySQL锁
                return self._acquire_mysql_lock(lock_key)
                
        except Exception as e:
            logger.error(f"获取锁失败 {lock_key}: {e}")
            return False
    
    def _release_lock(self, lock_key: str):
        """
        释放分布式锁
        
        Args:
            lock_key: 锁的key
        """
        try:
            if self.redis_alive:
                redis_service = self.get_redis_service()
                redis_service.delete(lock_key)
            else:
                self._release_mysql_lock(lock_key)
        except Exception as e:
            logger.error(f"释放锁失败 {lock_key}: {e}")
    
    def _acquire_mysql_lock(self, lock_key: str) -> bool:
        """
        使用MySQL实现分布式锁
        
        Args:
            lock_key: 锁的key
            
        Returns:
            bool: 是否成功获取锁
        """
        try:
            from models import RedisGatewayInfo
            
            with self.get_db_session() as session:
                now = datetime.now()
                expire_time = now + timedelta(seconds=self.LOCK_TTL)
                
                # 先尝试清理过期的锁
                session.query(RedisGatewayInfo).filter(
                    RedisGatewayInfo.key == lock_key,
                    RedisGatewayInfo.name == '__lock__',
                    RedisGatewayInfo.expire_time <= now
                ).delete(synchronize_session=False)
                session.commit()
                
                # 尝试插入锁记录
                try:
                    lock_record = RedisGatewayInfo(
                        key=lock_key,
                        name='__lock__',
                        value='locked',
                        expire_time=expire_time,
                        last_update_time=now
                    )
                    session.add(lock_record)
                    session.commit()
                    return True
                except Exception:
                    # 插入失败说明锁已被占用
                    session.rollback()
                    return False
                    
        except Exception as e:
            logger.error(f"获取MySQL锁失败: {e}")
            return False
    
    def _release_mysql_lock(self, lock_key: str):
        """
        释放MySQL锁
        
        Args:
            lock_key: 锁的key
        """
        try:
            from models import RedisGatewayInfo
            
            with self.get_db_session() as session:
                session.query(RedisGatewayInfo).filter(
                    RedisGatewayInfo.key == lock_key,
                    RedisGatewayInfo.name == '__lock__'
                ).delete(synchronize_session=False)
                session.commit()
                
        except Exception as e:
            logger.error(f"释放MySQL锁失败: {e}")


# 全局健康检查实例
_health_check_instance: Optional[RedisHealthCheck] = None


def get_health_check(get_redis_service_func, get_db_session_func) -> RedisHealthCheck:
    """
    获取健康检查服务单例
    
    Args:
        get_redis_service_func: 获取Redis服务的函数
        get_db_session_func: 获取数据库会话的函数
        
    Returns:
        RedisHealthCheck: 健康检查服务实例
    """
    global _health_check_instance
    if _health_check_instance is None:
        _health_check_instance = RedisHealthCheck(get_redis_service_func, get_db_session_func)
    return _health_check_instance
