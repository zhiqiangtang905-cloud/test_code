# -*- coding: utf-8 -*-
"""
健康拨测类
负责Redis健康检查、故障恢复和过期数据清理
"""
import time
import logging
import threading
from datetime import datetime
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HealthCheck:
    """
    健康拨测类
    
    功能：
    1. 监控Redis存活状态
    2. Redis故障时启动健康拨测
    3. Redis恢复时回写数据到Redis
    4. 定期清理MySQL中的过期数据
    
    属性：
    - redis_alive: Redis存活状态（True/False）
    - is_probing: 当前是否正在拨测（True/False）
    """
    
    def __init__(self, redis_client, db_manager, config):
        """
        初始化健康拨测类
        
        Args:
            redis_client: Redis客户端对象
            db_manager: 数据库管理器对象
            config: 健康拨测配置
        """
        self.redis_client = redis_client
        self.db_manager = db_manager
        self.config = config
        
        # Redis存活状态
        self.redis_alive = True
        
        # 当前拨测状态
        self.is_probing = False
        
        # 拨测线程
        self._probe_thread: Optional[threading.Thread] = None
        
        # 清理任务线程
        self._cleanup_thread: Optional[threading.Thread] = None
        
        # 停止标志
        self._stop_event = threading.Event()
        
        logger.info("健康拨测类初始化完成")
    
    def set_redis_down(self):
        """
        设置Redis为不可用状态
        当Redis操作失败时调用此方法
        """
        if self.redis_alive:
            logger.warning("检测到Redis不可用，开启健康拨测")
            self.redis_alive = False
            self.start_health_probe()
    
    def set_redis_up(self):
        """
        设置Redis为可用状态
        当Redis恢复时调用此方法
        """
        if not self.redis_alive:
            logger.info("Redis已恢复可用")
            self.redis_alive = True
            self.stop_health_probe()
    
    def start_health_probe(self):
        """
        开启健康拨测
        每10秒检测一次Redis是否恢复
        """
        if self.is_probing:
            logger.debug("健康拨测已经在运行中")
            return
        
        self.is_probing = True
        self._stop_event.clear()
        
        # 启动拨测线程
        self._probe_thread = threading.Thread(
            target=self._health_probe_loop,
            daemon=True,
            name="HealthProbeThread"
        )
        self._probe_thread.start()
        logger.info("健康拨测线程已启动")
    
    def stop_health_probe(self):
        """
        停止健康拨测
        """
        if not self.is_probing:
            return
        
        self.is_probing = False
        self._stop_event.set()
        
        if self._probe_thread and self._probe_thread.is_alive():
            self._probe_thread.join(timeout=5)
        
        logger.info("健康拨测线程已停止")
    
    def _health_probe_loop(self):
        """
        健康拨测循环
        持续检测Redis状态，恢复后回写数据
        """
        probe_interval = self.config.get('probe_interval', 10)
        
        while not self._stop_event.is_set() and self.is_probing:
            try:
                # 尝试ping Redis
                if self._check_redis_health():
                    logger.info("Redis健康检查成功，开始回写数据")
                    
                    # 从MySQL回写数据到Redis
                    self._sync_mysql_to_redis()
                    
                    # 设置Redis为可用状态
                    self.set_redis_up()
                    break
                else:
                    logger.debug(f"Redis仍不可用，{probe_interval}秒后重试")
            
            except Exception as e:
                logger.error(f"健康拨测过程出错: {e}", exc_info=True)
            
            # 等待下一次拨测
            self._stop_event.wait(timeout=probe_interval)
    
    def _check_redis_health(self):
        """
        检查Redis健康状态
        
        Returns:
            bool: True表示Redis可用，False表示不可用
        """
        try:
            # 尝试ping Redis
            result = self.redis_client.ping()
            return result is True
        except Exception as e:
            logger.debug(f"Redis健康检查失败: {e}")
            return False
    
    def _sync_mysql_to_redis(self):
        """
        从MySQL同步数据到Redis
        当Redis恢复时，将MySQL中的数据回写到Redis
        """
        session = None
        try:
            from .models import RedisGatewayInfo
            import json
            
            session = self.db_manager.get_session()
            
            # 查询所有未过期的数据
            now = datetime.now()
            records = session.query(RedisGatewayInfo).filter(
                (RedisGatewayInfo.expire_time > now) | 
                (RedisGatewayInfo.expire_time.is_(None))
            ).all()
            
            sync_count = 0
            for record in records:
                try:
                    key = record.key
                    name = record.name
                    value = record.value
                    expire_time = record.expire_time
                    
                    # 根据name是否为空判断数据类型
                    if name == '':
                        # 简单的key-value
                        self.redis_client.set(key, value)
                        
                        # 设置过期时间
                        if expire_time:
                            ttl = int((expire_time - now).total_seconds())
                            if ttl > 0:
                                self.redis_client.expire(key, ttl)
                    else:
                        # hash类型
                        self.redis_client.hset(key, name, value)
                        
                        # 设置过期时间（针对整个hash）
                        if expire_time:
                            ttl = int((expire_time - now).total_seconds())
                            if ttl > 0:
                                self.redis_client.expire(key, ttl)
                    
                    sync_count += 1
                
                except Exception as e:
                    logger.error(f"同步记录失败 key={key}, name={name}: {e}")
            
            logger.info(f"数据同步完成，共同步 {sync_count} 条记录")
        
        except Exception as e:
            logger.error(f"从MySQL同步数据到Redis失败: {e}", exc_info=True)
        
        finally:
            if session:
                session.close()
    
    def start_cleanup_task(self):
        """
        启动定时清理任务
        每60秒清理一次MySQL中的过期数据
        """
        self._cleanup_thread = threading.Thread(
            target=self._cleanup_loop,
            daemon=True,
            name="CleanupThread"
        )
        self._cleanup_thread.start()
        logger.info("定时清理任务已启动")
    
    def _cleanup_loop(self):
        """
        清理任务循环
        定期清理MySQL中的过期数据，使用分布式锁保证只有一个实例执行
        """
        cleanup_interval = self.config.get('cleanup_interval', 60)
        lock_key = self.config.get('cleanup_lock_key', 'redis_gateway:cleanup:lock')
        lock_timeout = self.config.get('lock_timeout', 300)
        
        while not self._stop_event.is_set():
            try:
                # 等待到下一个执行时间
                self._stop_event.wait(timeout=cleanup_interval)
                
                if self._stop_event.is_set():
                    break
                
                # 尝试获取分布式锁
                lock_acquired = False
                lock_identifier = None
                
                try:
                    # 使用Redis分布式锁或MySQL锁
                    if self.redis_alive:
                        # 使用Redis实现分布式锁
                        import uuid
                        lock_identifier = str(uuid.uuid4())
                        lock_acquired = self.redis_client.set(
                            lock_key,
                            lock_identifier,
                            nx=True,
                            ex=lock_timeout
                        )
                    else:
                        # Redis不可用时，使用MySQL实现简单锁（通过记录）
                        lock_acquired = self._acquire_mysql_lock(lock_key, lock_timeout)
                    
                    if lock_acquired:
                        logger.info("获取清理任务锁成功，开始清理过期数据")
                        self._cleanup_expired_data()
                    else:
                        logger.debug("其他实例正在执行清理任务")
                
                finally:
                    # 释放锁
                    if lock_acquired:
                        if self.redis_alive and lock_identifier:
                            # 使用Lua脚本安全释放锁
                            lua_script = """
                            if redis.call("get", KEYS[1]) == ARGV[1] then
                                return redis.call("del", KEYS[1])
                            else
                                return 0
                            end
                            """
                            self.redis_client.eval(lua_script, 1, lock_key, lock_identifier)
                        else:
                            self._release_mysql_lock(lock_key)
            
            except Exception as e:
                logger.error(f"清理任务执行出错: {e}", exc_info=True)
    
    def _acquire_mysql_lock(self, lock_key, timeout):
        """
        使用MySQL实现分布式锁（简单实现）
        
        Args:
            lock_key: 锁的key
            timeout: 锁超时时间（秒）
            
        Returns:
            bool: 是否成功获取锁
        """
        session = None
        try:
            from .models import RedisGatewayInfo
            from datetime import timedelta
            
            session = self.db_manager.get_session()
            now = datetime.now()
            
            # 查询锁记录
            lock_record = session.query(RedisGatewayInfo).filter(
                RedisGatewayInfo.key == lock_key,
                RedisGatewayInfo.name == ''
            ).first()
            
            if lock_record:
                # 检查锁是否过期
                if lock_record.expire_time and lock_record.expire_time > now:
                    return False
                else:
                    # 锁已过期，更新锁
                    lock_record.value = 'locked'
                    lock_record.expire_time = now + timedelta(seconds=timeout)
                    lock_record.last_update_time = now
            else:
                # 创建新锁
                lock_record = RedisGatewayInfo(
                    key=lock_key,
                    name='',
                    value='locked',
                    expire_time=now + timedelta(seconds=timeout),
                    last_update_time=now
                )
                session.add(lock_record)
            
            session.commit()
            return True
        
        except Exception as e:
            logger.error(f"获取MySQL锁失败: {e}")
            if session:
                session.rollback()
            return False
        
        finally:
            if session:
                session.close()
    
    def _release_mysql_lock(self, lock_key):
        """
        释放MySQL锁
        
        Args:
            lock_key: 锁的key
        """
        session = None
        try:
            from .models import RedisGatewayInfo
            
            session = self.db_manager.get_session()
            
            # 删除锁记录
            session.query(RedisGatewayInfo).filter(
                RedisGatewayInfo.key == lock_key,
                RedisGatewayInfo.name == ''
            ).delete()
            
            session.commit()
        
        except Exception as e:
            logger.error(f"释放MySQL锁失败: {e}")
            if session:
                session.rollback()
        
        finally:
            if session:
                session.close()
    
    def _cleanup_expired_data(self):
        """
        清理MySQL中的过期数据
        """
        session = None
        try:
            from .models import RedisGatewayInfo
            
            session = self.db_manager.get_session()
            now = datetime.now()
            
            # 删除过期数据
            deleted_count = session.query(RedisGatewayInfo).filter(
                RedisGatewayInfo.expire_time.isnot(None),
                RedisGatewayInfo.expire_time < now
            ).delete()
            
            session.commit()
            logger.info(f"清理过期数据完成，共删除 {deleted_count} 条记录")
        
        except Exception as e:
            logger.error(f"清理过期数据失败: {e}", exc_info=True)
            if session:
                session.rollback()
        
        finally:
            if session:
                session.close()
    
    def stop(self):
        """
        停止所有后台任务
        """
        logger.info("正在停止健康拨测服务...")
        self._stop_event.set()
        
        # 停止拨测线程
        self.stop_health_probe()
        
        # 停止清理线程
        if self._cleanup_thread and self._cleanup_thread.is_alive():
            self._cleanup_thread.join(timeout=5)
        
        logger.info("健康拨测服务已停止")
