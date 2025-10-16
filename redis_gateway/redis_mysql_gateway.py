# -*- coding: utf-8 -*-
"""
Redis降级MySQL网关核心逻辑
实现Redis到MySQL的无缝降级切换
"""
import json
import logging
import redis
from datetime import datetime, timedelta
from typing import Any, List, Optional, Union
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

from .models import DatabaseManager, RedisGatewayInfo
from .health_check import HealthCheck

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RedisGateway:
    """
    Redis网关类
    
    功能：
    1. 实现Redis到MySQL的降级逻辑
    2. 读取优先从Redis，失败则从MySQL读取
    3. 写入先写Redis，然后异步写入MySQL
    4. 自动健康检查和故障恢复
    
    支持的Redis操作：
    - set_ex: 设置键值对并指定过期时间
    - get: 获取键的值
    - exists: 检查键是否存在
    - delete: 删除键
    - expire: 设置过期时间
    - hset: 设置hash字段
    - hget: 获取hash字段值
    - hdel: 删除hash字段
    - lpush: 从列表左侧插入
    - rpush: 从列表右侧插入
    - lpop: 从列表左侧弹出
    - lrange: 获取列表范围
    - lindex: 获取列表指定索引的元素
    - lrem: 删除列表中的元素
    """
    
    def __init__(self, redis_config, mysql_config, health_config=None):
        """
        初始化Redis网关
        
        Args:
            redis_config: Redis配置字典
            mysql_config: MySQL配置字典
            health_config: 健康检查配置字典（可选）
        """
        # 初始化Redis客户端
        try:
            self.redis_client = redis.Redis(**redis_config)
            self.redis_client.ping()
            logger.info("Redis连接成功")
        except Exception as e:
            logger.error(f"Redis连接失败: {e}")
            self.redis_client = redis.Redis(**redis_config)
        
        # 初始化数据库管理器
        self.db_manager = DatabaseManager(mysql_config)
        self.db_manager.create_tables()
        logger.info("数据库初始化成功")
        
        # 初始化线程池用于异步写入MySQL
        self.executor = ThreadPoolExecutor(max_workers=10, thread_name_prefix="MySQLWriter")
        
        # 初始化健康检查
        if health_config is None:
            health_config = {
                'probe_interval': 10,
                'cleanup_interval': 60,
                'lock_timeout': 300,
                'lock_key': 'redis_gateway:health_check:lock',
                'cleanup_lock_key': 'redis_gateway:cleanup:lock',
            }
        
        self.health_check = HealthCheck(
            redis_client=self.redis_client,
            db_manager=self.db_manager,
            config=health_config
        )
        
        # 启动定时清理任务
        self.health_check.start_cleanup_task()
        
        logger.info("Redis网关初始化完成")
    
    def _is_redis_available(self):
        """
        检查Redis是否可用
        
        Returns:
            bool: True表示可用，False表示不可用
        """
        return self.health_check.redis_alive
    
    def _handle_redis_error(self, error):
        """
        处理Redis错误
        当Redis操作失败时，标记Redis为不可用状态
        
        Args:
            error: 异常对象
        """
        logger.error(f"Redis操作失败: {error}")
        self.health_check.set_redis_down()
    
    def _async_write_mysql(self, key, name, value, expire_time=None):
        """
        异步写入MySQL
        
        Args:
            key: Redis的key
            name: Redis的name/field（hash字段名或列表索引）
            value: 值
            expire_time: 过期时间
        """
        self.executor.submit(
            self._write_mysql_sync,
            key, name, value, expire_time
        )
    
    def _write_mysql_sync(self, key, name, value, expire_time=None):
        """
        同步写入MySQL
        
        Args:
            key: Redis的key
            name: Redis的name/field
            value: 值
            expire_time: 过期时间
        """
        session = None
        try:
            session = self.db_manager.get_session()
            now = datetime.now()
            
            # 查询是否存在
            record = session.query(RedisGatewayInfo).filter(
                RedisGatewayInfo.key == key,
                RedisGatewayInfo.name == name
            ).first()
            
            if record:
                # 更新现有记录
                record.value = value
                record.expire_time = expire_time
                record.last_update_time = now
            else:
                # 插入新记录
                record = RedisGatewayInfo(
                    key=key,
                    name=name,
                    value=value,
                    expire_time=expire_time,
                    last_update_time=now
                )
                session.add(record)
            
            session.commit()
            logger.debug(f"MySQL写入成功: key={key}, name={name}")
        
        except Exception as e:
            logger.error(f"MySQL写入失败: key={key}, name={name}, error={e}")
            if session:
                session.rollback()
        
        finally:
            if session:
                session.close()
    
    def _read_from_mysql(self, key, name=''):
        """
        从MySQL读取数据
        读取前先清理过期数据
        
        Args:
            key: Redis的key
            name: Redis的name/field
            
        Returns:
            值，如果不存在返回None
        """
        session = None
        try:
            session = self.db_manager.get_session()
            now = datetime.now()
            
            # 查询数据
            record = session.query(RedisGatewayInfo).filter(
                RedisGatewayInfo.key == key,
                RedisGatewayInfo.name == name,
                (RedisGatewayInfo.expire_time > now) | (RedisGatewayInfo.expire_time.is_(None))
            ).first()
            
            if record:
                logger.debug(f"MySQL读取成功: key={key}, name={name}")
                return record.value
            else:
                logger.debug(f"MySQL中不存在: key={key}, name={name}")
                return None
        
        except Exception as e:
            logger.error(f"MySQL读取失败: key={key}, name={name}, error={e}")
            return None
        
        finally:
            if session:
                session.close()
    
    def _delete_from_mysql(self, key, name=''):
        """
        从MySQL删除数据
        
        Args:
            key: Redis的key
            name: Redis的name/field
        """
        self.executor.submit(self._delete_from_mysql_sync, key, name)
    
    def _delete_from_mysql_sync(self, key, name=''):
        """
        同步从MySQL删除数据
        
        Args:
            key: Redis的key
            name: Redis的name/field
        """
        session = None
        try:
            session = self.db_manager.get_session()
            
            session.query(RedisGatewayInfo).filter(
                RedisGatewayInfo.key == key,
                RedisGatewayInfo.name == name
            ).delete()
            
            session.commit()
            logger.debug(f"MySQL删除成功: key={key}, name={name}")
        
        except Exception as e:
            logger.error(f"MySQL删除失败: key={key}, name={name}, error={e}")
            if session:
                session.rollback()
        
        finally:
            if session:
                session.close()
    
    # ==================== 基础操作 ====================
    
    def set_ex(self, key, value, ttl):
        """
        设置键值对并指定过期时间
        
        Args:
            key: 键
            value: 值（会被转换为JSON字符串）
            ttl: 过期时间（秒）
        """
        # 转换为JSON字符串
        if not isinstance(value, str):
            value = json.dumps(value, ensure_ascii=False)
        
        # 计算过期时间
        expire_time = datetime.now() + timedelta(seconds=ttl)
        
        try:
            if self._is_redis_available():
                # 写入Redis
                self.redis_client.setex(key, ttl, value)
                logger.debug(f"Redis set_ex成功: key={key}")
            else:
                logger.warning(f"Redis不可用，仅写入MySQL: key={key}")
        except Exception as e:
            self._handle_redis_error(e)
        
        # 异步写入MySQL
        self._async_write_mysql(key, '', value, expire_time)
    
    def get(self, key):
        """
        获取键的值
        
        Args:
            key: 键
            
        Returns:
            值，如果不存在返回None
        """
        try:
            if self._is_redis_available():
                # 从Redis读取
                value = self.redis_client.get(key)
                if value:
                    logger.debug(f"Redis get成功: key={key}")
                    return value
                else:
                    logger.debug(f"Redis中不存在: key={key}")
                    return None
        except Exception as e:
            self._handle_redis_error(e)
        
        # Redis失败或不可用，从MySQL读取
        logger.info(f"从MySQL读取: key={key}")
        return self._read_from_mysql(key, '')
    
    def exists(self, key):
        """
        检查键是否存在
        
        Args:
            key: 键
            
        Returns:
            bool: 存在返回True，不存在返回False
        """
        try:
            if self._is_redis_available():
                # 从Redis检查
                result = self.redis_client.exists(key)
                logger.debug(f"Redis exists: key={key}, result={result}")
                return result > 0
        except Exception as e:
            self._handle_redis_error(e)
        
        # Redis失败或不可用，从MySQL检查
        logger.info(f"从MySQL检查存在性: key={key}")
        value = self._read_from_mysql(key, '')
        return value is not None
    
    def delete(self, key):
        """
        删除键
        
        Args:
            key: 键
        """
        try:
            if self._is_redis_available():
                # 从Redis删除
                self.redis_client.delete(key)
                logger.debug(f"Redis delete成功: key={key}")
        except Exception as e:
            self._handle_redis_error(e)
        
        # 异步从MySQL删除（删除所有相关记录）
        self.executor.submit(self._delete_key_all_names, key)
    
    def _delete_key_all_names(self, key):
        """
        删除MySQL中指定key的所有记录
        
        Args:
            key: 键
        """
        session = None
        try:
            session = self.db_manager.get_session()
            
            session.query(RedisGatewayInfo).filter(
                RedisGatewayInfo.key == key
            ).delete()
            
            session.commit()
            logger.debug(f"MySQL删除所有记录成功: key={key}")
        
        except Exception as e:
            logger.error(f"MySQL删除所有记录失败: key={key}, error={e}")
            if session:
                session.rollback()
        
        finally:
            if session:
                session.close()
    
    def expire(self, key, ttl):
        """
        设置键的过期时间
        
        Args:
            key: 键
            ttl: 过期时间（秒）
        """
        expire_time = datetime.now() + timedelta(seconds=ttl)
        
        try:
            if self._is_redis_available():
                # 设置Redis过期时间
                self.redis_client.expire(key, ttl)
                logger.debug(f"Redis expire成功: key={key}, ttl={ttl}")
        except Exception as e:
            self._handle_redis_error(e)
        
        # 异步更新MySQL中的过期时间
        self.executor.submit(self._update_expire_time, key, expire_time)
    
    def _update_expire_time(self, key, expire_time):
        """
        更新MySQL中的过期时间
        
        Args:
            key: 键
            expire_time: 过期时间
        """
        session = None
        try:
            session = self.db_manager.get_session()
            
            # 更新所有相关记录的过期时间
            session.query(RedisGatewayInfo).filter(
                RedisGatewayInfo.key == key
            ).update({
                'expire_time': expire_time,
                'last_update_time': datetime.now()
            })
            
            session.commit()
            logger.debug(f"MySQL更新过期时间成功: key={key}")
        
        except Exception as e:
            logger.error(f"MySQL更新过期时间失败: key={key}, error={e}")
            if session:
                session.rollback()
        
        finally:
            if session:
                session.close()
    
    # ==================== Hash操作 ====================
    
    def hset(self, name, key, value):
        """
        设置hash字段
        
        Args:
            name: hash名称
            key: 字段名
            value: 值（会被转换为JSON字符串）
        """
        # 转换为JSON字符串
        if not isinstance(value, str):
            value = json.dumps(value, ensure_ascii=False)
        
        try:
            if self._is_redis_available():
                # 写入Redis
                self.redis_client.hset(name, key, value)
                logger.debug(f"Redis hset成功: name={name}, key={key}")
        except Exception as e:
            self._handle_redis_error(e)
        
        # 异步写入MySQL
        self._async_write_mysql(name, key, value)
    
    def hget(self, name, key):
        """
        获取hash字段值
        
        Args:
            name: hash名称
            key: 字段名
            
        Returns:
            值，如果不存在返回None
        """
        try:
            if self._is_redis_available():
                # 从Redis读取
                value = self.redis_client.hget(name, key)
                if value:
                    logger.debug(f"Redis hget成功: name={name}, key={key}")
                    return value
                else:
                    logger.debug(f"Redis中不存在: name={name}, key={key}")
                    return None
        except Exception as e:
            self._handle_redis_error(e)
        
        # Redis失败或不可用，从MySQL读取
        logger.info(f"从MySQL读取: name={name}, key={key}")
        return self._read_from_mysql(name, key)
    
    def hdel(self, name, key):
        """
        删除hash字段
        
        Args:
            name: hash名称
            key: 字段名
        """
        try:
            if self._is_redis_available():
                # 从Redis删除
                self.redis_client.hdel(name, key)
                logger.debug(f"Redis hdel成功: name={name}, key={key}")
        except Exception as e:
            self._handle_redis_error(e)
        
        # 异步从MySQL删除
        self._delete_from_mysql(name, key)
    
    # ==================== List操作 ====================
    
    def lpush(self, name, value):
        """
        从列表左侧插入元素
        
        Args:
            name: 列表名称
            value: 值（会被转换为JSON字符串）
        """
        # 转换为JSON字符串
        if not isinstance(value, str):
            value = json.dumps(value, ensure_ascii=False)
        
        try:
            if self._is_redis_available():
                # 写入Redis
                self.redis_client.lpush(name, value)
                logger.debug(f"Redis lpush成功: name={name}")
        except Exception as e:
            self._handle_redis_error(e)
        
        # 异步写入MySQL（列表使用name作为字段名的哈希表存储）
        # 使用索引作为key
        index = f"list_0_{value[:50]}"  # 简化处理，实际应该获取列表长度
        self._async_write_mysql(name, index, value)
    
    def rpush(self, name, value):
        """
        从列表右侧插入元素
        
        Args:
            name: 列表名称
            value: 值（会被转换为JSON字符串）
        """
        # 转换为JSON字符串
        if not isinstance(value, str):
            value = json.dumps(value, ensure_ascii=False)
        
        try:
            if self._is_redis_available():
                # 写入Redis
                self.redis_client.rpush(name, value)
                logger.debug(f"Redis rpush成功: name={name}")
        except Exception as e:
            self._handle_redis_error(e)
        
        # 异步写入MySQL
        index = f"list_{value[:50]}"  # 简化处理
        self._async_write_mysql(name, index, value)
    
    def lpop(self, key, count=1):
        """
        从列表左侧弹出元素
        
        Args:
            key: 列表名称
            count: 弹出数量
            
        Returns:
            弹出的元素列表
        """
        try:
            if self._is_redis_available():
                # 从Redis弹出
                if count == 1:
                    value = self.redis_client.lpop(key)
                    logger.debug(f"Redis lpop成功: key={key}")
                    return value
                else:
                    values = self.redis_client.lpop(key, count)
                    logger.debug(f"Redis lpop成功: key={key}, count={count}")
                    return values
        except Exception as e:
            self._handle_redis_error(e)
        
        # MySQL不适合实现列表的弹出操作，返回None
        logger.warning("MySQL不支持lpop操作")
        return None
    
    def lrange(self, name, start=0, end=-1):
        """
        获取列表范围
        
        Args:
            name: 列表名称
            start: 开始索引
            end: 结束索引
            
        Returns:
            元素列表
        """
        try:
            if self._is_redis_available():
                # 从Redis读取
                values = self.redis_client.lrange(name, start, end)
                logger.debug(f"Redis lrange成功: name={name}")
                return values
        except Exception as e:
            self._handle_redis_error(e)
        
        # Redis失败或不可用，从MySQL读取
        logger.info(f"从MySQL读取列表: name={name}")
        return self._read_list_from_mysql(name)
    
    def _read_list_from_mysql(self, name):
        """
        从MySQL读取列表
        
        Args:
            name: 列表名称
            
        Returns:
            元素列表
        """
        session = None
        try:
            session = self.db_manager.get_session()
            now = datetime.now()
            
            # 查询所有列表元素
            records = session.query(RedisGatewayInfo).filter(
                RedisGatewayInfo.key == name,
                RedisGatewayInfo.name.like('list_%'),
                (RedisGatewayInfo.expire_time > now) | (RedisGatewayInfo.expire_time.is_(None))
            ).all()
            
            values = [record.value for record in records]
            logger.debug(f"MySQL读取列表成功: name={name}, count={len(values)}")
            return values
        
        except Exception as e:
            logger.error(f"MySQL读取列表失败: name={name}, error={e}")
            return []
        
        finally:
            if session:
                session.close()
    
    def lindex(self, key, index):
        """
        获取列表指定索引的元素
        
        Args:
            key: 列表名称
            index: 索引
            
        Returns:
            元素值
        """
        try:
            if self._is_redis_available():
                # 从Redis读取
                value = self.redis_client.lindex(key, index)
                logger.debug(f"Redis lindex成功: key={key}, index={index}")
                return value
        except Exception as e:
            self._handle_redis_error(e)
        
        # MySQL不适合实现索引访问
        logger.warning("MySQL不支持lindex操作")
        return None
    
    def lrem(self, key, count, value):
        """
        删除列表中的元素
        
        Args:
            key: 列表名称
            count: 删除数量
            value: 要删除的值
        """
        # 转换为JSON字符串
        if not isinstance(value, str):
            value = json.dumps(value, ensure_ascii=False)
        
        try:
            if self._is_redis_available():
                # 从Redis删除
                self.redis_client.lrem(key, count, value)
                logger.debug(f"Redis lrem成功: key={key}")
        except Exception as e:
            self._handle_redis_error(e)
        
        # 异步从MySQL删除
        self.executor.submit(self._lrem_from_mysql, key, value)
    
    def _lrem_from_mysql(self, key, value):
        """
        从MySQL删除列表元素
        
        Args:
            key: 列表名称
            value: 要删除的值
        """
        session = None
        try:
            session = self.db_manager.get_session()
            
            # 删除匹配的记录
            session.query(RedisGatewayInfo).filter(
                RedisGatewayInfo.key == key,
                RedisGatewayInfo.value == value
            ).delete()
            
            session.commit()
            logger.debug(f"MySQL lrem成功: key={key}")
        
        except Exception as e:
            logger.error(f"MySQL lrem失败: key={key}, error={e}")
            if session:
                session.rollback()
        
        finally:
            if session:
                session.close()
    
    # ==================== 分布式锁 ====================
    
    @contextmanager
    def distributed_lock(self, lock_key, timeout=300, blocking=True, blocking_timeout=None):
        """
        Redis分布式锁上下文管理器
        
        Args:
            lock_key: 锁的key
            timeout: 锁的超时时间（秒）
            blocking: 是否阻塞等待
            blocking_timeout: 阻塞超时时间（秒）
            
        Example:
            with gateway.distributed_lock('my_lock'):
                # 执行需要加锁的操作
                pass
        """
        lock = None
        lock_acquired = False
        
        try:
            if self._is_redis_available():
                # 使用Redis分布式锁
                from redis.lock import Lock
                lock = Lock(
                    self.redis_client,
                    lock_key,
                    timeout=timeout,
                    blocking=blocking,
                    blocking_timeout=blocking_timeout
                )
                lock_acquired = lock.acquire()
            else:
                # Redis不可用，使用MySQL锁
                lock_acquired = self.health_check._acquire_mysql_lock(lock_key, timeout)
            
            if not lock_acquired:
                raise RuntimeError(f"无法获取分布式锁: {lock_key}")
            
            yield
        
        finally:
            # 释放锁
            if lock_acquired:
                try:
                    if lock:
                        lock.release()
                    else:
                        self.health_check._release_mysql_lock(lock_key)
                except Exception as e:
                    logger.error(f"释放锁失败: {e}")
    
    # ==================== 关闭资源 ====================
    
    def close(self):
        """
        关闭所有资源
        """
        logger.info("正在关闭Redis网关...")
        
        # 停止健康检查
        self.health_check.stop()
        
        # 关闭线程池
        self.executor.shutdown(wait=True)
        
        # 关闭Redis连接
        if self.redis_client:
            self.redis_client.close()
        
        # 关闭数据库连接
        if self.db_manager:
            self.db_manager.close()
        
        logger.info("Redis网关已关闭")
    
    def __enter__(self):
        """
        上下文管理器入口
        """
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        上下文管理器出口
        """
        self.close()
