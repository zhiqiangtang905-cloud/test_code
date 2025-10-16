"""
Redis降级MySQL实现
支持多实例环境，通过分布式锁确保只有一个实例执行关键任务
"""

import json
import time
import logging
import threading
from datetime import datetime, timedelta
from typing import Optional, Any, List, Union
from contextlib import contextmanager
import schedule

import redis
from sqlalchemy import create_engine, Column, String, DateTime, Text, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

Base = declarative_base()


class RedisGatewayInfo(Base):
    """Redis网关信息表模型"""
    __tablename__ = 'redis_gateway_info'
    
    # key和name作为联合主键
    key = Column(String(255), primary_key=True, nullable=False, comment='Redis键')
    name = Column(String(255), primary_key=True, nullable=False, comment='Redis名称(hash/list等的name)')
    value = Column(Text, nullable=True, comment='JSON序列化后的值')
    expire_time = Column(DateTime, nullable=True, comment='过期时间')
    last_update_time = Column(DateTime, nullable=False, default=datetime.now, onupdate=datetime.now, comment='最后更新时间')
    
    # 添加索引以提升查询性能
    __table_args__ = (
        Index('idx_expire_time', 'expire_time'),
        Index('idx_last_update_time', 'last_update_time'),
    )


class RedisHealthChecker:
    """Redis健康检查类"""
    
    def __init__(self, redis_client: redis.Redis, mysql_engine, check_interval: int = 10):
        """
        初始化健康检查器
        
        Args:
            redis_client: Redis客户端实例
            mysql_engine: MySQL数据库引擎
            check_interval: 健康检查间隔（秒）
        """
        self.redis_client = redis_client
        self.mysql_engine = mysql_engine
        self.check_interval = check_interval
        
        # Redis存活状态
        self.redis_alive = True
        # 当前是否正在拨测
        self.is_checking = False
        # 拨测线程
        self._check_thread: Optional[threading.Thread] = None
        # 停止标志
        self._stop_event = threading.Event()
        
        # 分布式锁的key
        self.health_check_lock_key = "redis_health_check_lock"
        self.sync_lock_key = "redis_mysql_sync_lock"
        
    def check_redis_health(self) -> bool:
        """
        检查Redis健康状态
        
        Returns:
            bool: Redis是否健康
        """
        try:
            self.redis_client.ping()
            return True
        except Exception as e:
            logger.warning(f"Redis健康检查失败: {e}")
            return False
    
    def mark_redis_down(self):
        """标记Redis为不可用状态并启动健康拨测"""
        if not self.redis_alive:
            return
            
        logger.warning("Redis标记为不可用，启动健康拨测")
        self.redis_alive = False
        
        if not self.is_checking:
            self.start_health_check()
    
    def start_health_check(self):
        """启动健康拨测（在独立线程中运行）"""
        if self.is_checking:
            logger.info("健康拨测已在运行中")
            return
            
        self.is_checking = True
        self._stop_event.clear()
        self._check_thread = threading.Thread(target=self._health_check_loop, daemon=True)
        self._check_thread.start()
        logger.info("健康拨测线程已启动")
    
    def _health_check_loop(self):
        """健康拨测循环（在独立线程中运行）"""
        while not self._stop_event.is_set():
            try:
                # 使用分布式锁确保只有一个实例在执行健康检查
                lock_acquired = self._acquire_distributed_lock(
                    self.health_check_lock_key, 
                    timeout=self.check_interval
                )
                
                if lock_acquired:
                    try:
                        if self.check_redis_health():
                            logger.info("Redis已恢复，开始同步MySQL数据到Redis")
                            self._sync_mysql_to_redis()
                            self.redis_alive = True
                            self.is_checking = False
                            logger.info("Redis恢复完成，健康拨测停止")
                            break
                        else:
                            logger.info(f"Redis仍不可用，{self.check_interval}秒后重试")
                    finally:
                        self._release_distributed_lock(self.health_check_lock_key)
                else:
                    logger.debug("其他实例正在执行健康检查")
                    
            except Exception as e:
                logger.error(f"健康拨测异常: {e}", exc_info=True)
            
            # 等待下一次检查
            self._stop_event.wait(self.check_interval)
    
    def _acquire_distributed_lock(self, lock_key: str, timeout: int = 10) -> bool:
        """
        获取分布式锁（基于MySQL）
        
        Args:
            lock_key: 锁的键名
            timeout: 锁的超时时间（秒）
            
        Returns:
            bool: 是否成功获取锁
        """
        try:
            SessionLocal = sessionmaker(bind=self.mysql_engine)
            session = SessionLocal()
            
            try:
                # 检查是否已存在锁
                now = datetime.now()
                lock = session.query(RedisGatewayInfo).filter(
                    RedisGatewayInfo.key == lock_key,
                    RedisGatewayInfo.name == '_lock'
                ).first()
                
                if lock:
                    # 检查锁是否过期
                    if lock.expire_time and lock.expire_time > now:
                        return False
                    # 锁已过期，删除旧锁
                    session.delete(lock)
                
                # 创建新锁
                new_lock = RedisGatewayInfo(
                    key=lock_key,
                    name='_lock',
                    value=json.dumps({'acquired_at': now.isoformat()}),
                    expire_time=now + timedelta(seconds=timeout),
                    last_update_time=now
                )
                session.add(new_lock)
                session.commit()
                return True
                
            except Exception as e:
                session.rollback()
                logger.error(f"获取分布式锁失败: {e}")
                return False
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"分布式锁异常: {e}")
            return False
    
    def _release_distributed_lock(self, lock_key: str):
        """
        释放分布式锁
        
        Args:
            lock_key: 锁的键名
        """
        try:
            SessionLocal = sessionmaker(bind=self.mysql_engine)
            session = SessionLocal()
            
            try:
                session.query(RedisGatewayInfo).filter(
                    RedisGatewayInfo.key == lock_key,
                    RedisGatewayInfo.name == '_lock'
                ).delete()
                session.commit()
            except Exception as e:
                session.rollback()
                logger.error(f"释放分布式锁失败: {e}")
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"释放分布式锁异常: {e}")
    
    def _sync_mysql_to_redis(self):
        """从MySQL同步数据到Redis"""
        SessionLocal = sessionmaker(bind=self.mysql_engine)
        session = SessionLocal()
        
        try:
            # 获取所有未过期的数据
            now = datetime.now()
            records = session.query(RedisGatewayInfo).filter(
                (RedisGatewayInfo.expire_time.is_(None)) | 
                (RedisGatewayInfo.expire_time > now)
            ).all()
            
            logger.info(f"开始同步{len(records)}条MySQL数据到Redis")
            
            for record in records:
                try:
                    # 跳过锁记录
                    if record.name == '_lock':
                        continue
                        
                    # 计算TTL
                    ttl = None
                    if record.expire_time:
                        ttl = int((record.expire_time - now).total_seconds())
                        if ttl <= 0:
                            continue
                    
                    # 根据不同的数据类型同步到Redis
                    if record.name == '_string':
                        # 字符串类型
                        if ttl:
                            self.redis_client.setex(record.key, ttl, record.value)
                        else:
                            self.redis_client.set(record.key, record.value)
                    else:
                        # Hash类型
                        self.redis_client.hset(record.key, record.name, record.value)
                        if ttl:
                            self.redis_client.expire(record.key, ttl)
                            
                except Exception as e:
                    logger.error(f"同步记录失败 key={record.key}, name={record.name}: {e}")
            
            logger.info("MySQL到Redis数据同步完成")
            
        except Exception as e:
            logger.error(f"同步MySQL到Redis失败: {e}", exc_info=True)
        finally:
            session.close()
    
    def stop(self):
        """停止健康检查"""
        self._stop_event.set()
        if self._check_thread:
            self._check_thread.join(timeout=5)


class RedisGateway:
    """Redis网关，支持降级到MySQL"""
    
    def __init__(self, redis_config: dict, mysql_config: dict):
        """
        初始化Redis网关
        
        Args:
            redis_config: Redis配置字典
            mysql_config: MySQL配置字典
        """
        # 初始化Redis客户端
        self.redis_client = redis.Redis(**redis_config)
        
        # 初始化MySQL引擎
        self.mysql_engine = create_engine(
            mysql_config['connection_string'],
            poolclass=QueuePool,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True
        )
        
        # 创建表
        Base.metadata.create_all(self.mysql_engine)
        
        # 初始化健康检查器
        self.health_checker = RedisHealthChecker(self.redis_client, self.mysql_engine)
        
        # 初始化时检查Redis状态
        if not self.health_checker.check_redis_health():
            self.health_checker.mark_redis_down()
    
    @contextmanager
    def _get_mysql_session(self) -> Session:
        """获取MySQL会话的上下文管理器"""
        SessionLocal = sessionmaker(bind=self.mysql_engine)
        session = SessionLocal()
        try:
            yield session
        finally:
            session.close()
    
    def _clean_expired_mysql_data(self, session: Session):
        """
        清除MySQL中过期的数据
        
        Args:
            session: SQLAlchemy会话
        """
        try:
            now = datetime.now()
            deleted_count = session.query(RedisGatewayInfo).filter(
                RedisGatewayInfo.expire_time.isnot(None),
                RedisGatewayInfo.expire_time < now
            ).delete()
            session.commit()
            
            if deleted_count > 0:
                logger.info(f"清理了{deleted_count}条过期的MySQL数据")
                
        except Exception as e:
            session.rollback()
            logger.error(f"清理过期数据失败: {e}")
    
    def _write_to_mysql(self, key: str, name: str, value: Any, expire_time: Optional[datetime] = None):
        """
        异步写入MySQL
        
        Args:
            key: 键
            name: 名称（用于hash等结构）
            value: 值
            expire_time: 过期时间
        """
        def _write():
            with self._get_mysql_session() as session:
                try:
                    # 查询是否存在
                    record = session.query(RedisGatewayInfo).filter(
                        RedisGatewayInfo.key == key,
                        RedisGatewayInfo.name == name
                    ).first()
                    
                    now = datetime.now()
                    value_str = json.dumps(value) if not isinstance(value, str) else value
                    
                    if record:
                        # 更新
                        record.value = value_str
                        record.expire_time = expire_time
                        record.last_update_time = now
                    else:
                        # 插入
                        new_record = RedisGatewayInfo(
                            key=key,
                            name=name,
                            value=value_str,
                            expire_time=expire_time,
                            last_update_time=now
                        )
                        session.add(new_record)
                    
                    session.commit()
                except Exception as e:
                    session.rollback()
                    logger.error(f"写入MySQL失败 key={key}, name={name}: {e}")
        
        # 在独立线程中执行写入
        threading.Thread(target=_write, daemon=True).start()
    
    def _read_from_mysql(self, key: str, name: str = '_string') -> Optional[str]:
        """
        从MySQL读取数据
        
        Args:
            key: 键
            name: 名称
            
        Returns:
            读取到的值，如果不存在或已过期返回None
        """
        with self._get_mysql_session() as session:
            # 清除过期数据
            self._clean_expired_mysql_data(session)
            
            # 查询数据
            record = session.query(RedisGatewayInfo).filter(
                RedisGatewayInfo.key == key,
                RedisGatewayInfo.name == name
            ).first()
            
            if not record:
                return None
            
            # 检查是否过期
            if record.expire_time and record.expire_time < datetime.now():
                return None
            
            return record.value
    
    # ==================== String操作 ====================
    
    def set(self, key: str, value: Any):
        """
        设置字符串值
        
        Args:
            key: 键
            value: 值
        """
        value_str = json.dumps(value) if not isinstance(value, str) else value
        
        if self.health_checker.redis_alive:
            try:
                self.redis_client.set(key, value_str)
            except Exception as e:
                logger.error(f"Redis set失败: {e}")
                self.health_checker.mark_redis_down()
        
        # 异步写入MySQL
        self._write_to_mysql(key, '_string', value_str)
    
    def set_ex(self, key: str, value: Any, ttl: int):
        """
        设置带过期时间的字符串值
        
        Args:
            key: 键
            value: 值
            ttl: 过期时间（秒）
        """
        value_str = json.dumps(value) if not isinstance(value, str) else value
        
        if self.health_checker.redis_alive:
            try:
                self.redis_client.setex(key, ttl, value_str)
            except Exception as e:
                logger.error(f"Redis setex失败: {e}")
                self.health_checker.mark_redis_down()
        
        # 异步写入MySQL
        expire_time = datetime.now() + timedelta(seconds=ttl)
        self._write_to_mysql(key, '_string', value_str, expire_time)
    
    def get(self, key: str) -> Optional[Any]:
        """
        获取字符串值
        
        Args:
            key: 键
            
        Returns:
            值，如果不存在返回None
        """
        if self.health_checker.redis_alive:
            try:
                result = self.redis_client.get(key)
                if result:
                    return result.decode('utf-8') if isinstance(result, bytes) else result
            except Exception as e:
                logger.error(f"Redis get失败: {e}")
                self.health_checker.mark_redis_down()
        
        # 降级到MySQL
        return self._read_from_mysql(key, '_string')
    
    def delete(self, key: str) -> int:
        """
        删除键
        
        Args:
            key: 键
            
        Returns:
            删除的数量
        """
        count = 0
        
        if self.health_checker.redis_alive:
            try:
                count = self.redis_client.delete(key)
            except Exception as e:
                logger.error(f"Redis delete失败: {e}")
                self.health_checker.mark_redis_down()
        
        # 从MySQL删除
        with self._get_mysql_session() as session:
            try:
                deleted = session.query(RedisGatewayInfo).filter(
                    RedisGatewayInfo.key == key
                ).delete()
                session.commit()
                count = max(count, deleted)
            except Exception as e:
                session.rollback()
                logger.error(f"MySQL delete失败: {e}")
        
        return count
    
    def exists(self, key: str) -> bool:
        """
        检查键是否存在
        
        Args:
            key: 键
            
        Returns:
            是否存在
        """
        if self.health_checker.redis_alive:
            try:
                return bool(self.redis_client.exists(key))
            except Exception as e:
                logger.error(f"Redis exists失败: {e}")
                self.health_checker.mark_redis_down()
        
        # 降级到MySQL
        with self._get_mysql_session() as session:
            self._clean_expired_mysql_data(session)
            return session.query(RedisGatewayInfo).filter(
                RedisGatewayInfo.key == key
            ).count() > 0
    
    def expire(self, key: str, ttl: int) -> bool:
        """
        设置过期时间
        
        Args:
            key: 键
            ttl: 过期时间（秒）
            
        Returns:
            是否成功
        """
        success = False
        
        if self.health_checker.redis_alive:
            try:
                success = bool(self.redis_client.expire(key, ttl))
            except Exception as e:
                logger.error(f"Redis expire失败: {e}")
                self.health_checker.mark_redis_down()
        
        # 更新MySQL中的过期时间
        with self._get_mysql_session() as session:
            try:
                expire_time = datetime.now() + timedelta(seconds=ttl)
                updated = session.query(RedisGatewayInfo).filter(
                    RedisGatewayInfo.key == key
                ).update({'expire_time': expire_time})
                session.commit()
                success = success or (updated > 0)
            except Exception as e:
                session.rollback()
                logger.error(f"MySQL expire失败: {e}")
        
        return success
    
    # ==================== Hash操作 ====================
    
    def hset(self, name: str, key: str, value: Any) -> int:
        """
        设置Hash字段
        
        Args:
            name: Hash名称
            key: 字段名
            value: 值
            
        Returns:
            新增字段数量
        """
        value_str = json.dumps(value) if not isinstance(value, str) else value
        count = 0
        
        if self.health_checker.redis_alive:
            try:
                count = self.redis_client.hset(name, key, value_str)
            except Exception as e:
                logger.error(f"Redis hset失败: {e}")
                self.health_checker.mark_redis_down()
        
        # 异步写入MySQL
        self._write_to_mysql(name, key, value_str)
        
        return count
    
    def hget(self, name: str, key: str) -> Optional[Any]:
        """
        获取Hash字段值
        
        Args:
            name: Hash名称
            key: 字段名
            
        Returns:
            字段值，如果不存在返回None
        """
        if self.health_checker.redis_alive:
            try:
                result = self.redis_client.hget(name, key)
                if result:
                    return result.decode('utf-8') if isinstance(result, bytes) else result
            except Exception as e:
                logger.error(f"Redis hget失败: {e}")
                self.health_checker.mark_redis_down()
        
        # 降级到MySQL
        return self._read_from_mysql(name, key)
    
    def hdel(self, name: str, *keys) -> int:
        """
        删除Hash字段
        
        Args:
            name: Hash名称
            keys: 字段名列表
            
        Returns:
            删除的字段数量
        """
        count = 0
        
        if self.health_checker.redis_alive:
            try:
                count = self.redis_client.hdel(name, *keys)
            except Exception as e:
                logger.error(f"Redis hdel失败: {e}")
                self.health_checker.mark_redis_down()
        
        # 从MySQL删除
        with self._get_mysql_session() as session:
            try:
                deleted = session.query(RedisGatewayInfo).filter(
                    RedisGatewayInfo.key == name,
                    RedisGatewayInfo.name.in_(keys)
                ).delete(synchronize_session=False)
                session.commit()
                count = max(count, deleted)
            except Exception as e:
                session.rollback()
                logger.error(f"MySQL hdel失败: {e}")
        
        return count
    
    def hgetall(self, name: str) -> dict:
        """
        获取Hash所有字段
        
        Args:
            name: Hash名称
            
        Returns:
            所有字段的字典
        """
        if self.health_checker.redis_alive:
            try:
                result = self.redis_client.hgetall(name)
                return {
                    k.decode('utf-8') if isinstance(k, bytes) else k: 
                    v.decode('utf-8') if isinstance(v, bytes) else v
                    for k, v in result.items()
                }
            except Exception as e:
                logger.error(f"Redis hgetall失败: {e}")
                self.health_checker.mark_redis_down()
        
        # 降级到MySQL
        with self._get_mysql_session() as session:
            self._clean_expired_mysql_data(session)
            records = session.query(RedisGatewayInfo).filter(
                RedisGatewayInfo.key == name,
                RedisGatewayInfo.name != '_string'
            ).all()
            
            return {record.name: record.value for record in records}
    
    # ==================== List操作 ====================
    
    def rpush(self, name: str, *values) -> int:
        """
        从右侧推入列表
        
        Args:
            name: 列表名称
            values: 值列表
            
        Returns:
            列表长度
        """
        length = 0
        
        if self.health_checker.redis_alive:
            try:
                # Redis中的值需要序列化
                redis_values = [json.dumps(v) if not isinstance(v, str) else v for v in values]
                length = self.redis_client.rpush(name, *redis_values)
            except Exception as e:
                logger.error(f"Redis rpush失败: {e}")
                self.health_checker.mark_redis_down()
        
        # 写入MySQL（使用特殊的name格式标识list）
        with self._get_mysql_session() as session:
            try:
                # 获取当前列表长度
                max_index = session.query(RedisGatewayInfo).filter(
                    RedisGatewayInfo.key == name,
                    RedisGatewayInfo.name.like('_list_%')
                ).count()
                
                for i, value in enumerate(values):
                    value_str = json.dumps(value) if not isinstance(value, str) else value
                    index = max_index + i
                    record = RedisGatewayInfo(
                        key=name,
                        name=f'_list_{index}',
                        value=value_str,
                        last_update_time=datetime.now()
                    )
                    session.add(record)
                
                session.commit()
                length = max_index + len(values)
            except Exception as e:
                session.rollback()
                logger.error(f"MySQL rpush失败: {e}")
        
        return length
    
    def lpush(self, name: str, *values) -> int:
        """
        从左侧推入列表
        
        Args:
            name: 列表名称
            values: 值列表
            
        Returns:
            列表长度
        """
        length = 0
        
        if self.health_checker.redis_alive:
            try:
                redis_values = [json.dumps(v) if not isinstance(v, str) else v for v in values]
                length = self.redis_client.lpush(name, *redis_values)
            except Exception as e:
                logger.error(f"Redis lpush失败: {e}")
                self.health_checker.mark_redis_down()
        
        # 写入MySQL（需要重新索引所有元素）
        with self._get_mysql_session() as session:
            try:
                # 获取现有列表
                existing = session.query(RedisGatewayInfo).filter(
                    RedisGatewayInfo.key == name,
                    RedisGatewayInfo.name.like('_list_%')
                ).order_by(RedisGatewayInfo.name).all()
                
                # 删除现有记录
                session.query(RedisGatewayInfo).filter(
                    RedisGatewayInfo.key == name,
                    RedisGatewayInfo.name.like('_list_%')
                ).delete()
                
                # 插入新值
                for i, value in enumerate(values):
                    value_str = json.dumps(value) if not isinstance(value, str) else value
                    record = RedisGatewayInfo(
                        key=name,
                        name=f'_list_{i}',
                        value=value_str,
                        last_update_time=datetime.now()
                    )
                    session.add(record)
                
                # 重新插入现有值
                for i, record in enumerate(existing):
                    record.name = f'_list_{len(values) + i}'
                    session.add(record)
                
                session.commit()
                length = len(values) + len(existing)
            except Exception as e:
                session.rollback()
                logger.error(f"MySQL lpush失败: {e}")
        
        return length
    
    def lrange(self, name: str, start: int = 0, end: int = -1) -> List[Any]:
        """
        获取列表范围
        
        Args:
            name: 列表名称
            start: 起始索引
            end: 结束索引（-1表示到末尾）
            
        Returns:
            值列表
        """
        if self.health_checker.redis_alive:
            try:
                result = self.redis_client.lrange(name, start, end)
                return [
                    item.decode('utf-8') if isinstance(item, bytes) else item 
                    for item in result
                ]
            except Exception as e:
                logger.error(f"Redis lrange失败: {e}")
                self.health_checker.mark_redis_down()
        
        # 降级到MySQL
        with self._get_mysql_session() as session:
            self._clean_expired_mysql_data(session)
            records = session.query(RedisGatewayInfo).filter(
                RedisGatewayInfo.key == name,
                RedisGatewayInfo.name.like('_list_%')
            ).order_by(RedisGatewayInfo.name).all()
            
            values = [record.value for record in records]
            
            # 处理负索引
            if end == -1:
                end = len(values)
            elif end < 0:
                end = len(values) + end + 1
            else:
                end = end + 1
            
            if start < 0:
                start = len(values) + start
            
            return values[start:end]
    
    def lpop(self, key: str, count: Optional[int] = None) -> Union[Optional[str], List[str]]:
        """
        从左侧弹出元素
        
        Args:
            key: 列表键
            count: 弹出数量
            
        Returns:
            弹出的值或值列表
        """
        if self.health_checker.redis_alive:
            try:
                if count is not None:
                    result = self.redis_client.lpop(key, count)
                else:
                    result = self.redis_client.lpop(key)
                
                if isinstance(result, bytes):
                    return result.decode('utf-8')
                elif isinstance(result, list):
                    return [item.decode('utf-8') if isinstance(item, bytes) else item for item in result]
                return result
            except Exception as e:
                logger.error(f"Redis lpop失败: {e}")
                self.health_checker.mark_redis_down()
        
        # 降级到MySQL
        with self._get_mysql_session() as session:
            try:
                records = session.query(RedisGatewayInfo).filter(
                    RedisGatewayInfo.key == key,
                    RedisGatewayInfo.name.like('_list_%')
                ).order_by(RedisGatewayInfo.name).limit(count or 1).all()
                
                if not records:
                    return None if count is None else []
                
                # 删除弹出的记录
                for record in records:
                    session.delete(record)
                
                session.commit()
                
                values = [record.value for record in records]
                return values[0] if count is None else values
            except Exception as e:
                session.rollback()
                logger.error(f"MySQL lpop失败: {e}")
                return None if count is None else []
    
    def lindex(self, key: str, index: int) -> Optional[str]:
        """
        获取列表指定索引的元素
        
        Args:
            key: 列表键
            index: 索引
            
        Returns:
            元素值
        """
        if self.health_checker.redis_alive:
            try:
                result = self.redis_client.lindex(key, index)
                return result.decode('utf-8') if isinstance(result, bytes) else result
            except Exception as e:
                logger.error(f"Redis lindex失败: {e}")
                self.health_checker.mark_redis_down()
        
        # 降级到MySQL
        values = self.lrange(key, 0, -1)
        if 0 <= index < len(values):
            return values[index]
        elif index < 0 and -index <= len(values):
            return values[index]
        return None
    
    def llen(self, key: str) -> int:
        """
        获取列表长度
        
        Args:
            key: 列表键
            
        Returns:
            列表长度
        """
        if self.health_checker.redis_alive:
            try:
                return self.redis_client.llen(key)
            except Exception as e:
                logger.error(f"Redis llen失败: {e}")
                self.health_checker.mark_redis_down()
        
        # 降级到MySQL
        with self._get_mysql_session() as session:
            return session.query(RedisGatewayInfo).filter(
                RedisGatewayInfo.key == key,
                RedisGatewayInfo.name.like('_list_%')
            ).count()
    
    def lrem(self, key: str, count: int, value: Any) -> int:
        """
        删除列表中的元素
        
        Args:
            key: 列表键
            count: 删除数量（0表示全部）
            value: 要删除的值
            
        Returns:
            删除的数量
        """
        value_str = json.dumps(value) if not isinstance(value, str) else value
        removed = 0
        
        if self.health_checker.redis_alive:
            try:
                removed = self.redis_client.lrem(key, count, value_str)
            except Exception as e:
                logger.error(f"Redis lrem失败: {e}")
                self.health_checker.mark_redis_down()
        
        # 从MySQL删除
        with self._get_mysql_session() as session:
            try:
                query = session.query(RedisGatewayInfo).filter(
                    RedisGatewayInfo.key == key,
                    RedisGatewayInfo.name.like('_list_%'),
                    RedisGatewayInfo.value == value_str
                )
                
                if count > 0:
                    records = query.limit(count).all()
                elif count < 0:
                    records = query.order_by(RedisGatewayInfo.name.desc()).limit(-count).all()
                else:
                    records = query.all()
                
                for record in records:
                    session.delete(record)
                
                session.commit()
                removed = max(removed, len(records))
            except Exception as e:
                session.rollback()
                logger.error(f"MySQL lrem失败: {e}")
        
        return removed
    
    # ==================== 分布式锁 ====================
    
    @contextmanager
    def distributed_lock(self, lock_key: str, timeout: int = 10):
        """
        Redis/MySQL分布式锁上下文管理器
        
        Args:
            lock_key: 锁的键名
            timeout: 锁的超时时间（秒）
            
        Yields:
            bool: 是否成功获取锁
        """
        lock_acquired = False
        redis_lock = None
        
        try:
            if self.health_checker.redis_alive:
                try:
                    # 尝试使用Redis锁
                    redis_lock = self.redis_client.lock(
                        lock_key, 
                        timeout=timeout,
                        blocking_timeout=0
                    )
                    lock_acquired = redis_lock.acquire(blocking=False)
                except Exception as e:
                    logger.error(f"Redis锁获取失败: {e}")
                    self.health_checker.mark_redis_down()
            
            if not lock_acquired:
                # 降级到MySQL锁
                lock_acquired = self.health_checker._acquire_distributed_lock(lock_key, timeout)
            
            yield lock_acquired
            
        finally:
            if redis_lock and lock_acquired:
                try:
                    redis_lock.release()
                except:
                    pass
            elif lock_acquired:
                self.health_checker._release_distributed_lock(lock_key)
    
    def cleanup_expired_data(self):
        """清理MySQL中的过期数据（供定时任务调用）"""
        with self._get_mysql_session() as session:
            self._clean_expired_mysql_data(session)
            logger.info("定时清理过期数据完成")
    
    def start_scheduled_tasks(self):
        """启动定时任务"""
        # 每60秒清理一次过期数据
        schedule.every(60).seconds.do(self.cleanup_expired_data)
        
        # 在独立线程中运行调度器
        def run_scheduler():
            while True:
                schedule.run_pending()
                time.sleep(1)
        
        scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
        scheduler_thread.start()
        logger.info("定时任务已启动：每60秒清理一次过期数据")
    
    def close(self):
        """关闭连接"""
        self.health_checker.stop()
        self.redis_client.close()
        self.mysql_engine.dispose()
