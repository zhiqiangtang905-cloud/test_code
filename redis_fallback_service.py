"""
Redis降级服务
提供Redis操作的降级到MySQL的完整实现
当Redis不可用时，自动降级到MySQL进行数据读写
"""
import json
import logging
import threading
import time
import schedule
from datetime import datetime, timedelta
from typing import Any, Optional, List
from contextlib import contextmanager
from sqlalchemy import and_, or_

from redis_models import RedisGatewayInfo
from redis_health_checker import RedisHealthChecker

# 假设从外部导入这两个函数
# from your_module import get_redis_cache_service, get_db_session

logger = logging.getLogger(__name__)


class RedisDistributedLock:
    """
    基于Redis的分布式锁
    用于确保多实例环境下只有一个实例执行特定任务
    """
    
    def __init__(self, redis_service, lock_key: str, timeout: int = 30):
        """
        Args:
            redis_service: Redis服务实例
            lock_key: 锁的键名
            timeout: 锁的超时时间（秒）
        """
        self.redis_service = redis_service
        self.lock_key = lock_key
        self.timeout = timeout
        self.lock_value = f"{threading.current_thread().ident}_{time.time()}"
    
    def __enter__(self):
        """获取锁"""
        return self.acquire()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """释放锁"""
        self.release()
    
    def acquire(self) -> bool:
        """
        尝试获取分布式锁
        Returns:
            bool: 是否成功获取锁
        """
        try:
            # 使用SET NX EX原子操作获取锁
            redis_client = self.redis_service
            result = redis_client.set(
                self.lock_key,
                self.lock_value,
                nx=True,  # 只在键不存在时设置
                ex=self.timeout  # 设置过期时间
            )
            if result:
                logger.debug(f"成功获取分布式锁: {self.lock_key}")
                return True
            else:
                logger.debug(f"未能获取分布式锁: {self.lock_key}，锁已被其他实例持有")
                return False
        except Exception as e:
            logger.error(f"获取分布式锁异常: {e}")
            return False
    
    def release(self):
        """释放分布式锁"""
        try:
            # 只有持有锁的实例才能释放（检查value）
            redis_client = self.redis_service
            current_value = redis_client.get(self.lock_key)
            if current_value and current_value.decode('utf-8') == self.lock_value:
                redis_client.delete(self.lock_key)
                logger.debug(f"成功释放分布式锁: {self.lock_key}")
        except Exception as e:
            logger.error(f"释放分布式锁异常: {e}")


class RedisContextManager:
    """
    Redis上下文管理器（分布式锁）
    用于with语句中管理分布式锁
    """
    
    def __init__(self, redis_service, lock_key: str, timeout: int = 30):
        self.lock = RedisDistributedLock(redis_service, lock_key, timeout)
    
    def __enter__(self):
        if not self.lock.acquire():
            raise RuntimeError(f"无法获取分布式锁: {self.lock.lock_key}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.lock.release()


class RedisFallbackService:
    """
    Redis降级服务
    提供Redis操作及其MySQL降级能力
    """
    
    def __init__(self, get_redis_func, get_db_session_func):
        """
        Args:
            get_redis_func: 获取Redis服务的函数
            get_db_session_func: 获取数据库会话的函数
        """
        self.get_redis = get_redis_func
        self.get_db_session = get_db_session_func
        
        # 健康检查器
        self.health_checker = RedisHealthChecker()
        
        # 定时任务调度器线程
        self._scheduler_thread: Optional[threading.Thread] = None
        self._scheduler_running = False
        
        logger.info("RedisFallbackService初始化完成")
    
    def _get_redis_service(self):
        """获取Redis服务实例"""
        return self.get_redis()
    
    def _serialize_value(self, value: Any) -> str:
        """
        序列化值为JSON字符串
        Redis中的数据需要用json.dumps()转为字符串
        """
        if isinstance(value, (str, bytes)):
            return value if isinstance(value, str) else value.decode('utf-8')
        return json.dumps(value, ensure_ascii=False)
    
    def _deserialize_value(self, value: str) -> Any:
        """
        反序列化JSON字符串
        从Redis或MySQL取出的数据使用json.loads()转换
        """
        if not value:
            return None
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return value
    
    def _write_to_mysql(self, key: str, name: str, value: Any, ttl: Optional[int] = None):
        """
        异步写入MySQL
        
        Args:
            key: Redis键名
            name: Hash/List等的name字段，普通key可传空字符串
            value: 要存储的值
            ttl: 过期时间（秒），None表示永不过期
        """
        def _async_write():
            try:
                with self.get_db_session() as session:
                    # 计算过期时间
                    expire_time = None
                    if ttl:
                        expire_time = datetime.now() + timedelta(seconds=ttl)
                    
                    # 序列化value
                    serialized_value = self._serialize_value(value)
                    
                    # 查询是否已存在
                    existing = session.query(RedisGatewayInfo).filter(
                        and_(
                            RedisGatewayInfo.key == key,
                            RedisGatewayInfo.name == name
                        )
                    ).first()
                    
                    if existing:
                        # 更新现有记录
                        existing.value = serialized_value
                        existing.expire_time = expire_time
                        existing.last_update_time = datetime.now()
                    else:
                        # 插入新记录
                        new_record = RedisGatewayInfo(
                            key=key,
                            name=name,
                            value=serialized_value,
                            expire_time=expire_time,
                            last_update_time=datetime.now()
                        )
                        session.add(new_record)
                    
                    session.commit()
                    logger.debug(f"成功写入MySQL: key={key}, name={name}")
                    
            except Exception as e:
                logger.error(f"写入MySQL失败: key={key}, name={name}, error={e}")
        
        # 在新线程中异步执行
        threading.Thread(target=_async_write, daemon=True).start()
    
    def _delete_from_mysql(self, key: str, name: str = ''):
        """
        从MySQL中删除记录
        
        Args:
            key: Redis键名
            name: name字段，默认为空字符串
        """
        def _async_delete():
            try:
                with self.get_db_session() as session:
                    session.query(RedisGatewayInfo).filter(
                        and_(
                            RedisGatewayInfo.key == key,
                            RedisGatewayInfo.name == name
                        )
                    ).delete()
                    session.commit()
                    logger.debug(f"成功从MySQL删除: key={key}, name={name}")
            except Exception as e:
                logger.error(f"从MySQL删除失败: key={key}, name={name}, error={e}")
        
        threading.Thread(target=_async_delete, daemon=True).start()
    
    def _read_from_mysql(self, key: str, name: str = '') -> Optional[Any]:
        """
        从MySQL读取数据
        读取前会先清理过期数据
        
        Args:
            key: Redis键名
            name: name字段，默认为空字符串
        Returns:
            读取到的值，不存在返回None
        """
        try:
            with self.get_db_session() as session:
                # 先清理该key的过期数据
                self._cleanup_expired_data_for_key(session, key, name)
                
                # 查询数据
                record = session.query(RedisGatewayInfo).filter(
                    and_(
                        RedisGatewayInfo.key == key,
                        RedisGatewayInfo.name == name
                    )
                ).first()
                
                if record:
                    logger.debug(f"从MySQL读取成功: key={key}, name={name}")
                    return self._deserialize_value(record.value)
                else:
                    logger.debug(f"MySQL中未找到数据: key={key}, name={name}")
                    return None
                    
        except Exception as e:
            logger.error(f"从MySQL读取失败: key={key}, name={name}, error={e}")
            return None
    
    def _cleanup_expired_data_for_key(self, session, key: str, name: str):
        """
        清理指定key的过期数据
        
        Args:
            session: 数据库会话
            key: 要清理的key
            name: 要清理的name
        """
        try:
            now = datetime.now()
            session.query(RedisGatewayInfo).filter(
                and_(
                    RedisGatewayInfo.key == key,
                    RedisGatewayInfo.name == name,
                    RedisGatewayInfo.expire_time.isnot(None),
                    RedisGatewayInfo.expire_time < now
                )
            ).delete()
            session.commit()
        except Exception as e:
            logger.error(f"清理过期数据失败: {e}")
    
    def cleanup_all_expired_data(self):
        """
        清理所有过期数据
        由定时任务定期调用
        """
        try:
            # 使用分布式锁确保只有一个实例执行清理
            redis_service = self._get_redis_service()
            lock = RedisDistributedLock(redis_service, "lock:cleanup_expired_data", timeout=300)
            
            if lock.acquire():
                try:
                    logger.info("开始清理MySQL中的过期数据")
                    with self.get_db_session() as session:
                        now = datetime.now()
                        deleted_count = session.query(RedisGatewayInfo).filter(
                            and_(
                                RedisGatewayInfo.expire_time.isnot(None),
                                RedisGatewayInfo.expire_time < now
                            )
                        ).delete()
                        session.commit()
                        logger.info(f"清理完成，删除了{deleted_count}条过期数据")
                finally:
                    lock.release()
            else:
                logger.debug("其他实例正在清理过期数据，跳过本次清理")
                
        except Exception as e:
            logger.error(f"清理所有过期数据失败: {e}")
    
    def _check_redis_health(self) -> bool:
        """
        检查Redis健康状态
        Returns:
            bool: Redis是否可用
        """
        try:
            redis_service = self._get_redis_service()
            # 尝试执行一个简单的ping操作
            redis_service.ping()
            return True
        except Exception as e:
            logger.debug(f"Redis健康检查失败: {e}")
            return False
    
    def _sync_mysql_to_redis(self):
        """
        将MySQL中的数据同步回Redis
        在Redis恢复可用后调用
        """
        try:
            logger.info("开始将MySQL数据同步回Redis")
            redis_service = self._get_redis_service()
            
            with self.get_db_session() as session:
                # 只同步未过期的数据
                now = datetime.now()
                records = session.query(RedisGatewayInfo).filter(
                    or_(
                        RedisGatewayInfo.expire_time.is_(None),
                        RedisGatewayInfo.expire_time > now
                    )
                ).all()
                
                sync_count = 0
                for record in records:
                    try:
                        # 根据不同的数据类型进行同步
                        value = record.value
                        
                        # 计算剩余TTL
                        ttl = None
                        if record.expire_time:
                            remaining = (record.expire_time - now).total_seconds()
                            ttl = int(remaining) if remaining > 0 else None
                        
                        if record.name:
                            # Hash类型数据
                            redis_service.hset(record.key, record.name, value)
                            if ttl:
                                redis_service.expire(record.key, ttl)
                        else:
                            # 普通String类型数据
                            if ttl:
                                redis_service.setex(record.key, ttl, value)
                            else:
                                redis_service.set(record.key, value)
                        
                        sync_count += 1
                        
                    except Exception as e:
                        logger.error(f"同步单条记录失败: key={record.key}, name={record.name}, error={e}")
                
                logger.info(f"MySQL数据同步完成，共同步{sync_count}条记录")
                
        except Exception as e:
            logger.error(f"MySQL数据同步到Redis失败: {e}")
    
    def _on_redis_recovered(self):
        """
        Redis恢复后的回调函数
        执行数据同步并停止健康拨测
        """
        logger.info("检测到Redis服务恢复，开始执行恢复流程")
        
        # 同步MySQL数据到Redis
        self._sync_mysql_to_redis()
        
        # 停止健康拨测
        self.health_checker.stop_probing()
        
        logger.info("Redis恢复流程完成")
    
    def _probe_and_recover(self) -> bool:
        """
        健康拨测函数
        Returns:
            bool: Redis是否可用
        """
        is_healthy = self._check_redis_health()
        
        if is_healthy:
            # Redis恢复，执行恢复流程
            self._on_redis_recovered()
            return True
        
        return False
    
    def _handle_redis_error(self, operation: str):
        """
        处理Redis错误
        标记Redis为不可用，并启动健康拨测
        
        Args:
            operation: 出错的操作名称
        """
        logger.warning(f"Redis操作失败: {operation}")
        
        # 标记Redis为不可用
        self.health_checker.mark_redis_down()
        
        # 启动健康拨测（如果尚未启动）
        if not self.health_checker.is_probing:
            self.health_checker.start_probing(
                probe_func=self._probe_and_recover,
                interval=10
            )
    
    # ==================== Redis操作封装 ====================
    
    def set_ex(self, key: str, value: Any, ttl: int):
        """
        设置带过期时间的键值对
        
        Args:
            key: 键名
            value: 值
            ttl: 过期时间（秒）
        """
        try:
            if self.health_checker.is_redis_alive:
                redis_service = self._get_redis_service()
                serialized_value = self._serialize_value(value)
                redis_service.setex(key, ttl, serialized_value)
                logger.debug(f"Redis写入成功: key={key}, ttl={ttl}")
        except Exception as e:
            logger.error(f"Redis set_ex失败: key={key}, error={e}")
            self._handle_redis_error("set_ex")
        
        # 异步写入MySQL
        self._write_to_mysql(key, '', value, ttl)
    
    def get(self, key: str) -> Optional[Any]:
        """
        获取键对应的值
        
        Args:
            key: 键名
        Returns:
            键对应的值，不存在返回None
        """
        try:
            if self.health_checker.is_redis_alive:
                redis_service = self._get_redis_service()
                value = redis_service.get(key)
                if value:
                    logger.debug(f"Redis读取成功: key={key}")
                    return self._deserialize_value(value)
                return None
        except Exception as e:
            logger.error(f"Redis get失败: key={key}, error={e}")
            self._handle_redis_error("get")
        
        # 降级到MySQL
        logger.info(f"降级到MySQL读取: key={key}")
        return self._read_from_mysql(key, '')
    
    def hset(self, name: str, key: str, value: Any):
        """
        设置Hash字段
        
        Args:
            name: Hash名称
            key: 字段名
            value: 字段值
        """
        try:
            if self.health_checker.is_redis_alive:
                redis_service = self._get_redis_service()
                serialized_value = self._serialize_value(value)
                redis_service.hset(name, key, serialized_value)
                logger.debug(f"Redis hset成功: name={name}, key={key}")
        except Exception as e:
            logger.error(f"Redis hset失败: name={name}, key={key}, error={e}")
            self._handle_redis_error("hset")
        
        # 异步写入MySQL
        self._write_to_mysql(name, key, value)
    
    def hget(self, name: str, key: str) -> Optional[Any]:
        """
        获取Hash字段值
        
        Args:
            name: Hash名称
            key: 字段名
        Returns:
            字段值，不存在返回None
        """
        try:
            if self.health_checker.is_redis_alive:
                redis_service = self._get_redis_service()
                value = redis_service.hget(name, key)
                if value:
                    logger.debug(f"Redis hget成功: name={name}, key={key}")
                    return self._deserialize_value(value)
                return None
        except Exception as e:
            logger.error(f"Redis hget失败: name={name}, key={key}, error={e}")
            self._handle_redis_error("hget")
        
        # 降级到MySQL
        logger.info(f"降级到MySQL读取: name={name}, key={key}")
        return self._read_from_mysql(name, key)
    
    def hdel(self, name: str, *keys):
        """
        删除Hash字段
        
        Args:
            name: Hash名称
            keys: 要删除的字段名
        """
        try:
            if self.health_checker.is_redis_alive:
                redis_service = self._get_redis_service()
                redis_service.hdel(name, *keys)
                logger.debug(f"Redis hdel成功: name={name}, keys={keys}")
        except Exception as e:
            logger.error(f"Redis hdel失败: name={name}, keys={keys}, error={e}")
            self._handle_redis_error("hdel")
        
        # 异步从MySQL删除
        for key in keys:
            self._delete_from_mysql(name, key)
    
    def hgetall(self, name: str) -> dict:
        """
        获取Hash的所有字段和值
        
        Args:
            name: Hash名称
        Returns:
            字典，包含所有字段和值
        """
        try:
            if self.health_checker.is_redis_alive:
                redis_service = self._get_redis_service()
                result = redis_service.hgetall(name)
                logger.debug(f"Redis hgetall成功: name={name}")
                # 反序列化所有值
                return {k.decode('utf-8') if isinstance(k, bytes) else k: 
                        self._deserialize_value(v) for k, v in result.items()}
        except Exception as e:
            logger.error(f"Redis hgetall失败: name={name}, error={e}")
            self._handle_redis_error("hgetall")
        
        # 降级到MySQL
        logger.info(f"降级到MySQL读取hgetall: name={name}")
        try:
            with self.get_db_session() as session:
                records = session.query(RedisGatewayInfo).filter(
                    RedisGatewayInfo.key == name
                ).all()
                result = {}
                for record in records:
                    if record.name:  # 只获取有name的记录（Hash字段）
                        result[record.name] = self._deserialize_value(record.value)
                return result
        except Exception as e:
            logger.error(f"从MySQL读取hgetall失败: name={name}, error={e}")
            return {}
    
    def rpush(self, name: str, *values):
        """
        向列表右侧推入元素
        
        Args:
            name: 列表名称
            values: 要推入的值
        """
        try:
            if self.health_checker.is_redis_alive:
                redis_service = self._get_redis_service()
                serialized_values = [self._serialize_value(v) for v in values]
                redis_service.rpush(name, *serialized_values)
                logger.debug(f"Redis rpush成功: name={name}")
        except Exception as e:
            logger.error(f"Redis rpush失败: name={name}, error={e}")
            self._handle_redis_error("rpush")
        
        # 异步写入MySQL（列表需要特殊处理）
        for idx, value in enumerate(values):
            # 使用索引作为name的一部分来模拟列表
            self._write_to_mysql(name, f"list_item_{int(time.time()*1000000)}_{idx}", value)
    
    def lpush(self, name: str, *values):
        """
        向列表左侧推入元素
        
        Args:
            name: 列表名称
            values: 要推入的值
        """
        try:
            if self.health_checker.is_redis_alive:
                redis_service = self._get_redis_service()
                serialized_values = [self._serialize_value(v) for v in values]
                redis_service.lpush(name, *serialized_values)
                logger.debug(f"Redis lpush成功: name={name}")
        except Exception as e:
            logger.error(f"Redis lpush失败: name={name}, error={e}")
            self._handle_redis_error("lpush")
        
        # 异步写入MySQL
        for idx, value in enumerate(values):
            self._write_to_mysql(name, f"list_item_{int(time.time()*1000000)}_{idx}", value)
    
    def lrange(self, name: str, start: int = 0, end: int = -1) -> List[Any]:
        """
        获取列表指定范围的元素
        
        Args:
            name: 列表名称
            start: 起始索引
            end: 结束索引，-1表示到末尾
        Returns:
            列表元素
        """
        try:
            if self.health_checker.is_redis_alive:
                redis_service = self._get_redis_service()
                values = redis_service.lrange(name, start, end)
                logger.debug(f"Redis lrange成功: name={name}")
                return [self._deserialize_value(v) for v in values]
        except Exception as e:
            logger.error(f"Redis lrange失败: name={name}, error={e}")
            self._handle_redis_error("lrange")
        
        # 降级到MySQL
        logger.info(f"降级到MySQL读取lrange: name={name}")
        try:
            with self.get_db_session() as session:
                records = session.query(RedisGatewayInfo).filter(
                    RedisGatewayInfo.key == name
                ).order_by(RedisGatewayInfo.name).all()
                result = [self._deserialize_value(r.value) for r in records]
                # 处理start和end
                if end == -1:
                    return result[start:]
                else:
                    return result[start:end+1]
        except Exception as e:
            logger.error(f"从MySQL读取lrange失败: name={name}, error={e}")
            return []
    
    def lpop(self, key: str, count: int = 1) -> Optional[Any]:
        """
        从列表左侧弹出元素
        
        Args:
            key: 列表名称
            count: 弹出的元素数量
        Returns:
            弹出的元素
        """
        try:
            if self.health_checker.is_redis_alive:
                redis_service = self._get_redis_service()
                if count == 1:
                    value = redis_service.lpop(key)
                    logger.debug(f"Redis lpop成功: key={key}")
                    return self._deserialize_value(value) if value else None
                else:
                    values = redis_service.lpop(key, count)
                    logger.debug(f"Redis lpop成功: key={key}, count={count}")
                    return [self._deserialize_value(v) for v in values]
        except Exception as e:
            logger.error(f"Redis lpop失败: key={key}, error={e}")
            self._handle_redis_error("lpop")
        
        # MySQL不太适合模拟lpop，返回None
        logger.warning("lpop操作不支持MySQL降级")
        return None
    
    def lindex(self, key: str, index: int) -> Optional[Any]:
        """
        获取列表指定索引的元素
        
        Args:
            key: 列表名称
            index: 索引
        Returns:
            指定索引的元素
        """
        try:
            if self.health_checker.is_redis_alive:
                redis_service = self._get_redis_service()
                value = redis_service.lindex(key, index)
                logger.debug(f"Redis lindex成功: key={key}, index={index}")
                return self._deserialize_value(value) if value else None
        except Exception as e:
            logger.error(f"Redis lindex失败: key={key}, error={e}")
            self._handle_redis_error("lindex")
        
        # 降级到MySQL
        logger.info(f"降级到MySQL读取lindex: key={key}, index={index}")
        try:
            items = self.lrange(key, 0, -1)
            if 0 <= index < len(items):
                return items[index]
            return None
        except Exception as e:
            logger.error(f"从MySQL读取lindex失败: key={key}, error={e}")
            return None
    
    def llen(self, key: str) -> int:
        """
        获取列表长度
        
        Args:
            key: 列表名称
        Returns:
            列表长度
        """
        try:
            if self.health_checker.is_redis_alive:
                redis_service = self._get_redis_service()
                length = redis_service.llen(key)
                logger.debug(f"Redis llen成功: key={key}")
                return length
        except Exception as e:
            logger.error(f"Redis llen失败: key={key}, error={e}")
            self._handle_redis_error("llen")
        
        # 降级到MySQL
        logger.info(f"降级到MySQL读取llen: key={key}")
        try:
            with self.get_db_session() as session:
                count = session.query(RedisGatewayInfo).filter(
                    RedisGatewayInfo.key == key
                ).count()
                return count
        except Exception as e:
            logger.error(f"从MySQL读取llen失败: key={key}, error={e}")
            return 0
    
    def lrem(self, key: str, count: int, value: Any) -> int:
        """
        从列表中删除元素
        
        Args:
            key: 列表名称
            count: 删除的数量
            value: 要删除的值
        Returns:
            删除的元素数量
        """
        try:
            if self.health_checker.is_redis_alive:
                redis_service = self._get_redis_service()
                serialized_value = self._serialize_value(value)
                removed = redis_service.lrem(key, count, serialized_value)
                logger.debug(f"Redis lrem成功: key={key}, removed={removed}")
                return removed
        except Exception as e:
            logger.error(f"Redis lrem失败: key={key}, error={e}")
            self._handle_redis_error("lrem")
        
        # MySQL的lrem实现复杂，暂不支持
        logger.warning("lrem操作不完全支持MySQL降级")
        return 0
    
    def exists(self, key: str) -> bool:
        """
        检查键是否存在
        
        Args:
            key: 键名
        Returns:
            键是否存在
        """
        try:
            if self.health_checker.is_redis_alive:
                redis_service = self._get_redis_service()
                exists = redis_service.exists(key)
                logger.debug(f"Redis exists成功: key={key}, exists={exists}")
                return bool(exists)
        except Exception as e:
            logger.error(f"Redis exists失败: key={key}, error={e}")
            self._handle_redis_error("exists")
        
        # 降级到MySQL
        logger.info(f"降级到MySQL检查exists: key={key}")
        try:
            with self.get_db_session() as session:
                count = session.query(RedisGatewayInfo).filter(
                    RedisGatewayInfo.key == key
                ).count()
                return count > 0
        except Exception as e:
            logger.error(f"从MySQL检查exists失败: key={key}, error={e}")
            return False
    
    def delete(self, *keys) -> int:
        """
        删除键
        
        Args:
            keys: 要删除的键名
        Returns:
            删除的键数量
        """
        try:
            if self.health_checker.is_redis_alive:
                redis_service = self._get_redis_service()
                deleted = redis_service.delete(*keys)
                logger.debug(f"Redis delete成功: keys={keys}, deleted={deleted}")
                return deleted
        except Exception as e:
            logger.error(f"Redis delete失败: keys={keys}, error={e}")
            self._handle_redis_error("delete")
        
        # 异步从MySQL删除
        for key in keys:
            self._delete_from_mysql(key, '')
        
        return len(keys)
    
    def expire(self, key: str, ttl: int) -> bool:
        """
        设置键的过期时间
        
        Args:
            key: 键名
            ttl: 过期时间（秒）
        Returns:
            是否设置成功
        """
        try:
            if self.health_checker.is_redis_alive:
                redis_service = self._get_redis_service()
                result = redis_service.expire(key, ttl)
                logger.debug(f"Redis expire成功: key={key}, ttl={ttl}")
                return bool(result)
        except Exception as e:
            logger.error(f"Redis expire失败: key={key}, error={e}")
            self._handle_redis_error("expire")
        
        # 更新MySQL中的过期时间
        def _async_update_expire():
            try:
                with self.get_db_session() as session:
                    expire_time = datetime.now() + timedelta(seconds=ttl)
                    session.query(RedisGatewayInfo).filter(
                        RedisGatewayInfo.key == key
                    ).update({
                        'expire_time': expire_time,
                        'last_update_time': datetime.now()
                    })
                    session.commit()
            except Exception as e:
                logger.error(f"更新MySQL过期时间失败: key={key}, error={e}")
        
        threading.Thread(target=_async_update_expire, daemon=True).start()
        return True
    
    # ==================== 分布式锁和信号量 ====================
    
    def distributed_lock(self, lock_key: str, timeout: int = 30):
        """
        获取分布式锁上下文管理器
        
        Args:
            lock_key: 锁的键名
            timeout: 锁超时时间（秒）
        Returns:
            上下文管理器
        
        使用示例:
            with service.distributed_lock("my_lock"):
                # 执行需要锁保护的代码
                pass
        """
        redis_service = self._get_redis_service()
        return RedisContextManager(redis_service, lock_key, timeout)
    
    def acquire_semaphore(self, semaphore_key: str, limit: int, timeout: int = 10) -> bool:
        """
        获取Redis信号量
        
        Args:
            semaphore_key: 信号量的键名
            limit: 信号量限制数量
            timeout: 超时时间（秒）
        Returns:
            是否成功获取信号量
        """
        try:
            if self.health_checker.is_redis_alive:
                redis_service = self._get_redis_service()
                now = time.time()
                
                # 清理过期的信号量
                redis_service.zremrangebyscore(semaphore_key, 0, now - timeout)
                
                # 尝试获取信号量
                current_count = redis_service.zcard(semaphore_key)
                if current_count < limit:
                    identifier = f"{threading.current_thread().ident}_{now}"
                    redis_service.zadd(semaphore_key, {identifier: now})
                    logger.debug(f"成功获取信号量: {semaphore_key}")
                    return True
                else:
                    logger.debug(f"信号量已满: {semaphore_key}")
                    return False
        except Exception as e:
            logger.error(f"获取信号量失败: {semaphore_key}, error={e}")
            self._handle_redis_error("acquire_semaphore")
            return False
    
    def release_semaphore(self, semaphore_key: str, identifier: str):
        """
        释放Redis信号量
        
        Args:
            semaphore_key: 信号量的键名
            identifier: 信号量标识符
        """
        try:
            if self.health_checker.is_redis_alive:
                redis_service = self._get_redis_service()
                redis_service.zrem(semaphore_key, identifier)
                logger.debug(f"释放信号量: {semaphore_key}")
        except Exception as e:
            logger.error(f"释放信号量失败: {semaphore_key}, error={e}")
    
    # ==================== 定时任务调度 ====================
    
    def start_scheduler(self):
        """
        启动定时任务调度器
        每60秒清理一次过期数据
        """
        if self._scheduler_running:
            logger.warning("调度器已在运行中")
            return
        
        self._scheduler_running = True
        
        # 配置定时任务
        schedule.every(60).seconds.do(self.cleanup_all_expired_data)
        
        def _run_scheduler():
            logger.info("定时任务调度器启动")
            while self._scheduler_running:
                schedule.run_pending()
                time.sleep(1)
            logger.info("定时任务调度器停止")
        
        self._scheduler_thread = threading.Thread(
            target=_run_scheduler,
            daemon=True,
            name="RedisScheduler"
        )
        self._scheduler_thread.start()
        logger.info("定时任务调度器已启动，每60秒清理一次过期数据")
    
    def stop_scheduler(self):
        """停止定时任务调度器"""
        if not self._scheduler_running:
            return
        
        self._scheduler_running = False
        schedule.clear()
        
        if self._scheduler_thread and self._scheduler_thread.is_alive():
            self._scheduler_thread.join(timeout=5)
        
        logger.info("定时任务调度器已停止")


# ==================== 使用示例 ====================

def example_usage():
    """
    使用示例
    """
    # 假设这两个函数已在你的代码中定义
    # from your_module import get_redis_cache_service, get_db_session
    
    # 初始化服务
    # service = RedisFallbackService(get_redis_cache_service, get_db_session)
    
    # 启动定时任务调度器
    # service.start_scheduler()
    
    # 使用Redis操作
    # service.set_ex("mykey", "myvalue", ttl=3600)
    # value = service.get("mykey")
    
    # 使用Hash操作
    # service.hset("myhash", "field1", "value1")
    # field_value = service.hget("myhash", "field1")
    
    # 使用分布式锁
    # with service.distributed_lock("my_task_lock", timeout=30):
    #     # 执行需要互斥的任务
    #     print("执行任务...")
    
    pass


if __name__ == "__main__":
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    example_usage()
