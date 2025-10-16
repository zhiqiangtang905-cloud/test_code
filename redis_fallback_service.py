"""
Redis降级服务
实现Redis到MySQL的自动降级和恢复
支持多实例场景下的分布式协调
"""
import json
import logging
import threading
from datetime import datetime, timedelta
from typing import Any, List, Optional, Union
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class RedisFallbackService:
    """
    Redis降级服务类
    
    功能：
    1. 提供与Redis相同的API接口
    2. Redis可用时直接操作Redis，异步写入MySQL
    3. Redis不可用时直接操作MySQL
    4. 自动检测Redis状态并触发健康检查
    """
    
    def __init__(self, get_redis_service_func, get_db_session_func, health_check):
        """
        初始化降级服务
        
        Args:
            get_redis_service_func: 获取Redis服务的函数
            get_db_session_func: 获取数据库会话的函数
            health_check: 健康检查服务实例
        """
        self.get_redis_service = get_redis_service_func
        self.get_db_session = get_db_session_func
        self.health_check = health_check
        
        logger.info("Redis降级服务初始化完成")
    
    def _handle_redis_error(self, operation: str, error: Exception):
        """
        处理Redis错误
        标记Redis为不可用并启动健康拨测
        
        Args:
            operation: 操作名称
            error: 异常对象
        """
        logger.error(f"Redis操作失败 [{operation}]: {error}")
        self.health_check.mark_redis_down()
    
    def _cleanup_expired_before_read(self):
        """
        读取MySQL前清理过期数据
        """
        try:
            from models import RedisGatewayInfo
            
            with self.get_db_session() as session:
                now = datetime.now()
                session.query(RedisGatewayInfo).filter(
                    RedisGatewayInfo.expire_time != None,
                    RedisGatewayInfo.expire_time <= now
                ).delete(synchronize_session=False)
                session.commit()
        except Exception as e:
            logger.error(f"清理过期数据失败: {e}")
    
    def _async_write_mysql(self, key: str, name: str, value: Any, expire_time: Optional[datetime] = None):
        """
        异步写入MySQL
        
        Args:
            key: Redis的key
            name: Redis的name（hash类型使用，普通key为空字符串）
            value: 值
            expire_time: 过期时间
        """
        def _write():
            try:
                from models import RedisGatewayInfo
                
                with self.get_db_session() as session:
                    # 查询是否已存在
                    record = session.query(RedisGatewayInfo).filter(
                        RedisGatewayInfo.key == key,
                        RedisGatewayInfo.name == name
                    ).first()
                    
                    now = datetime.now()
                    
                    if record:
                        # 更新
                        record.value = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
                        record.expire_time = expire_time
                        record.last_update_time = now
                    else:
                        # 插入
                        new_record = RedisGatewayInfo(
                            key=key,
                            name=name,
                            value=value if isinstance(value, str) else json.dumps(value, ensure_ascii=False),
                            expire_time=expire_time,
                            last_update_time=now
                        )
                        session.add(new_record)
                    
                    session.commit()
            except Exception as e:
                logger.error(f"异步写入MySQL失败 key={key}, name={name}: {e}")
        
        # 在新线程中执行写入操作
        thread = threading.Thread(target=_write, daemon=True)
        thread.start()
    
    def _write_mysql_sync(self, key: str, name: str, value: Any, expire_time: Optional[datetime] = None):
        """
        同步写入MySQL
        
        Args:
            key: Redis的key
            name: Redis的name（hash类型使用，普通key为空字符串）
            value: 值
            expire_time: 过期时间
        """
        try:
            from models import RedisGatewayInfo
            
            with self.get_db_session() as session:
                record = session.query(RedisGatewayInfo).filter(
                    RedisGatewayInfo.key == key,
                    RedisGatewayInfo.name == name
                ).first()
                
                now = datetime.now()
                
                if record:
                    record.value = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
                    record.expire_time = expire_time
                    record.last_update_time = now
                else:
                    new_record = RedisGatewayInfo(
                        key=key,
                        name=name,
                        value=value if isinstance(value, str) else json.dumps(value, ensure_ascii=False),
                        expire_time=expire_time,
                        last_update_time=now
                    )
                    session.add(new_record)
                
                session.commit()
        except Exception as e:
            logger.error(f"同步写入MySQL失败 key={key}, name={name}: {e}")
            raise
    
    def _read_mysql(self, key: str, name: str = '') -> Optional[str]:
        """
        从MySQL读取数据
        
        Args:
            key: Redis的key
            name: Redis的name
            
        Returns:
            Optional[str]: 值（字符串格式）
        """
        try:
            # 先清理过期数据
            self._cleanup_expired_before_read()
            
            from models import RedisGatewayInfo
            
            with self.get_db_session() as session:
                record = session.query(RedisGatewayInfo).filter(
                    RedisGatewayInfo.key == key,
                    RedisGatewayInfo.name == name
                ).first()
                
                if record:
                    # 检查是否过期
                    if record.expire_time and record.expire_time <= datetime.now():
                        return None
                    return record.value
                
                return None
        except Exception as e:
            logger.error(f"从MySQL读取失败 key={key}, name={name}: {e}")
            return None
    
    # ==================== 基础操作 ====================
    
    def set(self, key: str, value: Any) -> bool:
        """
        设置key-value
        
        Args:
            key: 键
            value: 值（会自动json.dumps序列化）
            
        Returns:
            bool: 是否成功
        """
        value_str = json.dumps(value, ensure_ascii=False) if not isinstance(value, str) else value
        
        if self.health_check.redis_alive:
            try:
                redis_service = self.get_redis_service()
                redis_service.set(key, value_str)
                # 异步写入MySQL
                self._async_write_mysql(key, '', value_str, None)
                return True
            except Exception as e:
                self._handle_redis_error('set', e)
                # 降级到MySQL
                self._write_mysql_sync(key, '', value_str, None)
                return True
        else:
            # 直接写MySQL
            self._write_mysql_sync(key, '', value_str, None)
            return True
    
    def get(self, key: str) -> Optional[Any]:
        """
        获取key的值
        
        Args:
            key: 键
            
        Returns:
            Optional[Any]: 值（会自动json.loads反序列化）
        """
        if self.health_check.redis_alive:
            try:
                redis_service = self.get_redis_service()
                value = redis_service.get(key)
                if value is None:
                    return None
                # 尝试反序列化
                try:
                    return json.loads(value)
                except:
                    return value
            except Exception as e:
                self._handle_redis_error('get', e)
                # 降级到MySQL
                value = self._read_mysql(key, '')
                if value is None:
                    return None
                try:
                    return json.loads(value)
                except:
                    return value
        else:
            # 直接读MySQL
            value = self._read_mysql(key, '')
            if value is None:
                return None
            try:
                return json.loads(value)
            except:
                return value
    
    def set_ex(self, key: str, value: Any, ttl: int) -> bool:
        """
        设置带过期时间的key-value
        
        Args:
            key: 键
            value: 值
            ttl: 过期时间（秒）
            
        Returns:
            bool: 是否成功
        """
        value_str = json.dumps(value, ensure_ascii=False) if not isinstance(value, str) else value
        expire_time = datetime.now() + timedelta(seconds=ttl)
        
        if self.health_check.redis_alive:
            try:
                redis_service = self.get_redis_service()
                redis_service.setex(key, ttl, value_str)
                # 异步写入MySQL
                self._async_write_mysql(key, '', value_str, expire_time)
                return True
            except Exception as e:
                self._handle_redis_error('set_ex', e)
                # 降级到MySQL
                self._write_mysql_sync(key, '', value_str, expire_time)
                return True
        else:
            # 直接写MySQL
            self._write_mysql_sync(key, '', value_str, expire_time)
            return True
    
    def delete(self, key: str) -> bool:
        """
        删除key
        
        Args:
            key: 键
            
        Returns:
            bool: 是否成功
        """
        if self.health_check.redis_alive:
            try:
                redis_service = self.get_redis_service()
                redis_service.delete(key)
                # 异步删除MySQL中的数据
                def _delete():
                    try:
                        from models import RedisGatewayInfo
                        with self.get_db_session() as session:
                            session.query(RedisGatewayInfo).filter(
                                RedisGatewayInfo.key == key
                            ).delete(synchronize_session=False)
                            session.commit()
                    except Exception as e:
                        logger.error(f"异步删除MySQL数据失败 key={key}: {e}")
                
                threading.Thread(target=_delete, daemon=True).start()
                return True
            except Exception as e:
                self._handle_redis_error('delete', e)
                # 降级到MySQL
                from models import RedisGatewayInfo
                with self.get_db_session() as session:
                    session.query(RedisGatewayInfo).filter(
                        RedisGatewayInfo.key == key
                    ).delete(synchronize_session=False)
                    session.commit()
                return True
        else:
            # 直接删除MySQL
            from models import RedisGatewayInfo
            with self.get_db_session() as session:
                session.query(RedisGatewayInfo).filter(
                    RedisGatewayInfo.key == key
                ).delete(synchronize_session=False)
                session.commit()
            return True
    
    def exists(self, key: str) -> bool:
        """
        检查key是否存在
        
        Args:
            key: 键
            
        Returns:
            bool: 是否存在
        """
        if self.health_check.redis_alive:
            try:
                redis_service = self.get_redis_service()
                return redis_service.exists(key) > 0
            except Exception as e:
                self._handle_redis_error('exists', e)
                # 降级到MySQL
                value = self._read_mysql(key, '')
                return value is not None
        else:
            # 直接查MySQL
            value = self._read_mysql(key, '')
            return value is not None
    
    def expire(self, key: str, ttl: int) -> bool:
        """
        设置key的过期时间
        
        Args:
            key: 键
            ttl: 过期时间（秒）
            
        Returns:
            bool: 是否成功
        """
        expire_time = datetime.now() + timedelta(seconds=ttl)
        
        if self.health_check.redis_alive:
            try:
                redis_service = self.get_redis_service()
                redis_service.expire(key, ttl)
                # 异步更新MySQL中的过期时间
                def _update():
                    try:
                        from models import RedisGatewayInfo
                        with self.get_db_session() as session:
                            session.query(RedisGatewayInfo).filter(
                                RedisGatewayInfo.key == key
                            ).update({'expire_time': expire_time}, synchronize_session=False)
                            session.commit()
                    except Exception as e:
                        logger.error(f"异步更新过期时间失败 key={key}: {e}")
                
                threading.Thread(target=_update, daemon=True).start()
                return True
            except Exception as e:
                self._handle_redis_error('expire', e)
                # 降级到MySQL
                from models import RedisGatewayInfo
                with self.get_db_session() as session:
                    session.query(RedisGatewayInfo).filter(
                        RedisGatewayInfo.key == key
                    ).update({'expire_time': expire_time}, synchronize_session=False)
                    session.commit()
                return True
        else:
            # 直接更新MySQL
            from models import RedisGatewayInfo
            with self.get_db_session() as session:
                session.query(RedisGatewayInfo).filter(
                    RedisGatewayInfo.key == key
                ).update({'expire_time': expire_time}, synchronize_session=False)
                session.commit()
            return True
    
    # ==================== Hash操作 ====================
    
    def hset(self, name: str, key: str, value: Any) -> bool:
        """
        设置hash字段
        
        Args:
            name: hash名称
            key: 字段名
            value: 值
            
        Returns:
            bool: 是否成功
        """
        value_str = json.dumps(value, ensure_ascii=False) if not isinstance(value, str) else value
        
        if self.health_check.redis_alive:
            try:
                redis_service = self.get_redis_service()
                redis_service.hset(name, key, value_str)
                # 异步写入MySQL
                self._async_write_mysql(key, name, value_str, None)
                return True
            except Exception as e:
                self._handle_redis_error('hset', e)
                # 降级到MySQL
                self._write_mysql_sync(key, name, value_str, None)
                return True
        else:
            # 直接写MySQL
            self._write_mysql_sync(key, name, value_str, None)
            return True
    
    def hget(self, name: str, key: str) -> Optional[Any]:
        """
        获取hash字段值
        
        Args:
            name: hash名称
            key: 字段名
            
        Returns:
            Optional[Any]: 值
        """
        if self.health_check.redis_alive:
            try:
                redis_service = self.get_redis_service()
                value = redis_service.hget(name, key)
                if value is None:
                    return None
                try:
                    return json.loads(value)
                except:
                    return value
            except Exception as e:
                self._handle_redis_error('hget', e)
                # 降级到MySQL
                value = self._read_mysql(key, name)
                if value is None:
                    return None
                try:
                    return json.loads(value)
                except:
                    return value
        else:
            # 直接读MySQL
            value = self._read_mysql(key, name)
            if value is None:
                return None
            try:
                return json.loads(value)
            except:
                return value
    
    def hdel(self, name: str, key: str) -> bool:
        """
        删除hash字段
        
        Args:
            name: hash名称
            key: 字段名
            
        Returns:
            bool: 是否成功
        """
        if self.health_check.redis_alive:
            try:
                redis_service = self.get_redis_service()
                redis_service.hdel(name, key)
                # 异步删除MySQL
                def _delete():
                    try:
                        from models import RedisGatewayInfo
                        with self.get_db_session() as session:
                            session.query(RedisGatewayInfo).filter(
                                RedisGatewayInfo.key == key,
                                RedisGatewayInfo.name == name
                            ).delete(synchronize_session=False)
                            session.commit()
                    except Exception as e:
                        logger.error(f"异步删除hash字段失败 name={name}, key={key}: {e}")
                
                threading.Thread(target=_delete, daemon=True).start()
                return True
            except Exception as e:
                self._handle_redis_error('hdel', e)
                # 降级到MySQL
                from models import RedisGatewayInfo
                with self.get_db_session() as session:
                    session.query(RedisGatewayInfo).filter(
                        RedisGatewayInfo.key == key,
                        RedisGatewayInfo.name == name
                    ).delete(synchronize_session=False)
                    session.commit()
                return True
        else:
            # 直接删除MySQL
            from models import RedisGatewayInfo
            with self.get_db_session() as session:
                session.query(RedisGatewayInfo).filter(
                    RedisGatewayInfo.key == key,
                    RedisGatewayInfo.name == name
                ).delete(synchronize_session=False)
                session.commit()
            return True
    
    def hgetall(self, name: str) -> dict:
        """
        获取hash的所有字段
        
        Args:
            name: hash名称
            
        Returns:
            dict: 所有字段的键值对
        """
        if self.health_check.redis_alive:
            try:
                redis_service = self.get_redis_service()
                data = redis_service.hgetall(name)
                # 尝试反序列化所有值
                result = {}
                for k, v in data.items():
                    try:
                        result[k] = json.loads(v)
                    except:
                        result[k] = v
                return result
            except Exception as e:
                self._handle_redis_error('hgetall', e)
                # 降级到MySQL
                return self._hgetall_from_mysql(name)
        else:
            # 直接读MySQL
            return self._hgetall_from_mysql(name)
    
    def _hgetall_from_mysql(self, name: str) -> dict:
        """从MySQL获取hash的所有字段"""
        try:
            self._cleanup_expired_before_read()
            from models import RedisGatewayInfo
            
            with self.get_db_session() as session:
                records = session.query(RedisGatewayInfo).filter(
                    RedisGatewayInfo.name == name
                ).all()
                
                result = {}
                now = datetime.now()
                for record in records:
                    # 检查是否过期
                    if record.expire_time and record.expire_time <= now:
                        continue
                    try:
                        result[record.key] = json.loads(record.value)
                    except:
                        result[record.key] = record.value
                
                return result
        except Exception as e:
            logger.error(f"从MySQL获取hgetall失败 name={name}: {e}")
            return {}
    
    # ==================== List操作 ====================
    
    def lpush(self, name: str, *values) -> int:
        """
        从列表左侧插入元素
        
        Args:
            name: 列表名称
            *values: 要插入的值
            
        Returns:
            int: 列表长度
        """
        if self.health_check.redis_alive:
            try:
                redis_service = self.get_redis_service()
                # 序列化值
                serialized_values = [json.dumps(v, ensure_ascii=False) if not isinstance(v, str) else v for v in values]
                result = redis_service.lpush(name, *serialized_values)
                # 异步写入MySQL（将整个列表存储）
                self._async_sync_list_to_mysql(name)
                return result
            except Exception as e:
                self._handle_redis_error('lpush', e)
                # 降级到MySQL
                return self._lpush_to_mysql(name, *values)
        else:
            # 直接写MySQL
            return self._lpush_to_mysql(name, *values)
    
    def rpush(self, name: str, *values) -> int:
        """
        从列表右侧插入元素
        
        Args:
            name: 列表名称
            *values: 要插入的值
            
        Returns:
            int: 列表长度
        """
        if self.health_check.redis_alive:
            try:
                redis_service = self.get_redis_service()
                serialized_values = [json.dumps(v, ensure_ascii=False) if not isinstance(v, str) else v for v in values]
                result = redis_service.rpush(name, *serialized_values)
                # 异步写入MySQL
                self._async_sync_list_to_mysql(name)
                return result
            except Exception as e:
                self._handle_redis_error('rpush', e)
                # 降级到MySQL
                return self._rpush_to_mysql(name, *values)
        else:
            # 直接写MySQL
            return self._rpush_to_mysql(name, *values)
    
    def lpop(self, key: str, count: Optional[int] = None) -> Union[Optional[Any], List[Any]]:
        """
        从列表左侧弹出元素
        
        Args:
            key: 列表名称
            count: 弹出数量
            
        Returns:
            单个元素或元素列表
        """
        if self.health_check.redis_alive:
            try:
                redis_service = self.get_redis_service()
                if count is None:
                    value = redis_service.lpop(key)
                    if value is None:
                        return None
                    try:
                        return json.loads(value)
                    except:
                        return value
                else:
                    values = redis_service.lpop(key, count)
                    return [json.loads(v) if isinstance(v, str) else v for v in values]
            except Exception as e:
                self._handle_redis_error('lpop', e)
                # 降级到MySQL
                return self._lpop_from_mysql(key, count)
        else:
            # 直接从MySQL操作
            return self._lpop_from_mysql(key, count)
    
    def lrange(self, name: str, start: int = 0, end: int = -1) -> List[Any]:
        """
        获取列表范围内的元素
        
        Args:
            name: 列表名称
            start: 起始索引
            end: 结束索引
            
        Returns:
            List[Any]: 元素列表
        """
        if self.health_check.redis_alive:
            try:
                redis_service = self.get_redis_service()
                values = redis_service.lrange(name, start, end)
                return [json.loads(v) if isinstance(v, str) else v for v in values]
            except Exception as e:
                self._handle_redis_error('lrange', e)
                # 降级到MySQL
                return self._lrange_from_mysql(name, start, end)
        else:
            # 直接从MySQL读取
            return self._lrange_from_mysql(name, start, end)
    
    def lindex(self, key: str, index: int) -> Optional[Any]:
        """
        获取列表指定索引的元素
        
        Args:
            key: 列表名称
            index: 索引
            
        Returns:
            Optional[Any]: 元素值
        """
        if self.health_check.redis_alive:
            try:
                redis_service = self.get_redis_service()
                value = redis_service.lindex(key, index)
                if value is None:
                    return None
                try:
                    return json.loads(value)
                except:
                    return value
            except Exception as e:
                self._handle_redis_error('lindex', e)
                # 降级到MySQL
                return self._lindex_from_mysql(key, index)
        else:
            # 直接从MySQL读取
            return self._lindex_from_mysql(key, index)
    
    def llen(self, key: str) -> int:
        """
        获取列表长度
        
        Args:
            key: 列表名称
            
        Returns:
            int: 列表长度
        """
        if self.health_check.redis_alive:
            try:
                redis_service = self.get_redis_service()
                return redis_service.llen(key)
            except Exception as e:
                self._handle_redis_error('llen', e)
                # 降级到MySQL
                return self._llen_from_mysql(key)
        else:
            # 直接从MySQL查询
            return self._llen_from_mysql(key)
    
    def lrem(self, key: str, count: int, value: Any) -> int:
        """
        从列表中删除元素
        
        Args:
            key: 列表名称
            count: 删除数量
            value: 要删除的值
            
        Returns:
            int: 删除的数量
        """
        value_str = json.dumps(value, ensure_ascii=False) if not isinstance(value, str) else value
        
        if self.health_check.redis_alive:
            try:
                redis_service = self.get_redis_service()
                result = redis_service.lrem(key, count, value_str)
                # 异步同步到MySQL
                self._async_sync_list_to_mysql(key)
                return result
            except Exception as e:
                self._handle_redis_error('lrem', e)
                # 降级到MySQL
                return self._lrem_from_mysql(key, count, value_str)
        else:
            # 直接从MySQL删除
            return self._lrem_from_mysql(key, count, value_str)
    
    # ==================== List的MySQL操作辅助方法 ====================
    
    def _async_sync_list_to_mysql(self, list_key: str):
        """异步同步列表到MySQL"""
        def _sync():
            try:
                redis_service = self.get_redis_service()
                values = redis_service.lrange(list_key, 0, -1)
                # 将列表作为JSON数组存储
                list_json = json.dumps(values, ensure_ascii=False)
                self._write_mysql_sync(list_key, '__list__', list_json, None)
            except Exception as e:
                logger.error(f"同步列表到MySQL失败 key={list_key}: {e}")
        
        threading.Thread(target=_sync, daemon=True).start()
    
    def _get_list_from_mysql(self, list_key: str) -> List[str]:
        """从MySQL获取列表"""
        value = self._read_mysql(list_key, '__list__')
        if value is None:
            return []
        try:
            return json.loads(value)
        except:
            return []
    
    def _save_list_to_mysql(self, list_key: str, values: List[str]):
        """保存列表到MySQL"""
        list_json = json.dumps(values, ensure_ascii=False)
        self._write_mysql_sync(list_key, '__list__', list_json, None)
    
    def _lpush_to_mysql(self, name: str, *values) -> int:
        """MySQL的lpush实现"""
        current_list = self._get_list_from_mysql(name)
        # 从左侧插入
        for value in reversed(values):
            value_str = json.dumps(value, ensure_ascii=False) if not isinstance(value, str) else value
            current_list.insert(0, value_str)
        self._save_list_to_mysql(name, current_list)
        return len(current_list)
    
    def _rpush_to_mysql(self, name: str, *values) -> int:
        """MySQL的rpush实现"""
        current_list = self._get_list_from_mysql(name)
        # 从右侧插入
        for value in values:
            value_str = json.dumps(value, ensure_ascii=False) if not isinstance(value, str) else value
            current_list.append(value_str)
        self._save_list_to_mysql(name, current_list)
        return len(current_list)
    
    def _lpop_from_mysql(self, key: str, count: Optional[int] = None):
        """MySQL的lpop实现"""
        current_list = self._get_list_from_mysql(key)
        if not current_list:
            return None if count is None else []
        
        if count is None:
            value = current_list.pop(0)
            self._save_list_to_mysql(key, current_list)
            try:
                return json.loads(value)
            except:
                return value
        else:
            result = []
            for _ in range(min(count, len(current_list))):
                value = current_list.pop(0)
                try:
                    result.append(json.loads(value))
                except:
                    result.append(value)
            self._save_list_to_mysql(key, current_list)
            return result
    
    def _lrange_from_mysql(self, name: str, start: int, end: int) -> List[Any]:
        """MySQL的lrange实现"""
        current_list = self._get_list_from_mysql(name)
        if end == -1:
            end = len(current_list)
        else:
            end = end + 1
        
        result = []
        for value in current_list[start:end]:
            try:
                result.append(json.loads(value))
            except:
                result.append(value)
        return result
    
    def _lindex_from_mysql(self, key: str, index: int) -> Optional[Any]:
        """MySQL的lindex实现"""
        current_list = self._get_list_from_mysql(key)
        if not current_list or index >= len(current_list):
            return None
        
        value = current_list[index]
        try:
            return json.loads(value)
        except:
            return value
    
    def _llen_from_mysql(self, key: str) -> int:
        """MySQL的llen实现"""
        current_list = self._get_list_from_mysql(key)
        return len(current_list)
    
    def _lrem_from_mysql(self, key: str, count: int, value: str) -> int:
        """MySQL的lrem实现"""
        current_list = self._get_list_from_mysql(key)
        removed = 0
        
        if count > 0:
            # 从头到尾删除
            i = 0
            while i < len(current_list) and removed < count:
                if current_list[i] == value:
                    current_list.pop(i)
                    removed += 1
                else:
                    i += 1
        elif count < 0:
            # 从尾到头删除
            i = len(current_list) - 1
            while i >= 0 and removed < abs(count):
                if current_list[i] == value:
                    current_list.pop(i)
                    removed += 1
                i -= 1
        else:
            # 删除所有匹配的元素
            current_list = [v for v in current_list if v != value]
            removed = len(current_list)
        
        self._save_list_to_mysql(key, current_list)
        return removed
    
    # ==================== 分布式锁 ====================
    
    @contextmanager
    def lock(self, lock_key: str, ttl: int = 30):
        """
        Redis分布式锁上下文管理器
        
        Args:
            lock_key: 锁的key
            ttl: 锁的过期时间（秒）
            
        Yields:
            bool: 是否成功获取锁
            
        Example:
            with service.lock("my_lock", ttl=30) as acquired:
                if acquired:
                    # 执行需要加锁的操作
                    pass
        """
        acquired = False
        
        try:
            if self.health_check.redis_alive:
                try:
                    redis_service = self.get_redis_service()
                    # 使用SET NX EX实现分布式锁
                    acquired = redis_service.set(lock_key, "locked", nx=True, ex=ttl)
                except Exception as e:
                    self._handle_redis_error('lock', e)
                    # 降级到MySQL锁
                    acquired = self.health_check._acquire_mysql_lock(lock_key)
            else:
                # 使用MySQL锁
                acquired = self.health_check._acquire_mysql_lock(lock_key)
            
            yield acquired
            
        finally:
            if acquired:
                # 释放锁
                try:
                    if self.health_check.redis_alive:
                        redis_service = self.get_redis_service()
                        redis_service.delete(lock_key)
                    else:
                        self.health_check._release_mysql_lock(lock_key)
                except Exception as e:
                    logger.error(f"释放锁失败 {lock_key}: {e}")
    
    # ==================== 信号量 ====================
    
    def acquire_semaphore(self, semaphore_key: str, limit: int, ttl: int = 30) -> bool:
        """
        获取信号量
        
        Args:
            semaphore_key: 信号量key
            limit: 最大并发数
            ttl: 超时时间（秒）
            
        Returns:
            bool: 是否成功获取
        """
        if self.health_check.redis_alive:
            try:
                redis_service = self.get_redis_service()
                import time
                import uuid
                
                # 生成唯一标识
                identifier = str(uuid.uuid4())
                now = time.time()
                
                # 清理过期的信号量
                redis_service.zremrangebyscore(semaphore_key, '-inf', now - ttl)
                
                # 尝试获取信号量
                redis_service.zadd(semaphore_key, {identifier: now})
                
                # 检查是否在允许范围内
                rank = redis_service.zrank(semaphore_key, identifier)
                
                if rank < limit:
                    return True
                else:
                    # 获取失败，删除标识
                    redis_service.zrem(semaphore_key, identifier)
                    return False
                    
            except Exception as e:
                self._handle_redis_error('acquire_semaphore', e)
                # 降级到MySQL实现（简化版）
                return self._acquire_semaphore_mysql(semaphore_key, limit, ttl)
        else:
            # 使用MySQL实现信号量
            return self._acquire_semaphore_mysql(semaphore_key, limit, ttl)
    
    def release_semaphore(self, semaphore_key: str, identifier: str):
        """
        释放信号量
        
        Args:
            semaphore_key: 信号量key
            identifier: 标识符
        """
        if self.health_check.redis_alive:
            try:
                redis_service = self.get_redis_service()
                redis_service.zrem(semaphore_key, identifier)
            except Exception as e:
                self._handle_redis_error('release_semaphore', e)
        # MySQL版本的信号量会自动过期，不需要显式释放
    
    def _acquire_semaphore_mysql(self, semaphore_key: str, limit: int, ttl: int) -> bool:
        """MySQL实现的信号量获取"""
        try:
            from models import RedisGatewayInfo
            import uuid
            
            with self.get_db_session() as session:
                # 清理过期的信号量
                now = datetime.now()
                session.query(RedisGatewayInfo).filter(
                    RedisGatewayInfo.key == semaphore_key,
                    RedisGatewayInfo.name == '__semaphore__',
                    RedisGatewayInfo.expire_time <= now
                ).delete(synchronize_session=False)
                
                # 检查当前持有信号量的数量
                count = session.query(RedisGatewayInfo).filter(
                    RedisGatewayInfo.key == semaphore_key,
                    RedisGatewayInfo.name == '__semaphore__'
                ).count()
                
                if count < limit:
                    # 可以获取信号量
                    identifier = str(uuid.uuid4())
                    expire_time = now + timedelta(seconds=ttl)
                    
                    new_record = RedisGatewayInfo(
                        key=semaphore_key,
                        name='__semaphore__',
                        value=identifier,
                        expire_time=expire_time,
                        last_update_time=now
                    )
                    session.add(new_record)
                    session.commit()
                    return True
                else:
                    return False
                    
        except Exception as e:
            logger.error(f"MySQL信号量获取失败: {e}")
            return False


# 全局服务实例
_fallback_service_instance: Optional[RedisFallbackService] = None


def get_redis_fallback_service(get_redis_service_func, get_db_session_func, health_check) -> RedisFallbackService:
    """
    获取Redis降级服务单例
    
    Args:
        get_redis_service_func: 获取Redis服务的函数
        get_db_session_func: 获取数据库会话的函数
        health_check: 健康检查服务实例
        
    Returns:
        RedisFallbackService: 降级服务实例
    """
    global _fallback_service_instance
    if _fallback_service_instance is None:
        _fallback_service_instance = RedisFallbackService(
            get_redis_service_func,
            get_db_session_func,
            health_check
        )
    return _fallback_service_instance
