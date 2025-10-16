"""
Redis降级MySQL单元测试
"""

import unittest
import time
from datetime import datetime, timedelta
from redis_mysql_fallback import RedisGateway, RedisGatewayInfo
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import json


class TestRedisGateway(unittest.TestCase):
    """Redis网关测试类"""
    
    @classmethod
    def setUpClass(cls):
        """测试类初始化"""
        cls.redis_config = {
            'host': 'localhost',
            'port': 6379,
            'db': 15,  # 使用独立的测试数据库
            'password': None,
            'decode_responses': False,
            'socket_connect_timeout': 5,
            'socket_timeout': 5,
        }
        
        cls.mysql_config = {
            'connection_string': 'mysql+pymysql://root:password@localhost:3306/redis_gateway_test?charset=utf8mb4'
        }
        
        # 初始化网关
        cls.gateway = RedisGateway(cls.redis_config, cls.mysql_config)
    
    @classmethod
    def tearDownClass(cls):
        """测试类清理"""
        cls.gateway.close()
    
    def setUp(self):
        """每个测试方法前的准备"""
        # 清空测试数据
        self._clear_test_data()
    
    def tearDown(self):
        """每个测试方法后的清理"""
        self._clear_test_data()
    
    def _clear_test_data(self):
        """清空测试数据"""
        # 清空Redis测试库
        try:
            self.gateway.redis_client.flushdb()
        except:
            pass
        
        # 清空MySQL测试表
        SessionLocal = sessionmaker(bind=self.gateway.mysql_engine)
        session = SessionLocal()
        try:
            session.query(RedisGatewayInfo).delete()
            session.commit()
        except:
            session.rollback()
        finally:
            session.close()
    
    # ==================== String操作测试 ====================
    
    def test_set_and_get(self):
        """测试set和get操作"""
        self.gateway.set('test_key', 'test_value')
        time.sleep(0.1)  # 等待异步写入
        
        # 从Redis读取
        value = self.gateway.get('test_key')
        self.assertEqual(value, 'test_value')
        
        # 模拟Redis故障，从MySQL读取
        self.gateway.health_checker.redis_alive = False
        value = self.gateway.get('test_key')
        self.assertEqual(value, 'test_value')
    
    def test_set_ex(self):
        """测试带过期时间的set操作"""
        self.gateway.set_ex('temp_key', 'temp_value', ttl=2)
        
        # 立即读取应该成功
        value = self.gateway.get('temp_key')
        self.assertEqual(value, 'temp_value')
        
        # 等待过期
        time.sleep(3)
        
        # 从MySQL读取应该返回None（已过期）
        self.gateway.health_checker.redis_alive = False
        value = self.gateway.get('temp_key')
        self.assertIsNone(value)
    
    def test_delete(self):
        """测试delete操作"""
        self.gateway.set('delete_key', 'delete_value')
        time.sleep(0.1)
        
        # 删除键
        count = self.gateway.delete('delete_key')
        self.assertGreater(count, 0)
        
        # 验证已删除
        value = self.gateway.get('delete_key')
        self.assertIsNone(value)
    
    def test_exists(self):
        """测试exists操作"""
        self.gateway.set('exists_key', 'value')
        time.sleep(0.1)
        
        # 键存在
        self.assertTrue(self.gateway.exists('exists_key'))
        
        # 键不存在
        self.assertFalse(self.gateway.exists('not_exists_key'))
    
    def test_expire(self):
        """测试expire操作"""
        self.gateway.set('expire_key', 'value')
        time.sleep(0.1)
        
        # 设置过期时间
        success = self.gateway.expire('expire_key', 1)
        self.assertTrue(success)
        
        # 等待过期
        time.sleep(2)
        
        # 从MySQL读取应该返回None
        self.gateway.health_checker.redis_alive = False
        value = self.gateway.get('expire_key')
        self.assertIsNone(value)
    
    # ==================== Hash操作测试 ====================
    
    def test_hset_and_hget(self):
        """测试hset和hget操作"""
        self.gateway.hset('user:1', 'name', 'Alice')
        time.sleep(0.1)
        
        # 从Redis读取
        name = self.gateway.hget('user:1', 'name')
        self.assertEqual(name, 'Alice')
        
        # 从MySQL读取
        self.gateway.health_checker.redis_alive = False
        name = self.gateway.hget('user:1', 'name')
        self.assertEqual(name, 'Alice')
    
    def test_hdel(self):
        """测试hdel操作"""
        self.gateway.hset('user:2', 'name', 'Bob')
        self.gateway.hset('user:2', 'age', '25')
        time.sleep(0.1)
        
        # 删除字段
        count = self.gateway.hdel('user:2', 'age')
        self.assertGreater(count, 0)
        
        # 验证已删除
        age = self.gateway.hget('user:2', 'age')
        self.assertIsNone(age)
        
        # name字段应该还在
        name = self.gateway.hget('user:2', 'name')
        self.assertEqual(name, 'Bob')
    
    def test_hgetall(self):
        """测试hgetall操作"""
        self.gateway.hset('user:3', 'name', 'Charlie')
        self.gateway.hset('user:3', 'age', '30')
        self.gateway.hset('user:3', 'city', 'Shanghai')
        time.sleep(0.1)
        
        # 从Redis读取
        user_info = self.gateway.hgetall('user:3')
        self.assertEqual(len(user_info), 3)
        self.assertEqual(user_info['name'], 'Charlie')
        
        # 从MySQL读取
        self.gateway.health_checker.redis_alive = False
        user_info = self.gateway.hgetall('user:3')
        self.assertEqual(len(user_info), 3)
        self.assertEqual(user_info['name'], 'Charlie')
    
    # ==================== List操作测试 ====================
    
    def test_rpush_and_lrange(self):
        """测试rpush和lrange操作"""
        self.gateway.rpush('list:1', 'a', 'b', 'c')
        time.sleep(0.1)
        
        # 从Redis读取
        values = self.gateway.lrange('list:1', 0, -1)
        self.assertEqual(values, ['a', 'b', 'c'])
        
        # 从MySQL读取
        self.gateway.health_checker.redis_alive = False
        values = self.gateway.lrange('list:1', 0, -1)
        self.assertEqual(len(values), 3)
    
    def test_lpush(self):
        """测试lpush操作"""
        self.gateway.rpush('list:2', 'a', 'b')
        time.sleep(0.1)
        
        self.gateway.lpush('list:2', 'z')
        time.sleep(0.1)
        
        # 从MySQL读取，z应该在最前面
        self.gateway.health_checker.redis_alive = False
        values = self.gateway.lrange('list:2', 0, -1)
        self.assertEqual(len(values), 3)
    
    def test_lpop(self):
        """测试lpop操作"""
        self.gateway.rpush('list:3', 'a', 'b', 'c')
        time.sleep(0.1)
        
        # 弹出一个元素
        value = self.gateway.lpop('list:3')
        self.assertIsNotNone(value)
        
        # 剩余2个元素
        length = self.gateway.llen('list:3')
        self.assertEqual(length, 2)
    
    def test_lindex(self):
        """测试lindex操作"""
        self.gateway.rpush('list:4', 'a', 'b', 'c')
        time.sleep(0.1)
        
        # 获取索引1的元素
        value = self.gateway.lindex('list:4', 1)
        self.assertEqual(value, 'b')
        
        # 从MySQL读取
        self.gateway.health_checker.redis_alive = False
        value = self.gateway.lindex('list:4', 1)
        self.assertEqual(value, 'b')
    
    def test_llen(self):
        """测试llen操作"""
        self.gateway.rpush('list:5', 'a', 'b', 'c', 'd')
        time.sleep(0.1)
        
        # 获取长度
        length = self.gateway.llen('list:5')
        self.assertEqual(length, 4)
        
        # 从MySQL读取
        self.gateway.health_checker.redis_alive = False
        length = self.gateway.llen('list:5')
        self.assertEqual(length, 4)
    
    def test_lrem(self):
        """测试lrem操作"""
        self.gateway.rpush('list:6', 'a', 'b', 'a', 'c', 'a')
        time.sleep(0.1)
        
        # 删除1个'a'
        count = self.gateway.lrem('list:6', 1, 'a')
        self.assertEqual(count, 1)
        
        # 剩余4个元素
        length = self.gateway.llen('list:6')
        self.assertEqual(length, 4)
    
    # ==================== 分布式锁测试 ====================
    
    def test_distributed_lock(self):
        """测试分布式锁"""
        # 第一个实例获取锁
        with self.gateway.distributed_lock('test_lock', timeout=5) as acquired1:
            self.assertTrue(acquired1)
            
            # 第二个实例尝试获取同一个锁（应该失败）
            with self.gateway.distributed_lock('test_lock', timeout=5) as acquired2:
                self.assertFalse(acquired2)
        
        # 锁释放后应该可以再次获取
        with self.gateway.distributed_lock('test_lock', timeout=5) as acquired3:
            self.assertTrue(acquired3)
    
    # ==================== 健康检查测试 ====================
    
    def test_health_check(self):
        """测试健康检查"""
        # 初始状态应该是健康的
        self.assertTrue(self.gateway.health_checker.redis_alive)
        
        # 手动标记为不健康
        self.gateway.health_checker.mark_redis_down()
        self.assertFalse(self.gateway.health_checker.redis_alive)
        self.assertTrue(self.gateway.health_checker.is_checking)
        
        # 等待健康检查恢复（如果Redis正常的话）
        time.sleep(12)  # 等待一个检查周期
        
        # 如果Redis正常，应该已经恢复
        if self.gateway.health_checker.check_redis_health():
            self.assertTrue(self.gateway.health_checker.redis_alive)
            self.assertFalse(self.gateway.health_checker.is_checking)
    
    # ==================== 过期数据清理测试 ====================
    
    def test_cleanup_expired_data(self):
        """测试过期数据清理"""
        # 插入一些过期数据
        SessionLocal = sessionmaker(bind=self.gateway.mysql_engine)
        session = SessionLocal()
        
        try:
            # 插入已过期的数据
            expired_record = RedisGatewayInfo(
                key='expired_key',
                name='_string',
                value='expired_value',
                expire_time=datetime.now() - timedelta(seconds=10),
                last_update_time=datetime.now()
            )
            session.add(expired_record)
            
            # 插入未过期的数据
            valid_record = RedisGatewayInfo(
                key='valid_key',
                name='_string',
                value='valid_value',
                expire_time=datetime.now() + timedelta(seconds=300),
                last_update_time=datetime.now()
            )
            session.add(valid_record)
            session.commit()
            
            # 执行清理
            self.gateway.cleanup_expired_data()
            
            # 验证过期数据已删除
            expired = session.query(RedisGatewayInfo).filter(
                RedisGatewayInfo.key == 'expired_key'
            ).first()
            self.assertIsNone(expired)
            
            # 验证有效数据仍存在
            valid = session.query(RedisGatewayInfo).filter(
                RedisGatewayInfo.key == 'valid_key'
            ).first()
            self.assertIsNotNone(valid)
            
        finally:
            session.close()
    
    # ==================== 数据同步测试 ====================
    
    def test_data_sync(self):
        """测试MySQL到Redis的数据同步"""
        # 在MySQL中插入数据
        SessionLocal = sessionmaker(bind=self.gateway.mysql_engine)
        session = SessionLocal()
        
        try:
            record = RedisGatewayInfo(
                key='sync_key',
                name='_string',
                value='sync_value',
                expire_time=None,
                last_update_time=datetime.now()
            )
            session.add(record)
            session.commit()
            
            # 执行同步
            self.gateway.health_checker._sync_mysql_to_redis()
            
            # 从Redis读取验证
            value = self.gateway.redis_client.get('sync_key')
            if value:
                value = value.decode('utf-8')
            self.assertEqual(value, 'sync_value')
            
        finally:
            session.close()


def run_tests():
    """运行测试"""
    # 创建测试套件
    suite = unittest.TestLoader().loadTestsFromTestCase(TestRedisGateway)
    
    # 运行测试
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # 返回测试结果
    return result.wasSuccessful()


if __name__ == '__main__':
    success = run_tests()
    exit(0 if success else 1)
