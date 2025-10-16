"""
Redis降级服务测试脚本
用于验证服务是否正常工作
"""
import logging
import time
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_basic_operations():
    """测试基础操作"""
    logger.info("\n" + "="*60)
    logger.info("测试1: 基础操作（set_ex, get）")
    logger.info("="*60)
    
    try:
        from redis_fallback_service import RedisFallbackService
        # 这里需要你提供实际的get_redis和get_db_session函数
        # from your_module import get_redis_cache_service, get_db_session
        
        # service = RedisFallbackService(get_redis_cache_service, get_db_session)
        
        # 测试写入
        # test_key = f"test:basic:{int(time.time())}"
        # test_value = {"name": "测试用户", "timestamp": datetime.now().isoformat()}
        # service.set_ex(test_key, test_value, ttl=60)
        # logger.info(f"✓ 写入成功: {test_key} = {test_value}")
        
        # 测试读取
        # result = service.get(test_key)
        # logger.info(f"✓ 读取成功: {result}")
        
        # 验证数据一致性
        # if result == test_value:
        #     logger.info("✓ 数据一致性验证通过")
        # else:
        #     logger.error(f"✗ 数据不一致: 期望{test_value}, 实际{result}")
        
        logger.info("基础操作测试完成")
        
    except Exception as e:
        logger.error(f"✗ 基础操作测试失败: {e}")


def test_hash_operations():
    """测试Hash操作"""
    logger.info("\n" + "="*60)
    logger.info("测试2: Hash操作（hset, hget, hgetall, hdel）")
    logger.info("="*60)
    
    try:
        # from redis_fallback_service import RedisFallbackService
        # from your_module import get_redis_cache_service, get_db_session
        
        # service = RedisFallbackService(get_redis_cache_service, get_db_session)
        
        # 测试Hash
        # hash_key = f"test:hash:{int(time.time())}"
        # service.hset(hash_key, "field1", "value1")
        # service.hset(hash_key, "field2", {"nested": "value2"})
        # logger.info(f"✓ Hash写入成功")
        
        # 读取单个字段
        # value1 = service.hget(hash_key, "field1")
        # logger.info(f"✓ 读取字段1: {value1}")
        
        # 读取所有字段
        # all_fields = service.hgetall(hash_key)
        # logger.info(f"✓ 读取所有字段: {all_fields}")
        
        # 删除字段
        # service.hdel(hash_key, "field1")
        # logger.info(f"✓ 删除字段成功")
        
        # 验证删除
        # value1_after_delete = service.hget(hash_key, "field1")
        # if value1_after_delete is None:
        #     logger.info("✓ 字段删除验证通过")
        
        logger.info("Hash操作测试完成")
        
    except Exception as e:
        logger.error(f"✗ Hash操作测试失败: {e}")


def test_list_operations():
    """测试List操作"""
    logger.info("\n" + "="*60)
    logger.info("测试3: List操作（rpush, lpush, lrange, llen）")
    logger.info("="*60)
    
    try:
        # from redis_fallback_service import RedisFallbackService
        # from your_module import get_redis_cache_service, get_db_session
        
        # service = RedisFallbackService(get_redis_cache_service, get_db_session)
        
        # 测试List
        # list_key = f"test:list:{int(time.time())}"
        # service.rpush(list_key, "item1", "item2", "item3")
        # logger.info(f"✓ rpush成功")
        
        # service.lpush(list_key, "item0")
        # logger.info(f"✓ lpush成功")
        
        # 读取列表
        # items = service.lrange(list_key, 0, -1)
        # logger.info(f"✓ 列表内容: {items}")
        
        # 获取长度
        # length = service.llen(list_key)
        # logger.info(f"✓ 列表长度: {length}")
        
        logger.info("List操作测试完成")
        
    except Exception as e:
        logger.error(f"✗ List操作测试失败: {e}")


def test_distributed_lock():
    """测试分布式锁"""
    logger.info("\n" + "="*60)
    logger.info("测试4: 分布式锁")
    logger.info("="*60)
    
    try:
        # from redis_fallback_service import RedisFallbackService
        # from your_module import get_redis_cache_service, get_db_session
        
        # service = RedisFallbackService(get_redis_cache_service, get_db_session)
        
        # lock_key = f"test:lock:{int(time.time())}"
        
        # 测试获取锁
        # try:
        #     with service.distributed_lock(lock_key, timeout=10):
        #         logger.info("✓ 成功获取分布式锁")
        #         logger.info("执行临界区代码...")
        #         time.sleep(1)
        #         logger.info("✓ 任务执行完成")
        # except RuntimeError as e:
        #     logger.error(f"✗ 无法获取锁: {e}")
        
        logger.info("分布式锁测试完成")
        
    except Exception as e:
        logger.error(f"✗ 分布式锁测试失败: {e}")


def test_redis_failover():
    """测试Redis故障切换"""
    logger.info("\n" + "="*60)
    logger.info("测试5: Redis故障切换（需要手动停止Redis）")
    logger.info("="*60)
    
    try:
        # from redis_fallback_service import RedisFallbackService
        # from your_module import get_redis_cache_service, get_db_session
        
        # service = RedisFallbackService(get_redis_cache_service, get_db_session)
        
        # 正常写入
        # test_key = f"test:failover:{int(time.time())}"
        # service.set_ex(test_key, "test_value", ttl=300)
        # logger.info("✓ 正常写入成功")
        
        # logger.info("\n请手动停止Redis服务，然后按Enter继续...")
        # input()
        
        # 尝试读取（应该降级到MySQL）
        # logger.info("尝试读取数据...")
        # value = service.get(test_key)
        # if value:
        #     logger.info(f"✓ 从MySQL读取成功: {value}")
        # else:
        #     logger.error("✗ 读取失败")
        
        # logger.info("\n请重新启动Redis服务，然后按Enter继续...")
        # input()
        
        # logger.info("等待健康拨测检测到Redis恢复...")
        # time.sleep(15)  # 等待健康拨测
        
        # if service.health_checker.is_redis_alive:
        #     logger.info("✓ Redis已恢复，数据已同步")
        # else:
        #     logger.warning("Redis可能仍未恢复")
        
        logger.info("故障切换测试完成")
        logger.info("注意：此测试需要手动操作Redis服务")
        
    except Exception as e:
        logger.error(f"✗ 故障切换测试失败: {e}")


def test_scheduled_cleanup():
    """测试定时清理"""
    logger.info("\n" + "="*60)
    logger.info("测试6: 定时清理过期数据")
    logger.info("="*60)
    
    try:
        # from redis_fallback_service import RedisFallbackService
        # from your_module import get_redis_cache_service, get_db_session
        
        # service = RedisFallbackService(get_redis_cache_service, get_db_session)
        
        # 启动调度器
        # service.start_scheduler()
        # logger.info("✓ 调度器已启动")
        
        # 写入一些短期数据
        # for i in range(3):
        #     key = f"test:expire:{i}"
        #     service.set_ex(key, f"value_{i}", ttl=5)  # 5秒过期
        # logger.info("✓ 写入3条短期数据（5秒后过期）")
        
        # logger.info("等待数据过期...")
        # time.sleep(10)
        
        # 手动触发清理
        # service.cleanup_all_expired_data()
        # logger.info("✓ 手动清理完成")
        
        # 验证数据已清理
        # for i in range(3):
        #     key = f"test:expire:{i}"
        #     value = service.get(key)
        #     if value is None:
        #         logger.info(f"✓ 数据{key}已被清理")
        #     else:
        #         logger.warning(f"✗ 数据{key}仍存在: {value}")
        
        logger.info("定时清理测试完成")
        
    except Exception as e:
        logger.error(f"✗ 定时清理测试失败: {e}")


def run_all_tests():
    """运行所有测试"""
    logger.info("\n" + "="*70)
    logger.info("Redis降级服务测试套件")
    logger.info("="*70)
    logger.info(f"开始时间: {datetime.now()}")
    
    # 检查依赖
    try:
        import redis
        import sqlalchemy
        import schedule
        logger.info("✓ 所有依赖包已安装")
    except ImportError as e:
        logger.error(f"✗ 缺少依赖包: {e}")
        logger.error("请运行: pip install -r requirements.txt")
        return
    
    logger.info("\n注意: 以下测试需要你先配置好Redis和MySQL连接")
    logger.info("请在测试代码中取消注释并提供get_redis_cache_service和get_db_session函数")
    logger.info("\n如果已配置，按Enter继续，否则按Ctrl+C退出")
    try:
        input()
    except KeyboardInterrupt:
        logger.info("\n测试已取消")
        return
    
    # 运行所有测试
    test_basic_operations()
    test_hash_operations()
    test_list_operations()
    test_distributed_lock()
    test_redis_failover()
    test_scheduled_cleanup()
    
    logger.info("\n" + "="*70)
    logger.info("所有测试完成")
    logger.info(f"结束时间: {datetime.now()}")
    logger.info("="*70)


def quick_test():
    """快速测试 - 验证代码是否可以导入"""
    logger.info("快速测试：验证模块导入")
    
    try:
        # 测试导入
        from redis_models import RedisGatewayInfo, Base
        logger.info("✓ redis_models 导入成功")
        
        from redis_health_checker import RedisHealthChecker
        logger.info("✓ redis_health_checker 导入成功")
        
        from redis_fallback_service import RedisFallbackService, RedisDistributedLock
        logger.info("✓ redis_fallback_service 导入成功")
        
        logger.info("\n所有模块导入成功！代码结构正确。")
        logger.info("下一步：配置Redis和MySQL连接，然后运行完整测试。")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ 模块导入失败: {e}")
        logger.error("请检查代码是否有语法错误")
        return False


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--quick":
        # 快速测试模式
        quick_test()
    else:
        # 完整测试模式
        run_all_tests()
