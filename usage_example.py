"""
Redis降级服务使用示例
展示如何在实际项目中使用RedisFallbackService
"""
import logging
from redis_fallback_service import RedisFallbackService

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def example_basic_usage():
    """基础使用示例"""
    
    # 假设你已经在项目中定义了这两个函数
    # from your_module import get_redis_cache_service, get_db_session
    
    # 初始化Redis降级服务
    # service = RedisFallbackService(get_redis_cache_service, get_db_session)
    
    # 启动定时任务调度器（每60秒清理一次过期数据）
    # service.start_scheduler()
    
    # ========== 1. 基础的key-value操作 ==========
    # 设置带过期时间的值
    # service.set_ex("user:1001", {"name": "张三", "age": 25}, ttl=3600)
    
    # 获取值（优先从Redis读取，失败时自动降级到MySQL）
    # user_data = service.get("user:1001")
    # logger.info(f"用户数据: {user_data}")
    
    # ========== 2. Hash操作 ==========
    # 设置Hash字段
    # service.hset("user:profile:1001", "nickname", "小明")
    # service.hset("user:profile:1001", "email", "xiaoming@example.com")
    
    # 获取Hash字段
    # nickname = service.hget("user:profile:1001", "nickname")
    # logger.info(f"昵称: {nickname}")
    
    # 获取Hash的所有字段
    # profile = service.hgetall("user:profile:1001")
    # logger.info(f"完整资料: {profile}")
    
    # 删除Hash字段
    # service.hdel("user:profile:1001", "email")
    
    # ========== 3. List操作 ==========
    # 向列表右侧推入
    # service.rpush("task:queue", "task1", "task2", "task3")
    
    # 向列表左侧推入
    # service.lpush("task:queue", "urgent_task")
    
    # 获取列表范围
    # tasks = service.lrange("task:queue", 0, -1)
    # logger.info(f"任务列表: {tasks}")
    
    # 获取列表长度
    # length = service.llen("task:queue")
    # logger.info(f"任务数量: {length}")
    
    # 从列表左侧弹出
    # task = service.lpop("task:queue")
    # logger.info(f"弹出任务: {task}")
    
    # ========== 4. 其他操作 ==========
    # 检查键是否存在
    # exists = service.exists("user:1001")
    # logger.info(f"键是否存在: {exists}")
    
    # 设置过期时间
    # service.expire("user:1001", 7200)
    
    # 删除键
    # service.delete("user:1001", "user:profile:1001")
    
    logger.info("基础操作示例完成")


def example_distributed_lock():
    """分布式锁使用示例"""
    
    # from your_module import get_redis_cache_service, get_db_session
    # service = RedisFallbackService(get_redis_cache_service, get_db_session)
    
    # 使用分布式锁确保多实例环境下只有一个实例执行任务
    # try:
    #     with service.distributed_lock("task:process_orders", timeout=60):
    #         logger.info("获取到分布式锁，开始处理订单...")
    #         
    #         # 执行你的业务逻辑
    #         process_orders()
    #         
    #         logger.info("订单处理完成，释放锁")
    # except RuntimeError as e:
    #     logger.warning(f"无法获取锁: {e}")
    #     logger.info("其他实例正在处理，跳过本次执行")
    
    pass


def example_semaphore():
    """信号量使用示例"""
    
    # from your_module import get_redis_cache_service, get_db_session
    # service = RedisFallbackService(get_redis_cache_service, get_db_session)
    
    # 使用信号量限制并发数量（例如限制同时只能有3个实例处理）
    # semaphore_key = "semaphore:api_calls"
    # limit = 3
    # timeout = 30
    
    # if service.acquire_semaphore(semaphore_key, limit, timeout):
    #     try:
    #         logger.info("获取信号量成功，执行API调用...")
    #         
    #         # 执行API调用
    #         result = call_external_api()
    #         
    #         logger.info(f"API调用完成: {result}")
    #     finally:
    #         # 释放信号量
    #         import time
    #         identifier = f"{threading.current_thread().ident}_{time.time()}"
    #         service.release_semaphore(semaphore_key, identifier)
    # else:
    #     logger.warning("信号量已满，稍后重试")
    
    pass


def example_health_check():
    """健康检查示例"""
    
    # from your_module import get_redis_cache_service, get_db_session
    # service = RedisFallbackService(get_redis_cache_service, get_db_session)
    
    # 健康检查器会自动工作：
    # 1. 当Redis操作失败时，自动标记Redis为不可用
    # 2. 启动健康拨测（每10秒检测一次）
    # 3. 所有读写操作自动降级到MySQL
    # 4. Redis恢复后，自动将MySQL数据同步回Redis
    # 5. 恢复正常的Redis读写
    
    # 你只需要正常使用service的方法即可，无需手动处理降级逻辑
    # service.set_ex("test_key", "test_value", ttl=60)
    # value = service.get("test_key")
    
    logger.info("健康检查会在后台自动运行")


def example_complete_workflow():
    """完整工作流示例"""
    
    logger.info("=== Redis降级服务完整使用示例 ===")
    
    # from your_module import get_redis_cache_service, get_db_session
    
    # 1. 初始化服务
    # service = RedisFallbackService(get_redis_cache_service, get_db_session)
    # logger.info("1. Redis降级服务已初始化")
    
    # 2. 启动定时任务调度器
    # service.start_scheduler()
    # logger.info("2. 定时任务调度器已启动（每60秒清理过期数据）")
    
    # 3. 在业务代码中使用（多实例环境下确保只有一个实例执行）
    # try:
    #     with service.distributed_lock("scheduled_task:daily_report", timeout=300):
    #         logger.info("3. 获取分布式锁成功，开始执行定时任务...")
    #         
    #         # 执行业务逻辑
    #         # 读取配置
    #         config = service.hgetall("app:config")
    #         
    #         # 处理数据
    #         service.rpush("reports:queue", {"date": "2025-10-16", "type": "daily"})
    #         
    #         # 缓存结果
    #         service.set_ex("report:latest", {"status": "completed"}, ttl=86400)
    #         
    #         logger.info("4. 任务执行完成")
    # except RuntimeError:
    #     logger.info("其他实例正在执行任务，本实例跳过")
    
    # 5. 程序退出时清理
    # service.stop_scheduler()
    # logger.info("5. 调度器已停止")
    
    logger.info("=== 示例完成 ===")


if __name__ == "__main__":
    # 运行示例
    example_basic_usage()
    example_distributed_lock()
    example_semaphore()
    example_health_check()
    example_complete_workflow()
