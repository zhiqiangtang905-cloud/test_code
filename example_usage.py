"""
Redis降级服务使用示例
演示如何在实际项目中使用Redis降级MySQL的功能
"""
import logging
from health_check import get_health_check
from redis_fallback_service import get_redis_fallback_service

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_redis_cache_service():
    """
    获取Redis服务（这是你原有项目中的函数）
    需要根据你的实际项目实现
    
    示例实现：
    import redis
    return redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    """
    # 这里需要替换成你实际的Redis服务获取方式
    import redis
    return redis.Redis(
        host='localhost',
        port=6379,
        db=0,
        decode_responses=True,
        socket_connect_timeout=2,
        socket_timeout=2
    )


def get_db_session():
    """
    获取数据库会话（这是你原有项目中的函数）
    需要根据你的实际项目实现
    
    示例实现：
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from contextlib import contextmanager
    
    engine = create_engine('mysql+pymysql://user:password@localhost/redis_gateway_info')
    SessionLocal = sessionmaker(bind=engine)
    
    @contextmanager
    def session_scope():
        session = SessionLocal()
        try:
            yield session
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    return session_scope()
    """
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from contextlib import contextmanager
    
    # 这里需要替换成你实际的数据库连接配置
    engine = create_engine(
        'mysql+pymysql://root:password@localhost:3306/redis_gateway_info',
        pool_size=10,
        max_overflow=20,
        pool_pre_ping=True
    )
    
    SessionLocal = sessionmaker(bind=engine)
    
    @contextmanager
    def session_scope():
        """数据库会话上下文管理器"""
        session = SessionLocal()
        try:
            yield session
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    return session_scope()


def init_database():
    """
    初始化数据库表
    首次使用时需要调用此函数创建表结构
    """
    from sqlalchemy import create_engine
    from models import Base
    
    engine = create_engine(
        'mysql+pymysql://root:password@localhost:3306/redis_gateway_info'
    )
    
    # 创建所有表
    Base.metadata.create_all(engine)
    logger.info("数据库表创建成功")


def main():
    """主函数：演示如何使用Redis降级服务"""
    
    # 1. 初始化数据库（首次使用时需要）
    # init_database()
    
    # 2. 初始化健康检查服务
    health_check = get_health_check(get_redis_cache_service, get_db_session)
    
    # 3. 启动定时任务（每60秒清理过期数据）
    health_check.start_schedule()
    
    # 4. 获取Redis降级服务实例
    redis_service = get_redis_fallback_service(
        get_redis_cache_service,
        get_db_session,
        health_check
    )
    
    logger.info("=" * 60)
    logger.info("Redis降级服务初始化完成")
    logger.info("=" * 60)
    
    # ==================== 基础操作示例 ====================
    
    # 设置和获取普通key-value
    redis_service.set("user:1001", {"name": "张三", "age": 25})
    user_data = redis_service.get("user:1001")
    logger.info(f"获取用户数据: {user_data}")
    
    # 设置带过期时间的key
    redis_service.set_ex("session:abc123", {"user_id": 1001, "login_time": "2025-10-16"}, ttl=3600)
    session_data = redis_service.get("session:abc123")
    logger.info(f"获取会话数据: {session_data}")
    
    # 检查key是否存在
    exists = redis_service.exists("user:1001")
    logger.info(f"user:1001是否存在: {exists}")
    
    # 删除key
    # redis_service.delete("user:1001")
    
    # ==================== Hash操作示例 ====================
    
    # 设置hash字段
    redis_service.hset("user:profile:1001", "name", "张三")
    redis_service.hset("user:profile:1001", "age", 25)
    redis_service.hset("user:profile:1001", "city", "北京")
    
    # 获取hash字段
    name = redis_service.hget("user:profile:1001", "name")
    logger.info(f"用户名: {name}")
    
    # 获取hash所有字段
    profile = redis_service.hgetall("user:profile:1001")
    logger.info(f"用户完整资料: {profile}")
    
    # 删除hash字段
    # redis_service.hdel("user:profile:1001", "city")
    
    # ==================== List操作示例 ====================
    
    # 从右侧插入元素
    redis_service.rpush("message_queue", "消息1", "消息2", "消息3")
    
    # 从左侧插入元素
    redis_service.lpush("message_queue", "紧急消息")
    
    # 获取列表长度
    queue_length = redis_service.llen("message_queue")
    logger.info(f"消息队列长度: {queue_length}")
    
    # 获取列表范围
    messages = redis_service.lrange("message_queue", 0, -1)
    logger.info(f"所有消息: {messages}")
    
    # 获取指定索引的元素
    first_message = redis_service.lindex("message_queue", 0)
    logger.info(f"第一条消息: {first_message}")
    
    # 从左侧弹出元素
    popped = redis_service.lpop("message_queue")
    logger.info(f"弹出的消息: {popped}")
    
    # 删除指定值的元素
    # redis_service.lrem("message_queue", 1, "消息2")
    
    # ==================== 分布式锁示例 ====================
    
    logger.info("\n" + "=" * 60)
    logger.info("分布式锁示例")
    logger.info("=" * 60)
    
    # 使用分布式锁保护关键操作
    with redis_service.lock("critical_section:task1", ttl=30) as acquired:
        if acquired:
            logger.info("成功获取锁，执行关键操作...")
            # 这里执行需要加锁的操作
            import time
            time.sleep(2)
            logger.info("关键操作完成")
        else:
            logger.warning("获取锁失败，其他实例正在执行")
    
    # ==================== 信号量示例 ====================
    
    logger.info("\n" + "=" * 60)
    logger.info("信号量示例")
    logger.info("=" * 60)
    
    # 限制并发数为3
    semaphore_key = "concurrent_task:limit3"
    if redis_service.acquire_semaphore(semaphore_key, limit=3, ttl=60):
        logger.info("成功获取信号量，可以执行任务")
        # 执行任务
        import time
        time.sleep(1)
        logger.info("任务完成")
    else:
        logger.warning("信号量已满，稍后再试")
    
    # ==================== 模拟Redis故障 ====================
    
    logger.info("\n" + "=" * 60)
    logger.info("模拟Redis故障场景")
    logger.info("=" * 60)
    
    # 手动标记Redis为不可用（实际场景中这会在Redis操作失败时自动触发）
    # health_check.mark_redis_down()
    # logger.info("Redis已标记为不可用，所有操作将降级到MySQL")
    
    # 即使Redis不可用，服务仍然可以正常工作
    # redis_service.set("test:key", {"message": "这条数据写入了MySQL"})
    # test_data = redis_service.get("test:key")
    # logger.info(f"从MySQL读取的数据: {test_data}")
    
    logger.info("\n" + "=" * 60)
    logger.info("示例运行完成")
    logger.info("健康检查服务将在后台持续运行，每60秒清理一次过期数据")
    logger.info("如果Redis不可用，将每10秒进行一次健康拨测")
    logger.info("=" * 60)
    
    # 保持程序运行以观察定时任务
    try:
        import time
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        logger.info("程序退出")
        health_check.stop_schedule()


if __name__ == "__main__":
    main()
