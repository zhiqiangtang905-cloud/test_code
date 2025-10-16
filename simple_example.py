"""
简化的使用示例
演示最简单的集成方式
"""
import logging
from init_service import quick_init

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


def get_redis_cache_service():
    """
    获取Redis服务
    这是你原项目中已有的函数
    """
    import redis
    return redis.Redis(
        host='localhost',
        port=6379,
        db=0,
        decode_responses=True
    )


def get_db_session():
    """
    获取数据库会话
    这是你原项目中已有的函数
    """
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from contextlib import contextmanager
    
    engine = create_engine(
        'mysql+pymysql://root:password@localhost:3306/redis_gateway_info'
    )
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


def main():
    """
    最简化的使用方式
    只需要3行代码即可完成集成
    """
    
    # 1. 一行代码初始化服务（使用你原项目的函数）
    redis_service = quick_init(get_redis_cache_service, get_db_session)
    
    # 2. 像使用普通Redis一样使用（完全透明）
    redis_service.set("test:key", {"message": "Hello World", "count": 123})
    
    # 3. 读取数据
    data = redis_service.get("test:key")
    print(f"读取到的数据: {data}")
    
    # 其他操作示例
    print("\n" + "=" * 60)
    print("更多操作示例")
    print("=" * 60)
    
    # Hash操作
    redis_service.hset("user:1001", "name", "张三")
    redis_service.hset("user:1001", "age", 25)
    user_info = redis_service.hgetall("user:1001")
    print(f"用户信息: {user_info}")
    
    # List操作
    redis_service.rpush("tasks", "任务1", "任务2", "任务3")
    tasks = redis_service.lrange("tasks", 0, -1)
    print(f"任务列表: {tasks}")
    
    # 分布式锁
    with redis_service.lock("my_lock", ttl=30) as acquired:
        if acquired:
            print("成功获取锁，执行业务逻辑...")
        else:
            print("获取锁失败")
    
    print("\n服务已就绪，支持：")
    print("✓ Redis自动降级到MySQL")
    print("✓ 自动健康检查和恢复")
    print("✓ 多实例分布式协调")
    print("✓ 定时清理过期数据")


if __name__ == "__main__":
    main()
