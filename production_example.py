"""
生产环境使用示例
演示如何在生产环境中正确使用Redis降级服务
"""
import logging
import redis
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager

from config import config
from init_service import quick_init, get_manager

# 配置日志
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format=config.LOG_FORMAT
)
logger = logging.getLogger(__name__)


# ==================== 连接管理 ====================

# Redis连接池（单例）
_redis_pool = None

def get_redis_cache_service():
    """
    获取Redis服务（使用连接池）
    
    注意：这是你原项目中的函数，这里提供一个参考实现
    建议使用连接池以提高性能
    """
    global _redis_pool
    
    if _redis_pool is None:
        # 创建Redis连接池
        _redis_pool = redis.ConnectionPool(
            **config.get_redis_connection_kwargs()
        )
    
    return redis.Redis(connection_pool=_redis_pool)


# SQLAlchemy引擎（单例）
_db_engine = None

def get_db_engine():
    """获取数据库引擎"""
    global _db_engine
    
    if _db_engine is None:
        _db_engine = create_engine(
            config.get_mysql_url(),
            pool_size=config.MYSQL_POOL_SIZE,
            max_overflow=config.MYSQL_MAX_OVERFLOW,
            pool_recycle=config.MYSQL_POOL_RECYCLE,
            pool_pre_ping=config.MYSQL_POOL_PRE_PING,
            echo=False  # 生产环境不输出SQL
        )
    
    return _db_engine


def get_db_session():
    """
    获取数据库会话上下文管理器
    
    注意：这是你原项目中的函数，这里提供一个参考实现
    使用上下文管理器确保会话正确关闭
    """
    engine = get_db_engine()
    SessionLocal = sessionmaker(bind=engine)
    
    @contextmanager
    def session_scope():
        """提供事务性会话作用域"""
        session = SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    return session_scope()


# ==================== 服务初始化 ====================

def init_redis_fallback_service():
    """
    初始化Redis降级服务
    建议在应用启动时调用一次
    
    Returns:
        RedisFallbackService: 降级服务实例
    """
    try:
        # 验证配置
        config.validate()
        
        # 打印配置（方便排查问题）
        if config.LOG_LEVEL == 'DEBUG':
            config.print_config()
        
        # 初始化服务
        logger.info("正在初始化Redis降级服务...")
        redis_service = quick_init(
            get_redis_cache_service,
            get_db_session,
            auto_start_schedule=True  # 自动启动定时任务
        )
        
        logger.info("Redis降级服务初始化成功")
        return redis_service
        
    except Exception as e:
        logger.error(f"初始化Redis降级服务失败: {e}", exc_info=True)
        raise


# ==================== 业务示例 ====================

class UserService:
    """
    用户服务示例
    演示如何在业务代码中使用Redis降级服务
    """
    
    def __init__(self, redis_service):
        self.redis = redis_service
    
    def cache_user_info(self, user_id: int, user_info: dict, ttl: int = 3600):
        """
        缓存用户信息
        
        Args:
            user_id: 用户ID
            user_info: 用户信息字典
            ttl: 缓存过期时间（秒）
        """
        cache_key = f"user:info:{user_id}"
        self.redis.set_ex(cache_key, user_info, ttl=ttl)
        logger.info(f"用户信息已缓存: user_id={user_id}")
    
    def get_user_info(self, user_id: int) -> dict:
        """
        获取用户信息
        
        Args:
            user_id: 用户ID
            
        Returns:
            dict: 用户信息，不存在返回None
        """
        cache_key = f"user:info:{user_id}"
        user_info = self.redis.get(cache_key)
        
        if user_info:
            logger.info(f"从缓存获取用户信息: user_id={user_id}")
            return user_info
        else:
            logger.info(f"缓存未命中: user_id={user_id}")
            # 这里可以从数据库加载并缓存
            return None
    
    def cache_user_profile(self, user_id: int, profile: dict):
        """
        使用Hash缓存用户资料
        
        Args:
            user_id: 用户ID
            profile: 用户资料字典
        """
        hash_name = f"user:profile:{user_id}"
        
        for key, value in profile.items():
            self.redis.hset(hash_name, key, value)
        
        logger.info(f"用户资料已缓存: user_id={user_id}")
    
    def get_user_profile(self, user_id: int) -> dict:
        """
        获取用户资料
        
        Args:
            user_id: 用户ID
            
        Returns:
            dict: 用户资料
        """
        hash_name = f"user:profile:{user_id}"
        return self.redis.hgetall(hash_name)


class TaskQueue:
    """
    任务队列示例
    演示如何使用List实现简单的任务队列
    """
    
    def __init__(self, redis_service, queue_name: str = "task:queue"):
        self.redis = redis_service
        self.queue_name = queue_name
    
    def push_task(self, task: dict):
        """
        添加任务到队列
        
        Args:
            task: 任务信息字典
        """
        self.redis.rpush(self.queue_name, task)
        logger.info(f"任务已添加到队列: {task}")
    
    def pop_task(self) -> dict:
        """
        从队列获取任务
        
        Returns:
            dict: 任务信息，队列为空返回None
        """
        task = self.redis.lpop(self.queue_name)
        if task:
            logger.info(f"从队列获取任务: {task}")
        return task
    
    def get_queue_length(self) -> int:
        """
        获取队列长度
        
        Returns:
            int: 队列中的任务数量
        """
        return self.redis.llen(self.queue_name)


class DistributedTaskExecutor:
    """
    分布式任务执行器示例
    演示如何使用分布式锁避免任务重复执行
    """
    
    def __init__(self, redis_service):
        self.redis = redis_service
    
    def execute_once(self, task_id: str, task_func, *args, **kwargs):
        """
        确保任务在多实例场景下只执行一次
        
        Args:
            task_id: 任务唯一标识
            task_func: 任务函数
            *args, **kwargs: 任务函数参数
            
        Returns:
            任务执行结果或None（如果获取锁失败）
        """
        lock_key = f"task:lock:{task_id}"
        
        with self.redis.lock(lock_key, ttl=300) as acquired:
            if acquired:
                logger.info(f"成功获取任务锁，开始执行: {task_id}")
                try:
                    result = task_func(*args, **kwargs)
                    logger.info(f"任务执行完成: {task_id}")
                    return result
                except Exception as e:
                    logger.error(f"任务执行失败: {task_id}, error: {e}")
                    raise
            else:
                logger.warning(f"获取任务锁失败，其他实例正在执行: {task_id}")
                return None


# ==================== 主程序 ====================

def main():
    """主函数：演示完整的使用流程"""
    
    logger.info("=" * 60)
    logger.info("Redis降级服务 - 生产环境示例")
    logger.info("=" * 60)
    
    # 1. 初始化服务
    redis_service = init_redis_fallback_service()
    
    # 2. 使用用户服务
    logger.info("\n" + "=" * 60)
    logger.info("用户服务示例")
    logger.info("=" * 60)
    
    user_service = UserService(redis_service)
    
    # 缓存用户信息
    user_service.cache_user_info(
        user_id=1001,
        user_info={
            "name": "张三",
            "email": "zhangsan@example.com",
            "age": 25
        },
        ttl=3600
    )
    
    # 获取用户信息
    user_info = user_service.get_user_info(1001)
    logger.info(f"用户信息: {user_info}")
    
    # 缓存用户资料
    user_service.cache_user_profile(
        user_id=1001,
        profile={
            "nickname": "小张",
            "city": "北京",
            "company": "某某公司"
        }
    )
    
    # 获取用户资料
    profile = user_service.get_user_profile(1001)
    logger.info(f"用户资料: {profile}")
    
    # 3. 使用任务队列
    logger.info("\n" + "=" * 60)
    logger.info("任务队列示例")
    logger.info("=" * 60)
    
    task_queue = TaskQueue(redis_service)
    
    # 添加任务
    task_queue.push_task({"task_id": "T001", "type": "email", "to": "user@example.com"})
    task_queue.push_task({"task_id": "T002", "type": "sms", "phone": "13800138000"})
    
    logger.info(f"队列长度: {task_queue.get_queue_length()}")
    
    # 处理任务
    task = task_queue.pop_task()
    logger.info(f"处理任务: {task}")
    
    # 4. 使用分布式任务执行器
    logger.info("\n" + "=" * 60)
    logger.info("分布式任务执行器示例")
    logger.info("=" * 60)
    
    executor = DistributedTaskExecutor(redis_service)
    
    def sample_task(message: str):
        """示例任务"""
        logger.info(f"执行任务: {message}")
        import time
        time.sleep(1)
        return f"任务完成: {message}"
    
    result = executor.execute_once(
        task_id="daily_report_2025_10_16",
        task_func=sample_task,
        message="生成每日报告"
    )
    logger.info(f"任务结果: {result}")
    
    # 5. 检查服务状态
    logger.info("\n" + "=" * 60)
    logger.info("服务状态")
    logger.info("=" * 60)
    
    manager = get_manager()
    redis_alive = manager.is_redis_alive()
    logger.info(f"Redis状态: {'可用' if redis_alive else '不可用（已降级到MySQL）'}")
    
    logger.info("\n" + "=" * 60)
    logger.info("示例运行完成")
    logger.info("=" * 60)
    
    logger.info("\n服务将持续运行以下后台任务：")
    logger.info(f"  ✓ 每{config.CLEANUP_INTERVAL}秒清理过期数据")
    logger.info(f"  ✓ Redis不可用时每{config.HEALTH_CHECK_INTERVAL}秒健康拨测")
    logger.info("  ✓ 多实例场景使用分布式锁协调")


if __name__ == "__main__":
    try:
        main()
        
        # 保持程序运行
        logger.info("\n按 Ctrl+C 退出程序")
        import time
        while True:
            time.sleep(10)
            
    except KeyboardInterrupt:
        logger.info("\n正在优雅退出...")
        manager = get_manager()
        manager.stop()
        logger.info("程序已退出")
    except Exception as e:
        logger.error(f"程序异常: {e}", exc_info=True)
        import sys
        sys.exit(1)
