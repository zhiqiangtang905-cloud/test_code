"""
配置模板
复制此文件为config.py，然后修改为你的实际配置
"""
import redis
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager


# ==================== Redis配置 ====================

class RedisConfig:
    """Redis连接配置"""
    HOST = 'localhost'      # Redis主机地址
    PORT = 6379             # Redis端口
    DB = 0                  # Redis数据库编号
    PASSWORD = None         # Redis密码（如果有的话）
    DECODE_RESPONSES = True # 自动解码响应为字符串


# ==================== MySQL配置 ====================

class MySQLConfig:
    """MySQL连接配置"""
    HOST = 'localhost'          # MySQL主机地址
    PORT = 3306                 # MySQL端口
    USER = 'root'               # MySQL用户名
    PASSWORD = 'your_password'  # MySQL密码
    DATABASE = 'mydb'           # 数据库名称
    CHARSET = 'utf8mb4'         # 字符集


# ==================== SQLAlchemy配置 ====================

class DatabaseConfig:
    """数据库连接配置"""
    
    # 数据库连接URL
    SQLALCHEMY_DATABASE_URI = (
        f"mysql+pymysql://{MySQLConfig.USER}:{MySQLConfig.PASSWORD}"
        f"@{MySQLConfig.HOST}:{MySQLConfig.PORT}/{MySQLConfig.DATABASE}"
        f"?charset={MySQLConfig.CHARSET}"
    )
    
    # 连接池配置
    SQLALCHEMY_POOL_SIZE = 10           # 连接池大小
    SQLALCHEMY_POOL_RECYCLE = 3600      # 连接回收时间（秒）
    SQLALCHEMY_POOL_PRE_PING = True     # 连接前预检查
    SQLALCHEMY_ECHO = False             # 是否打印SQL（调试用）


# ==================== Redis降级服务配置 ====================

class FallbackConfig:
    """降级服务配置"""
    
    # 健康拨测间隔（秒）
    HEALTH_CHECK_INTERVAL = 10
    
    # 定时清理间隔（秒）
    CLEANUP_INTERVAL = 60
    
    # 分布式锁默认超时（秒）
    DEFAULT_LOCK_TIMEOUT = 30
    
    # 是否启用自动调度器
    ENABLE_SCHEDULER = True


# ==================== 连接实例 ====================

# 创建Redis客户端
redis_client = None

def get_redis_client():
    """获取Redis客户端（单例）"""
    global redis_client
    if redis_client is None:
        redis_client = redis.Redis(
            host=RedisConfig.HOST,
            port=RedisConfig.PORT,
            db=RedisConfig.DB,
            password=RedisConfig.PASSWORD,
            decode_responses=RedisConfig.DECODE_RESPONSES,
            socket_connect_timeout=5,
            socket_timeout=5,
            retry_on_timeout=True
        )
    return redis_client


# 创建数据库引擎
engine = None
SessionLocal = None

def get_database_engine():
    """获取数据库引擎（单例）"""
    global engine, SessionLocal
    if engine is None:
        engine = create_engine(
            DatabaseConfig.SQLALCHEMY_DATABASE_URI,
            pool_size=DatabaseConfig.SQLALCHEMY_POOL_SIZE,
            pool_recycle=DatabaseConfig.SQLALCHEMY_POOL_RECYCLE,
            pool_pre_ping=DatabaseConfig.SQLALCHEMY_POOL_PRE_PING,
            echo=DatabaseConfig.SQLALCHEMY_ECHO,
        )
        SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
    return engine


# ==================== 服务函数 ====================

def get_redis_cache_service():
    """
    获取Redis缓存服务
    这是RedisFallbackService需要的函数
    """
    return get_redis_client()


@contextmanager
def get_db_session():
    """
    获取数据库会话（上下文管理器）
    这是RedisFallbackService需要的函数
    
    使用方式:
        with get_db_session() as session:
            # 使用session进行数据库操作
            pass
    """
    get_database_engine()  # 确保引擎已创建
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


# ==================== 初始化函数 ====================

def initialize_services():
    """
    初始化所有服务
    在应用启动时调用
    """
    from redis_fallback_service import RedisFallbackService
    
    # 创建Redis降级服务
    service = RedisFallbackService(
        get_redis_cache_service,
        get_db_session
    )
    
    # 启动定时任务调度器
    if FallbackConfig.ENABLE_SCHEDULER:
        service.start_scheduler()
    
    return service


# ==================== 测试连接 ====================

def test_connections():
    """
    测试Redis和MySQL连接是否正常
    """
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    # 测试Redis连接
    try:
        client = get_redis_client()
        client.ping()
        logger.info("✓ Redis连接成功")
    except Exception as e:
        logger.error(f"✗ Redis连接失败: {e}")
        logger.error("请检查Redis配置和服务是否启动")
    
    # 测试MySQL连接
    try:
        engine = get_database_engine()
        with engine.connect() as conn:
            conn.execute("SELECT 1")
        logger.info("✓ MySQL连接成功")
    except Exception as e:
        logger.error(f"✗ MySQL连接失败: {e}")
        logger.error("请检查MySQL配置和服务是否启动")
    
    # 测试会话管理器
    try:
        with get_db_session() as session:
            session.execute("SELECT 1")
        logger.info("✓ 数据库会话管理器正常")
    except Exception as e:
        logger.error(f"✗ 会话管理器测试失败: {e}")


# ==================== 使用示例 ====================

if __name__ == "__main__":
    print("配置测试")
    print("="*60)
    
    # 显示当前配置
    print("\n当前配置:")
    print(f"Redis: {RedisConfig.HOST}:{RedisConfig.PORT}/{RedisConfig.DB}")
    print(f"MySQL: {MySQLConfig.HOST}:{MySQLConfig.PORT}/{MySQLConfig.DATABASE}")
    print(f"用户: {MySQLConfig.USER}")
    print()
    
    # 测试连接
    print("测试连接...")
    test_connections()
    
    print("\n" + "="*60)
    print("配置测试完成")
    print("\n下一步:")
    print("1. 如果连接失败，请修改配置")
    print("2. 如果连接成功，运行: python init_database.py 初始化数据库")
    print("3. 然后运行: python test_service.py --quick 快速测试")
