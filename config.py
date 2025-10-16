"""
配置文件
用于配置Redis降级服务的各项参数
"""
import os


class Config:
    """Redis降级服务配置类"""
    
    # ==================== Redis配置 ====================
    REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
    REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
    REDIS_DB = int(os.getenv('REDIS_DB', 0))
    REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', None)
    REDIS_SOCKET_TIMEOUT = int(os.getenv('REDIS_SOCKET_TIMEOUT', 2))  # 连接超时（秒）
    REDIS_SOCKET_CONNECT_TIMEOUT = int(os.getenv('REDIS_SOCKET_CONNECT_TIMEOUT', 2))  # 建立连接超时（秒）
    
    # ==================== MySQL配置 ====================
    MYSQL_HOST = os.getenv('MYSQL_HOST', 'localhost')
    MYSQL_PORT = int(os.getenv('MYSQL_PORT', 3306))
    MYSQL_USER = os.getenv('MYSQL_USER', 'root')
    MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', 'password')
    MYSQL_DATABASE = os.getenv('MYSQL_DATABASE', 'redis_gateway_info')
    MYSQL_CHARSET = os.getenv('MYSQL_CHARSET', 'utf8mb4')
    
    # 数据库连接池配置
    MYSQL_POOL_SIZE = int(os.getenv('MYSQL_POOL_SIZE', 10))  # 连接池大小
    MYSQL_MAX_OVERFLOW = int(os.getenv('MYSQL_MAX_OVERFLOW', 20))  # 最大溢出连接数
    MYSQL_POOL_RECYCLE = int(os.getenv('MYSQL_POOL_RECYCLE', 3600))  # 连接回收时间（秒）
    MYSQL_POOL_PRE_PING = os.getenv('MYSQL_POOL_PRE_PING', 'true').lower() == 'true'  # 是否在获取连接前ping
    
    # ==================== 健康检查配置 ====================
    # 健康拨测间隔（秒）- 当Redis不可用时，每隔多少秒拨测一次
    HEALTH_CHECK_INTERVAL = int(os.getenv('HEALTH_CHECK_INTERVAL', 10))
    
    # 数据清理间隔（秒）- 每隔多少秒清理一次过期数据
    CLEANUP_INTERVAL = int(os.getenv('CLEANUP_INTERVAL', 60))
    
    # 分布式锁TTL（秒）- 分布式锁的过期时间
    LOCK_TTL = int(os.getenv('LOCK_TTL', 30))
    
    # ==================== 日志配置 ====================
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FORMAT = os.getenv(
        'LOG_FORMAT',
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # ==================== 性能配置 ====================
    # 是否启用异步写入MySQL（建议开启以提升性能）
    ASYNC_MYSQL_WRITE = os.getenv('ASYNC_MYSQL_WRITE', 'true').lower() == 'true'
    
    # 异步写入超时时间（秒）
    ASYNC_WRITE_TIMEOUT = int(os.getenv('ASYNC_WRITE_TIMEOUT', 5))
    
    @classmethod
    def get_mysql_url(cls) -> str:
        """
        获取MySQL连接URL
        
        Returns:
            str: SQLAlchemy连接URL
        """
        return (
            f"mysql+pymysql://{cls.MYSQL_USER}:{cls.MYSQL_PASSWORD}"
            f"@{cls.MYSQL_HOST}:{cls.MYSQL_PORT}/{cls.MYSQL_DATABASE}"
            f"?charset={cls.MYSQL_CHARSET}"
        )
    
    @classmethod
    def get_redis_connection_kwargs(cls) -> dict:
        """
        获取Redis连接参数
        
        Returns:
            dict: Redis连接参数
        """
        kwargs = {
            'host': cls.REDIS_HOST,
            'port': cls.REDIS_PORT,
            'db': cls.REDIS_DB,
            'decode_responses': True,
            'socket_timeout': cls.REDIS_SOCKET_TIMEOUT,
            'socket_connect_timeout': cls.REDIS_SOCKET_CONNECT_TIMEOUT,
        }
        
        if cls.REDIS_PASSWORD:
            kwargs['password'] = cls.REDIS_PASSWORD
        
        return kwargs
    
    @classmethod
    def validate(cls):
        """
        验证配置是否正确
        
        Raises:
            ValueError: 如果配置不正确
        """
        # 验证必填配置
        if not cls.MYSQL_PASSWORD or cls.MYSQL_PASSWORD == 'password':
            raise ValueError(
                "请设置MySQL密码：MYSQL_PASSWORD环境变量或修改config.py中的默认值"
            )
        
        # 验证端口范围
        if not (1 <= cls.REDIS_PORT <= 65535):
            raise ValueError(f"Redis端口无效: {cls.REDIS_PORT}")
        
        if not (1 <= cls.MYSQL_PORT <= 65535):
            raise ValueError(f"MySQL端口无效: {cls.MYSQL_PORT}")
        
        # 验证时间间隔
        if cls.HEALTH_CHECK_INTERVAL < 1:
            raise ValueError("健康检查间隔必须大于0秒")
        
        if cls.CLEANUP_INTERVAL < 1:
            raise ValueError("清理间隔必须大于0秒")
    
    @classmethod
    def print_config(cls):
        """打印当前配置（隐藏敏感信息）"""
        print("=" * 60)
        print("Redis降级服务配置")
        print("=" * 60)
        print("\nRedis配置:")
        print(f"  主机: {cls.REDIS_HOST}")
        print(f"  端口: {cls.REDIS_PORT}")
        print(f"  数据库: {cls.REDIS_DB}")
        print(f"  密码: {'***' if cls.REDIS_PASSWORD else '未设置'}")
        print(f"  连接超时: {cls.REDIS_SOCKET_TIMEOUT}秒")
        
        print("\nMySQL配置:")
        print(f"  主机: {cls.MYSQL_HOST}")
        print(f"  端口: {cls.MYSQL_PORT}")
        print(f"  用户: {cls.MYSQL_USER}")
        print(f"  密码: ***")
        print(f"  数据库: {cls.MYSQL_DATABASE}")
        print(f"  连接池大小: {cls.MYSQL_POOL_SIZE}")
        print(f"  最大溢出: {cls.MYSQL_MAX_OVERFLOW}")
        
        print("\n健康检查配置:")
        print(f"  拨测间隔: {cls.HEALTH_CHECK_INTERVAL}秒")
        print(f"  清理间隔: {cls.CLEANUP_INTERVAL}秒")
        print(f"  锁TTL: {cls.LOCK_TTL}秒")
        
        print("\n性能配置:")
        print(f"  异步MySQL写入: {'开启' if cls.ASYNC_MYSQL_WRITE else '关闭'}")
        print(f"  异步写入超时: {cls.ASYNC_WRITE_TIMEOUT}秒")
        
        print("\n日志配置:")
        print(f"  日志级别: {cls.LOG_LEVEL}")
        print("=" * 60)


# 开发环境配置
class DevelopmentConfig(Config):
    """开发环境配置"""
    LOG_LEVEL = 'DEBUG'
    MYSQL_POOL_SIZE = 5
    MYSQL_MAX_OVERFLOW = 10


# 生产环境配置
class ProductionConfig(Config):
    """生产环境配置"""
    LOG_LEVEL = 'WARNING'
    MYSQL_POOL_SIZE = 20
    MYSQL_MAX_OVERFLOW = 40
    HEALTH_CHECK_INTERVAL = 10
    CLEANUP_INTERVAL = 60


# 测试环境配置
class TestingConfig(Config):
    """测试环境配置"""
    LOG_LEVEL = 'DEBUG'
    REDIS_DB = 15  # 使用独立的测试数据库
    MYSQL_DATABASE = 'redis_gateway_info_test'
    HEALTH_CHECK_INTERVAL = 5
    CLEANUP_INTERVAL = 30


# 根据环境变量选择配置
ENV = os.getenv('APP_ENV', 'development').lower()

if ENV == 'production':
    config = ProductionConfig
elif ENV == 'testing':
    config = TestingConfig
else:
    config = DevelopmentConfig


# 导出当前配置
__all__ = ['config', 'Config', 'DevelopmentConfig', 'ProductionConfig', 'TestingConfig']
