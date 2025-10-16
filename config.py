"""
配置文件
根据实际环境修改以下配置
"""

# Redis配置
REDIS_CONFIG = {
    'host': 'localhost',           # Redis主机地址
    'port': 6379,                  # Redis端口
    'db': 0,                       # Redis数据库编号
    'password': None,              # Redis密码，如果没有设置则为None
    'decode_responses': False,      # 保持为False，手动处理编码
    'socket_connect_timeout': 5,   # 连接超时（秒）
    'socket_timeout': 5,           # 操作超时（秒）
    'max_connections': 50,         # 最大连接数
}

# MySQL配置
MYSQL_CONFIG = {
    'connection_string': 'mysql+pymysql://username:password@localhost:3306/redis_gateway_info?charset=utf8mb4',
    # 格式说明：mysql+pymysql://用户名:密码@主机:端口/数据库名?charset=utf8mb4
}

# 健康检查配置
HEALTH_CHECK_CONFIG = {
    'check_interval': 10,          # Redis健康检查间隔（秒）
    'cleanup_interval': 60,        # 过期数据清理间隔（秒）
}

# 分布式锁配置
LOCK_CONFIG = {
    'default_timeout': 10,         # 默认锁超时时间（秒）
    'health_check_lock_key': 'redis_health_check_lock',
    'sync_lock_key': 'redis_mysql_sync_lock',
}

# 数据库连接池配置
DB_POOL_CONFIG = {
    'pool_size': 10,              # 连接池大小
    'max_overflow': 20,           # 最大溢出连接数
    'pool_pre_ping': True,        # 连接前ping测试
    'pool_recycle': 3600,         # 连接回收时间（秒）
}

# 日志配置
LOGGING_CONFIG = {
    'level': 'INFO',              # 日志级别: DEBUG, INFO, WARNING, ERROR
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'file': 'redis_gateway.log',  # 日志文件路径
}
