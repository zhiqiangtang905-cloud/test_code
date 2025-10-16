# -*- coding: utf-8 -*-
"""
Redis和MySQL配置文件
"""

# Redis配置
REDIS_CONFIG = {
    'host': 'localhost',
    'port': 6379,
    'db': 0,
    'password': None,
    'decode_responses': True,
    'socket_timeout': 5,
    'socket_connect_timeout': 5,
}

# MySQL配置
MYSQL_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'your_password',
    'database': 'redis_gateway_info',
    'charset': 'utf8mb4',
}

# 健康拨测配置
HEALTH_CHECK_CONFIG = {
    'probe_interval': 10,  # Redis健康拨测间隔（秒）
    'cleanup_interval': 60,  # 过期数据清理间隔（秒）
    'lock_timeout': 300,  # 分布式锁超时时间（秒）
    'lock_key': 'redis_gateway:health_check:lock',  # 健康拨测锁的key
    'cleanup_lock_key': 'redis_gateway:cleanup:lock',  # 清理任务锁的key
}
