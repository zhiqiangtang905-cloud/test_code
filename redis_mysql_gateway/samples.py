# -*- coding: utf-8 -*-
"""
使用示例（非运行必需）：

from redis_mysql_gateway import RedisMySQLGateway, HealthMonitor

gateway = RedisMySQLGateway()
health = HealthMonitor(gateway)
gateway.attach_health_monitor(health)
health.start()

# 写入
gateway.set("k1", {"a": 1})
gateway.set_ex("k2", [1,2,3], ttl=30)

gateway.hset("user:1", "name", "Tom", ttl=60)
print(gateway.hget("user:1", "name"))

# 列表
gateway.lpush("list:jobs", {"id":1})
gateway.rpush("list:jobs", {"id":2})
print(gateway.lrange("list:jobs", 0, -1))

"""
