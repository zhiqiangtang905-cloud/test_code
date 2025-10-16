# -*- coding: utf-8 -*-
"""
使用示例（非运行必需）：

from redis_mysql_gateway import RedisMySQLService, HealthMonitor


# 你的项目中已有：
# from your_project import get_redis_cache_service, get_db_session

def get_redis_cache_service():
    import redis
    return redis.Redis.from_url("redis://127.0.0.1:6379/0", decode_responses=True)


from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

engine = create_engine("mysql+pymysql://root:password@127.0.0.1:3306/redis_gateway_info", future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False, future=True)


@contextmanager
def get_db_session():
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


service = RedisMySQLService(get_redis_cache_service, get_db_session)
health = HealthMonitor(service)
service.attach_health_monitor(health)
health.start()

# 写入
service.set("k1", {"a": 1})
service.set_ex("k2", [1,2,3], ttl=30)

service.hset("user:1", "name", "Tom", ttl=60)
print(service.hget("user:1", "name"))

# 列表
service.lpush("list:jobs", {"id":1})
service.rpush("list:jobs", {"id":2})
print(service.lrange("list:jobs", 0, -1))

"""
