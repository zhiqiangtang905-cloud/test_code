from __future__ import annotations
"""Example usage of Redis-MySQL gateway and health monitor.

Adjust DB_URL/REDIS_URL via environment variables accordingly.
"""

import os
import time
import json
from redis import Redis

from redis_mysql_gateway.config import REDIS_URL
from redis_mysql_gateway.db import init_db
from redis_mysql_gateway.gateway import RedisMySQLGateway, RedisState
from redis_mysql_gateway.health import RedisHealthMonitor


def main():
    init_db()
    redis_client = Redis.from_url(REDIS_URL, decode_responses=False)
    state = RedisState()
    gateway = RedisMySQLGateway(redis_client, state)

    # Start health monitor (background thread)
    monitor = RedisHealthMonitor(redis_client, state)

    # KV set/get
    gateway.set("greeting", {"hello": "world"})
    print("get greeting:", gateway.get("greeting"))

    # Hash set/get
    gateway.hset("user:1", "name", "alice")
    print("hget user:1 name:", gateway.hget("user:1", "name"))

    # List ops
    gateway.rpush("jobs", {"id": 1})
    gateway.lpush("jobs", {"id": 0})
    print("lrange jobs:", gateway.lrange("jobs", 0, -1))
    print("lpop jobs 1:", gateway.lpop("jobs", 1))

    # Simulate long-running process to let scheduler run
    print("Running... press Ctrl+C to exit")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
