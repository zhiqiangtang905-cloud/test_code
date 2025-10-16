# -*- coding: utf-8 -*-
import json
import os
import threading
from typing import Any, Iterable, Optional

import redis


class RedisJSONClient:
    """
    Redis JSON 客户端包装：
    - 所有写入自动 json.dumps
    - 所有读取自动 json.loads
    - 提供必要的 KV/Hash/List API
    - 维护可用状态标记
    """

    def __init__(self, url: Optional[str] = None, decode_responses: bool = True):
        self._url = url or os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
        # decode_responses=True 让我们得到 str 而非 bytes
        self._client = redis.Redis.from_url(self._url, decode_responses=decode_responses)
        self._lock = threading.RLock()
        self.is_available = True

    # ---------- 基础能力 ----------
    def ping(self) -> bool:
        try:
            return bool(self._client.ping())
        except redis.RedisError:
            return False

    def mark_unavailable(self):
        with self._lock:
            self.is_available = False

    def mark_available(self):
        with self._lock:
            self.is_available = True

    # ---------- KV ----------
    def set(self, key: str, value: Any) -> bool:
        return bool(self._client.set(key, json.dumps(value)))

    def get(self, key: str) -> Any:
        v = self._client.get(key)
        if v is None:
            return None
        return json.loads(v)

    def setex(self, key: str, ttl: int, value: Any) -> bool:
        return bool(self._client.setex(key, ttl, json.dumps(value)))

    def expire(self, key: str, ttl: int) -> bool:
        return bool(self._client.expire(key, ttl))

    def exists(self, key: str) -> bool:
        return bool(self._client.exists(key))

    def delete(self, key: str) -> int:
        return int(self._client.delete(key))

    # ---------- Hash ----------
    def hset(self, name: str, key: str, value: Any) -> int:
        return int(self._client.hset(name, key, json.dumps(value)))

    def hget(self, name: str, key: str) -> Any:
        v = self._client.hget(name, key)
        if v is None:
            return None
        return json.loads(v)

    def hgetall(self, name: str) -> dict[str, Any]:
        data = self._client.hgetall(name)
        return {k: json.loads(v) for k, v in data.items()}

    def hdel(self, name: str, key: str) -> int:
        return int(self._client.hdel(name, key))

    # ---------- List ----------
    def lpush(self, name: str, *values: Any) -> int:
        encoded = [json.dumps(v) for v in values]
        return int(self._client.lpush(name, *encoded))

    def rpush(self, name: str, *values: Any) -> int:
        encoded = [json.dumps(v) for v in values]
        return int(self._client.rpush(name, *encoded))

    def lrange(self, name: str, start: int = 0, end: int = -1) -> list[Any]:
        arr = self._client.lrange(name, start, end)
        return [json.loads(v) for v in arr]

    def lpop(self, name: str, count: Optional[int] = None) -> Any:
        if count is None:
            v = self._client.lpop(name)
            return None if v is None else json.loads(v)
        else:
            arr = self._client.lpop(name, count=count)
            if arr is None:
                return None
            return [json.loads(v) for v in arr]

    def lindex(self, name: str, index: int) -> Any:
        v = self._client.lindex(name, index)
        return None if v is None else json.loads(v)

    def llen(self, name: str) -> int:
        return int(self._client.llen(name))

    def lrem(self, name: str, count: int, value: Any) -> int:
        return int(self._client.lrem(name, count, json.dumps(value)))

    # 别名（用户列出了大小写混用）
    def Lrange(self, name: str, start: int = 0, end: int = -1) -> list[Any]:
        return self.lrange(name, start, end)
