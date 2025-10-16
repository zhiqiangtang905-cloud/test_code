# Redis-MySQL Gateway (Degrade and Health Monitor)

This module provides Redis operations with MySQL fallback and background health monitoring. It ensures only one instance executes cluster-wide logical jobs by using a distributed lock.

Key features:
- Redis-first writes with async MySQL persistence; on Redis failure, degrade to MySQL
- Reads prioritize Redis; on Redis error, fallback to MySQL after cleaning expired rows
- Cross-backend distributed lock and counting semaphore
- Automatic health probe every 10s when Redis is down; database cleanup every 60s
- On Redis recovery, data is restored from MySQL back into Redis

See `redis_mysql_gateway/` for implementation and in-code comments.
