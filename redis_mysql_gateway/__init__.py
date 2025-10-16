"""Redis-MySQL gateway package.

Exports:
- RedisMySQLGateway: High-level API mirroring Redis operations with MySQL fallback
- DistributedLock: Context-managed distributed lock (Redis first, MySQL fallback)
- CountingSemaphore: Cross-backend counting semaphore
- RedisHealthMonitor: Health checker with probe and cleanup scheduler
- RedisGatewayInfo: SQLAlchemy model representing the persistent store
- init_db, get_session: DB helpers
"""
from .gateway import RedisMySQLGateway
from .lock import DistributedLock, CountingSemaphore
from .health import RedisHealthMonitor
from .db import RedisGatewayInfo, init_db, get_session
