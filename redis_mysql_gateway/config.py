"""Basic configuration for Redis-MySQL gateway.

Environment variables:
- DB_URL: SQLAlchemy database URL. Example for MySQL (recommended):
  mysql+pymysql://USER:PASSWORD@HOST:3306/redis_gateway_info
  Defaults to local MySQL placeholder; change per your environment.

- REDIS_URL: Redis connection URL, e.g. redis://localhost:6379/0
- INSTANCE_ID: Unique identifier for this process instance (used for locks)
"""
from __future__ import annotations
import os
import uuid as _uuid

DB_URL: str = os.getenv(
    "DB_URL",
    # Default to MySQL DSN name requested by user; replace credentials accordingly
    "mysql+pymysql://user:password@localhost:3306/redis_gateway_info",
)

REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# Unique token for this process, used for safe lock releases
INSTANCE_ID: str = os.getenv("INSTANCE_ID", str(_uuid.uuid4()))
