from __future__ import annotations
"""SQLAlchemy engine, session and model for persisting Redis-like data.

Schema summary (table: redis_gateway_info):
- key: varchar primary key component (e.g., field for hash, '__list__' for lists, actual key for strings)
- name: varchar primary key component (e.g., hash name, list name, or '_kv_' for simple string keys)
- value: JSON-serialized string payload. For lists, the entire list JSON array is stored.
- expire_time: optional timezone-aware datetime indicating when the record expires
- last_update_time: timezone-aware datetime for auditing

Primary key: (key, name)

Design mapping:
- Strings: name = '_kv_', key = <string_key>
- Hashes: name = <hash_name>, key = <field>
- Lists:  name = <list_name>, key = '__list__', value is JSON array
- Locks:  name = '__lock__:<lock_name>', key = 'token', value is lock token
- Semaphores: name = '__sema__:<sema_name>', key = 'count', value is integer as JSON
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Generator, Optional

from sqlalchemy import (
    create_engine,
    String,
    Text,
    DateTime,
    PrimaryKeyConstraint,
)
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from sqlalchemy import Column

from .config import DB_URL

Base = declarative_base()


class RedisGatewayInfo(Base):
    __tablename__ = "redis_gateway_info"

    key = Column(String(255), nullable=False)
    name = Column(String(255), nullable=False)
    value = Column(Text, nullable=True)
    expire_time = Column(DateTime(timezone=True), nullable=True)
    last_update_time = Column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint("key", "name", name="pk_redis_gateway_info"),
    )


_engine = create_engine(DB_URL, pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=_engine, autoflush=False, autocommit=False, future=True)


def init_db() -> None:
    """Create tables if they don't exist."""
    Base.metadata.create_all(_engine)


@dataclass
class SessionManager:
    """Context manager for SQLAlchemy sessions."""

    def __enter__(self) -> Session:
        self.session = SessionLocal()
        return self.session

    def __exit__(self, exc_type, exc, tb) -> None:
        try:
            if exc is None:
                self.session.commit()
            else:
                self.session.rollback()
        finally:
            self.session.close()


def get_session() -> SessionManager:
    """Get a context-managed SQLAlchemy session.

    Usage:
        with get_session() as session:
            ...
    """
    return SessionManager()
