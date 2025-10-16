from __future__ import annotations

import asyncio
import random
import string
import time
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator, Iterator, Optional, Sequence

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Integer,
    MetaData,
    String,
    Table,
    and_,
    func,
    insert,
    select,
    update,
)
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import Select

try:
    # SQLAlchemy 2.x
    from sqlalchemy.ext.asyncio import AsyncEngine
except Exception:  # pragma: no cover - optional dependency in sync-only envs
    AsyncEngine = None  # type: ignore[assignment]


# ---------------------------
# Schema definition
# ---------------------------

_metadata = MetaData()

# Table-based counting semaphore. One row per permit slot.
# Primary key is (resource, slot_index) for portability across DBs.
semaphore_slots = Table(
    "semaphore_slots",
    _metadata,
    Column("resource", String(190), primary_key=True),
    Column("slot_index", Integer, primary_key=True),
    Column("holder_id", String(190), nullable=True),
    Column("acquired_at", DateTime(timezone=True), nullable=True),
    Column("lease_expires_at", DateTime(timezone=True), nullable=True),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

# Helpful index to speed up owner-scoped lookups
try:
    from sqlalchemy import Index

    Index("idx_semaphore_slots_owner", semaphore_slots.c.resource, semaphore_slots.c.holder_id)
except Exception:
    pass


# ---------------------------
# Utilities
# ---------------------------

@dataclass
class AcquireResult:
    acquired: int
    required: int


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _random_holder_id(prefix: str = "sem") -> str:
    token = "".join(random.choices(string.ascii_lowercase + string.digits, k=16))
    return f"{prefix}:{token}"


# ---------------------------
# Schema helpers
# ---------------------------

def ensure_schema(engine: Engine) -> None:
    """Create semaphore schema if missing (sync)."""
    _metadata.create_all(bind=engine)


async def ensure_schema_async(engine: AsyncEngine) -> None:  # type: ignore[valid-type]
    """Create semaphore schema if missing (async)."""
    async with engine.begin() as conn:  # type: ignore[attr-defined]
        await conn.run_sync(_metadata.create_all)


def _get_existing_slot_indexes(conn: Connection, resource: str) -> set[int]:
    rows = conn.execute(
        select(semaphore_slots.c.slot_index).where(semaphore_slots.c.resource == resource)
    ).all()
    return {r[0] for r in rows}


def ensure_capacity(engine: Engine, resource: str, capacity: int) -> None:
    """Ensure [0, capacity) slots exist for given resource (sync)."""
    if capacity <= 0:
        raise ValueError("capacity must be positive")
    ensure_schema(engine)
    with engine.begin() as conn:
        existing = _get_existing_slot_indexes(conn, resource)
        missing = [i for i in range(capacity) if i not in existing]
        if not missing:
            return
        batch = [
            {
                "resource": resource,
                "slot_index": i,
                "holder_id": None,
                "acquired_at": None,
                "lease_expires_at": None,
                "updated_at": _utcnow(),
            }
            for i in missing
        ]
        conn.execute(insert(semaphore_slots), batch)


async def ensure_capacity_async(engine: AsyncEngine, resource: str, capacity: int) -> None:  # type: ignore[valid-type]
    if capacity <= 0:
        raise ValueError("capacity must be positive")
    await ensure_schema_async(engine)
    async with engine.begin() as conn:  # type: ignore[attr-defined]
        existing = {
            r[0]
            for r in (await conn.execute(
                select(semaphore_slots.c.slot_index).where(semaphore_slots.c.resource == resource)
            )).all()
        }
        missing = [i for i in range(capacity) if i not in existing]
        if not missing:
            return
        batch = [
            {
                "resource": resource,
                "slot_index": i,
                "holder_id": None,
                "acquired_at": None,
                "lease_expires_at": None,
                "updated_at": _utcnow(),
            }
            for i in missing
        ]
        await conn.execute(insert(semaphore_slots), batch)


# ---------------------------
# Core acquisition logic (shared helpers)
# ---------------------------

def _free_slot_select(resource: str) -> Select:
    # Pick first free or expired slot; lock and skip already-locked rows to avoid blocking.
    return (
        select(semaphore_slots.c.slot_index)
        .where(
            and_(
                semaphore_slots.c.resource == resource,
                # free or expired
                (
                    (semaphore_slots.c.holder_id.is_(None))
                    | (semaphore_slots.c.lease_expires_at.is_not(None) & (semaphore_slots.c.lease_expires_at < _utcnow()))
                ),
            )
        )
        .order_by(semaphore_slots.c.slot_index.asc())
        .with_for_update(skip_locked=True)
        .limit(1)
    )


# ---------------------------
# Synchronous semaphore
# ---------------------------

class DbSemaphore:
    """
    Database-backed counting semaphore using SQLAlchemy (sync).

    - Cross-DB via table-backed slots and SELECT ... FOR UPDATE SKIP LOCKED
    - Supports TTL leases and renewal
    - Reentrant by holder_id (you hold multiple permits if you want)

    Usage:
        sem = DbSemaphore(engine, resource="video:transcode", capacity=5, ttl_seconds=30)
        with sem:
            # critical section
            ...
    """

    def __init__(
        self,
        engine: Engine,
        resource: str,
        capacity: int,
        ttl_seconds: int = 30,
        holder_id: Optional[str] = None,
    ) -> None:
        self.engine = engine
        self.resource = resource
        self.capacity = capacity
        self.ttl_seconds = ttl_seconds
        self.holder_id = holder_id or _random_holder_id()
        ensure_capacity(engine, resource, capacity)

    # ------------- acquisition API -------------

    def try_acquire(self, permits: int = 1) -> AcquireResult:
        if permits <= 0:
            raise ValueError("permits must be positive")
        acquired_total = 0
        now = _utcnow()
        lease_exp = now + timedelta(seconds=self.ttl_seconds)
        with self.engine.begin() as conn:
            for _ in range(permits):
                # Try lock a free/expired slot
                row = conn.execute(_free_slot_select(self.resource)).first()
                if not row:
                    break
                slot_index = int(row[0])
                upd = (
                    update(semaphore_slots)
                    .where(
                        and_(
                            semaphore_slots.c.resource == self.resource,
                            semaphore_slots.c.slot_index == slot_index,
                        )
                    )
                    .values(
                        holder_id=self.holder_id,
                        acquired_at=now,
                        lease_expires_at=lease_exp,
                        updated_at=now,
                    )
                )
                conn.execute(upd)
                acquired_total += 1
        return AcquireResult(acquired=acquired_total, required=permits)

    def acquire(
        self,
        permits: int = 1,
        timeout_seconds: Optional[float] = None,
        retry_interval_seconds: float = 0.2,
    ) -> bool:
        """Block until acquired required permits or timeout. Returns True if success."""
        deadline = None if timeout_seconds is None else time.monotonic() + max(0.0, timeout_seconds)
        acquired = 0
        while acquired < permits:
            res = self.try_acquire(permits - acquired)
            acquired += res.acquired
            if acquired >= permits:
                return True
            if deadline is not None and time.monotonic() >= deadline:
                return False
            time.sleep(retry_interval_seconds)
        return True

    def renew(self, additional_ttl_seconds: Optional[int] = None) -> int:
        """Extend lease for all permits held by this holder. Returns number of renewed permits."""
        now = _utcnow()
        ttl = self.ttl_seconds if additional_ttl_seconds is None else additional_ttl_seconds
        new_exp = now + timedelta(seconds=ttl)
        with self.engine.begin() as conn:
            res = conn.execute(
                update(semaphore_slots)
                .where(
                    and_(
                        semaphore_slots.c.resource == self.resource,
                        semaphore_slots.c.holder_id == self.holder_id,
                    )
                )
                .values(lease_expires_at=new_exp, updated_at=now)
            )
            return int(res.rowcount or 0)

    def release(self, permits: Optional[int] = None) -> int:
        """
        Release permits held by this holder. If permits is None, release all.
        Returns number of released permits.
        """
        now = _utcnow()
        with self.engine.begin() as conn:
            if permits is None:
                res = conn.execute(
                    update(semaphore_slots)
                    .where(
                        and_(
                            semaphore_slots.c.resource == self.resource,
                            semaphore_slots.c.holder_id == self.holder_id,
                        )
                    )
                    .values(holder_id=None, acquired_at=None, lease_expires_at=None, updated_at=now)
                )
                return int(res.rowcount or 0)
            # release specific count: choose newest first to reduce starvation
            held_rows = conn.execute(
                select(semaphore_slots.c.slot_index)
                .where(
                    and_(
                        semaphore_slots.c.resource == self.resource,
                        semaphore_slots.c.holder_id == self.holder_id,
                    )
                )
                .order_by(semaphore_slots.c.acquired_at.desc().nullslast())
                .limit(permits)
            ).all()
            released = 0
            for (slot_index,) in held_rows:
                conn.execute(
                    update(semaphore_slots)
                    .where(
                        and_(
                            semaphore_slots.c.resource == self.resource,
                            semaphore_slots.c.slot_index == slot_index,
                        )
                    )
                    .values(holder_id=None, acquired_at=None, lease_expires_at=None, updated_at=now)
                )
                released += 1
            return released

    def held_count(self) -> int:
        with self.engine.begin() as conn:
            rows = conn.execute(
                select(func.count())
                .select_from(semaphore_slots)
                .where(
                    and_(
                        semaphore_slots.c.resource == self.resource,
                        semaphore_slots.c.holder_id == self.holder_id,
                    )
                )
            ).scalar_one()
            return int(rows)

    # ------------- context manager -------------

    def __enter__(self) -> "DbSemaphore":
        ok = self.acquire(permits=1, timeout_seconds=None)
        if not ok:  # pragma: no cover - defensive
            raise TimeoutError("failed to acquire semaphore")
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: D401
        self.release(permits=1)


# ---------------------------
# Asynchronous semaphore
# ---------------------------

class AsyncDbSemaphore:
    """
    Async database-backed counting semaphore using SQLAlchemy (asyncio).

    Usage:
        sem = AsyncDbSemaphore(async_engine, resource="video", capacity=5, ttl_seconds=30)
        async with sem:
            ...
    """

    def __init__(
        self,
        engine: AsyncEngine,  # type: ignore[valid-type]
        resource: str,
        capacity: int,
        ttl_seconds: int = 30,
        holder_id: Optional[str] = None,
    ) -> None:
        if AsyncDbSemaphore._is_none(engine):  # pragma: no cover - guard for sync-only envs
            raise RuntimeError("Async SQLAlchemy not available. Install sqlalchemy[asyncio].")
        self.engine = engine
        self.resource = resource
        self.capacity = capacity
        self.ttl_seconds = ttl_seconds
        self.holder_id = holder_id or _random_holder_id()
        # best-effort schema bootstrap
        # use background task style awaitable for capacity ensure upon first use
        self._bootstrap_done = False

    @staticmethod
    def _is_none(engine: Optional[AsyncEngine]) -> bool:  # type: ignore[valid-type]
        return engine is None  # type: ignore[return-value]

    async def _ensure_bootstrap(self) -> None:
        if self._bootstrap_done:
            return
        await ensure_capacity_async(self.engine, self.resource, self.capacity)  # type: ignore[arg-type]
        self._bootstrap_done = True

    # ------------- acquisition API -------------

    async def try_acquire(self, permits: int = 1) -> AcquireResult:
        if permits <= 0:
            raise ValueError("permits must be positive")
        await self._ensure_bootstrap()
        acquired_total = 0
        now = _utcnow()
        lease_exp = now + timedelta(seconds=self.ttl_seconds)
        async with self.engine.begin() as conn:  # type: ignore[attr-defined]
            for _ in range(permits):
                row = (await conn.execute(_free_slot_select(self.resource))).first()
                if not row:
                    break
                slot_index = int(row[0])
                await conn.execute(
                    update(semaphore_slots)
                    .where(
                        and_(
                            semaphore_slots.c.resource == self.resource,
                            semaphore_slots.c.slot_index == slot_index,
                        )
                    )
                    .values(
                        holder_id=self.holder_id,
                        acquired_at=now,
                        lease_expires_at=lease_exp,
                        updated_at=now,
                    )
                )
                acquired_total += 1
        return AcquireResult(acquired=acquired_total, required=permits)

    async def acquire(
        self,
        permits: int = 1,
        timeout_seconds: Optional[float] = None,
        retry_interval_seconds: float = 0.2,
    ) -> bool:
        deadline = None if timeout_seconds is None else time.monotonic() + max(0.0, timeout_seconds)
        acquired = 0
        while acquired < permits:
            res = await self.try_acquire(permits - acquired)
            acquired += res.acquired
            if acquired >= permits:
                return True
            if deadline is not None and time.monotonic() >= deadline:
                return False
            await asyncio.sleep(retry_interval_seconds)
        return True

    async def renew(self, additional_ttl_seconds: Optional[int] = None) -> int:
        await self._ensure_bootstrap()
        now = _utcnow()
        ttl = self.ttl_seconds if additional_ttl_seconds is None else additional_ttl_seconds
        new_exp = now + timedelta(seconds=ttl)
        async with self.engine.begin() as conn:  # type: ignore[attr-defined]
            res = await conn.execute(
                update(semaphore_slots)
                .where(
                    and_(
                        semaphore_slots.c.resource == self.resource,
                        semaphore_slots.c.holder_id == self.holder_id,
                    )
                )
                .values(lease_expires_at=new_exp, updated_at=now)
            )
            return int(res.rowcount or 0)

    async def release(self, permits: Optional[int] = None) -> int:
        await self._ensure_bootstrap()
        now = _utcnow()
        async with self.engine.begin() as conn:  # type: ignore[attr-defined]
            if permits is None:
                res = await conn.execute(
                    update(semaphore_slots)
                    .where(
                        and_(
                            semaphore_slots.c.resource == self.resource,
                            semaphore_slots.c.holder_id == self.holder_id,
                        )
                    )
                    .values(holder_id=None, acquired_at=None, lease_expires_at=None, updated_at=now)
                )
                return int(res.rowcount or 0)
            held_rows = (
                await conn.execute(
                    select(semaphore_slots.c.slot_index)
                    .where(
                        and_(
                            semaphore_slots.c.resource == self.resource,
                            semaphore_slots.c.holder_id == self.holder_id,
                        )
                    )
                    .order_by(semaphore_slots.c.acquired_at.desc().nullslast())
                    .limit(permits)
                )
            ).all()
            released = 0
            for (slot_index,) in held_rows:
                await conn.execute(
                    update(semaphore_slots)
                    .where(
                        and_(
                            semaphore_slots.c.resource == self.resource,
                            semaphore_slots.c.slot_index == slot_index,
                        )
                    )
                    .values(holder_id=None, acquired_at=None, lease_expires_at=None, updated_at=now)
                )
                released += 1
            return released

    async def held_count(self) -> int:
        await self._ensure_bootstrap()
        async with self.engine.begin() as conn:  # type: ignore[attr-defined]
            rows = (
                await conn.execute(
                    select(func.count())
                    .select_from(semaphore_slots)
                    .where(
                        and_(
                            semaphore_slots.c.resource == self.resource,
                            semaphore_slots.c.holder_id == self.holder_id,
                        )
                    )
                )
            ).scalar_one()
            return int(rows)

    # ------------- async context manager -------------

    async def __aenter__(self) -> "AsyncDbSemaphore":
        ok = await self.acquire(permits=1, timeout_seconds=None)
        if not ok:  # pragma: no cover - defensive
            raise TimeoutError("failed to acquire semaphore")
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # noqa: D401
        await self.release(permits=1)


__all__ = [
    "DbSemaphore",
    "AsyncDbSemaphore",
    "ensure_schema",
    "ensure_schema_async",
    "ensure_capacity",
    "ensure_capacity_async",
    "AcquireResult",
]
