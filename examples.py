from __future__ import annotations

import asyncio
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine

from db_semaphore import DbSemaphore, AsyncDbSemaphore


# Adjust DSNs as needed for your environment before running
SYNC_DSN = "sqlite+pysqlite:///./test.db"  # works for demo; SKIP LOCKED emulated by SQLite but acceptable for local tests
ASYNC_DSN = "sqlite+aiosqlite:///./test.db"


def sync_demo() -> None:
    engine = create_engine(SYNC_DSN, future=True)
    sem = DbSemaphore(engine, resource="demo", capacity=2, ttl_seconds=10)
    with sem:
        print("Acquired sync permit; held:", sem.held_count())
    print("Released sync permit; held:", sem.held_count())


async def async_demo() -> None:
    engine = create_async_engine(ASYNC_DSN, future=True)
    sem = AsyncDbSemaphore(engine, resource="demo", capacity=2, ttl_seconds=10)
    async with sem:
        print("Acquired async permit; held:", await sem.held_count())
    print("Released async permit; held:", await sem.held_count())


if __name__ == "__main__":
    sync_demo()
    asyncio.run(async_demo())
