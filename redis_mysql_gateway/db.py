# -*- coding: utf-8 -*-
import os
import contextlib
from datetime import datetime, timezone
from typing import Iterator, Optional

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, scoped_session


# 默认的数据库名为 redis_gateway_info
# 可通过环境变量 DATABASE_URL 覆盖，例如：
# mysql+pymysql://user:password@host:3306/redis_gateway_info
DEFAULT_DB_URL = os.getenv(
    "DATABASE_URL",
    "mysql+pymysql://root:password@127.0.0.1:3306/redis_gateway_info",
)

# 为了在多线程异步写入时提升复用，使用连接池
engine = create_engine(
    DEFAULT_DB_URL,
    pool_pre_ping=True,
    pool_recycle=1800,
    future=True,
)

SessionLocal = scoped_session(
    sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False, future=True)
)


@contextlib.contextmanager
def get_session() -> Iterator:
    """获取一个 SQLAlchemy 会话，自动提交或回滚。"""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def ensure_database_initialized():
    """确保表结构已创建。"""
    from .models import Base  # 延迟导入，避免循环依赖
    Base.metadata.create_all(bind=engine)


def cleanup_expired(session) -> int:
    """
    清理过期数据：
    - 删除超时的 __meta__ 元数据
    - 删除过期的 KV/List 主行（key=目标key, name=''）
    - 删除过期的 Hash 字段（name=hash_name 的行）
    - 对过期的元数据，联动删除其子项

    返回删除行数总计。
    """
    from sqlalchemy import select, delete, and_, or_
    from .models import RedisGatewayInfo

    now = utc_now()
    total_deleted = 0

    # 1) 过期的 meta 行
    expired_meta_stmt = select(RedisGatewayInfo.name).where(
        and_(RedisGatewayInfo.key == "__meta__", RedisGatewayInfo.expire_time.is_not(None), RedisGatewayInfo.expire_time <= now)
    )
    expired_meta_names = [row[0] for row in session.execute(expired_meta_stmt).all()]

    if expired_meta_names:
        # 删除这些 meta
        del_meta = delete(RedisGatewayInfo).where(
            and_(RedisGatewayInfo.key == "__meta__", RedisGatewayInfo.name.in_(expired_meta_names))
        )
        total_deleted += session.execute(del_meta).rowcount or 0

        # 删除子项：
        # - KV/List: key=name, name=''
        # - Hash: name=name
        del_kv_list = delete(RedisGatewayInfo).where(
            and_(RedisGatewayInfo.name == "", RedisGatewayInfo.key.in_(expired_meta_names))
        )
        total_deleted += session.execute(del_kv_list).rowcount or 0

        del_hash_children = delete(RedisGatewayInfo).where(
            RedisGatewayInfo.name.in_(expired_meta_names)
        )
        total_deleted += session.execute(del_hash_children).rowcount or 0

    # 2) 删除自身过期的数据行（非 meta）
    del_expired_rows = delete(RedisGatewayInfo).where(
        and_(
            RedisGatewayInfo.key != "__meta__",
            RedisGatewayInfo.expire_time.is_not(None),
            RedisGatewayInfo.expire_time <= now,
        )
    )
    total_deleted += session.execute(del_expired_rows).rowcount or 0

    return total_deleted
