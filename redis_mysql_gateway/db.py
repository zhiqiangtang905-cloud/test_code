# -*- coding: utf-8 -*-
from datetime import datetime, timezone

# 本模块仅保留与时间、清理等通用逻辑；
# 连接与会话由外部注入的 get_db_session() 提供。


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def ensure_database_initialized(create_all_callable=None):
    """确保表结构已创建。
    由外部传入 create_all_callable(engine_or_bind) 来完成初始化；
    如果未提供，则跳过（假设外部已初始化）。
    """
    if create_all_callable is not None:
        create_all_callable()


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
