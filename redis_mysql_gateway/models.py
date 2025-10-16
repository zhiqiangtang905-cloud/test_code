# -*- coding: utf-8 -*-
from datetime import datetime
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import String, Text, DateTime, Index
from sqlalchemy.sql import func


class Base(DeclarativeBase):
    pass


class RedisGatewayInfo(Base):
    __tablename__ = "redis_gateway_info"

    # key 与 name 组成联合主键
    key: Mapped[str] = mapped_column(String(255), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), primary_key=True, default="")

    # 存放 json.dumps 后的字符串
    value: Mapped[str | None] = mapped_column(Text, nullable=True)

    # 过期时间（UTC）
    expire_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # 最后更新时间
    last_update_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    __table_args__ = (
        Index("idx_expire_time", "expire_time"),
        Index("idx_name", "name"),
        Index("idx_key", "key"),
    )
