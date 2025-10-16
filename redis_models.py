"""
Redis降级MySQL的数据库模型
用于存储Redis数据的MySQL表结构
"""
from sqlalchemy import Column, String, DateTime, Text, Index
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()


class RedisGatewayInfo(Base):
    """
    Redis数据降级存储表
    gateway在此处表示Redis和MySQL之间的网关/代理层，用于数据降级和恢复
    """
    __tablename__ = 'redis_gateway_info'
    
    # key和name作为联合主键，用于唯一标识一条记录
    key = Column(String(255), primary_key=True, nullable=False, comment='Redis键名')
    name = Column(String(255), primary_key=True, nullable=False, default='', comment='Redis hash/list等的name字段')
    
    # value存储JSON序列化后的字符串
    value = Column(Text, nullable=True, comment='存储json.dumps()后的值')
    
    # 过期时间，用于定时清理
    expire_time = Column(DateTime, nullable=True, comment='数据过期时间')
    
    # 最后更新时间
    last_update_time = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment='最后更新时间')
    
    def __repr__(self):
        return f"<RedisGatewayInfo(key={self.key}, name={self.name})>"


# 创建索引以提升查询性能
Index('idx_expire_time', RedisGatewayInfo.expire_time)
Index('idx_last_update', RedisGatewayInfo.last_update_time)
