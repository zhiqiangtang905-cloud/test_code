"""
Redis网关数据库模型
用于存储Redis降级时的数据
"""
from sqlalchemy import Column, String, DateTime, Index, text
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()


class RedisGatewayInfo(Base):
    """
    Redis网关信息表
    用于在Redis不可用时存储数据
    
    表结构：
    - key: Redis的key，联合主键
    - name: Redis的hash name或list name，联合主键
    - value: 存储的值，使用json.dumps()序列化后的字符串
    - expire_time: 过期时间
    - last_update_time: 最后更新时间
    """
    __tablename__ = 'redis_gateway_info'
    
    # 联合主键：key和name
    key = Column(String(255), primary_key=True, nullable=False, comment='Redis的key')
    name = Column(String(255), primary_key=True, nullable=False, default='', comment='Redis的hash name或list name')
    value = Column(String(65535), nullable=True, comment='存储的值(JSON序列化)')
    expire_time = Column(DateTime, nullable=True, comment='过期时间')
    last_update_time = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment='最后更新时间')
    
    # 创建索引以提高查询效率
    __table_args__ = (
        Index('idx_expire_time', 'expire_time'),
        Index('idx_last_update_time', 'last_update_time'),
        {'mysql_charset': 'utf8mb4', 'mysql_collate': 'utf8mb4_unicode_ci'}
    )
    
    def __repr__(self):
        return f"<RedisGatewayInfo(key='{self.key}', name='{self.name}', expire_time='{self.expire_time}')>"
