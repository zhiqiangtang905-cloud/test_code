# -*- coding: utf-8 -*-
"""
数据库模型定义
"""
from sqlalchemy import Column, String, DateTime, Text, Index, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

Base = declarative_base()


class RedisGatewayInfo(Base):
    """
    Redis网关信息表
    存储Redis中的键值对数据，用于Redis降级时的数据存储
    
    字段说明：
    - key: Redis的key（主键之一）
    - name: Redis的name/field（主键之一）
    - value: 存储的值（JSON字符串）
    - expire_time: 过期时间
    - last_update_time: 最后更新时间
    """
    __tablename__ = 'redis_gateway_info'
    
    # key和name作为联合主键
    key = Column(String(255), primary_key=True, nullable=False, comment='Redis的key')
    name = Column(String(255), primary_key=True, nullable=False, default='', comment='Redis的name/field')
    value = Column(Text, nullable=True, comment='存储的值（JSON字符串）')
    expire_time = Column(DateTime, nullable=True, comment='过期时间')
    last_update_time = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment='最后更新时间')
    
    # 创建索引以提高查询性能
    __table_args__ = (
        Index('idx_expire_time', 'expire_time'),
        Index('idx_last_update_time', 'last_update_time'),
    )
    
    def __repr__(self):
        return f"<RedisGatewayInfo(key='{self.key}', name='{self.name}', value='{self.value}')>"


class DatabaseManager:
    """
    数据库管理器
    负责创建和管理数据库连接
    """
    
    def __init__(self, mysql_config):
        """
        初始化数据库管理器
        
        Args:
            mysql_config: MySQL配置字典
        """
        # 构建数据库连接URL
        db_url = (
            f"mysql+pymysql://{mysql_config['user']}:{mysql_config['password']}"
            f"@{mysql_config['host']}:{mysql_config['port']}"
            f"/{mysql_config['database']}?charset={mysql_config['charset']}"
        )
        
        # 创建数据库引擎
        self.engine = create_engine(
            db_url,
            pool_size=10,  # 连接池大小
            max_overflow=20,  # 超过连接池大小时最多可以创建的连接数
            pool_pre_ping=True,  # 每次连接前先ping，确保连接可用
            pool_recycle=3600,  # 连接回收时间（秒）
            echo=False  # 是否输出SQL语句
        )
        
        # 创建会话工厂
        self.SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=self.engine
        )
    
    def create_tables(self):
        """
        创建数据库表
        """
        Base.metadata.create_all(bind=self.engine)
    
    def get_session(self):
        """
        获取数据库会话
        
        Returns:
            数据库会话对象
        """
        return self.SessionLocal()
    
    def close(self):
        """
        关闭数据库连接
        """
        self.engine.dispose()
