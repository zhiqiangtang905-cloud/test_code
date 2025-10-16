"""
数据库初始化脚本
用于创建redis_gateway_info表
"""
from sqlalchemy import create_engine
from redis_models import Base, RedisGatewayInfo
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def init_database(database_url: str):
    """
    初始化数据库，创建所需的表
    
    Args:
        database_url: 数据库连接URL
                     格式: mysql+pymysql://username:password@host:port/database
                     示例: mysql+pymysql://root:password@localhost:3306/mydb
    """
    try:
        logger.info(f"开始初始化数据库...")
        
        # 创建数据库引擎
        engine = create_engine(
            database_url,
            echo=True,  # 打印SQL语句
            pool_pre_ping=True,  # 连接池预检查
        )
        
        # 创建所有表
        Base.metadata.create_all(engine)
        
        logger.info("数据库表创建成功！")
        logger.info(f"已创建表: {RedisGatewayInfo.__tablename__}")
        
        # 验证表是否创建成功
        with engine.connect() as conn:
            result = conn.execute(
                f"SHOW TABLES LIKE '{RedisGatewayInfo.__tablename__}'"
            )
            if result.fetchone():
                logger.info("✓ 表结构验证成功")
            else:
                logger.error("✗ 表创建可能失败，请检查")
        
        return True
        
    except Exception as e:
        logger.error(f"数据库初始化失败: {e}")
        return False


def drop_tables(database_url: str):
    """
    删除所有表（谨慎使用）
    
    Args:
        database_url: 数据库连接URL
    """
    try:
        logger.warning("准备删除所有表...")
        
        engine = create_engine(database_url, echo=True)
        Base.metadata.drop_all(engine)
        
        logger.info("所有表已删除")
        return True
        
    except Exception as e:
        logger.error(f"删除表失败: {e}")
        return False


if __name__ == "__main__":
    # 配置你的数据库连接
    # 格式: mysql+pymysql://用户名:密码@主机:端口/数据库名
    DATABASE_URL = "mysql+pymysql://root:password@localhost:3306/mydb"
    
    # 使用提示
    print("=" * 60)
    print("Redis降级服务 - 数据库初始化脚本")
    print("=" * 60)
    print("\n请修改DATABASE_URL变量为你的数据库连接信息")
    print("格式: mysql+pymysql://用户名:密码@主机:端口/数据库名")
    print("\n示例:")
    print("  mysql+pymysql://root:123456@localhost:3306/mydb")
    print("\n")
    
    # 提示用户确认
    confirm = input("确认要初始化数据库吗？(yes/no): ")
    
    if confirm.lower() == 'yes':
        success = init_database(DATABASE_URL)
        if success:
            print("\n✓ 数据库初始化完成！")
            print("你现在可以使用RedisFallbackService了")
        else:
            print("\n✗ 数据库初始化失败，请检查错误信息")
    else:
        print("\n取消初始化")
    
    print("\n" + "=" * 60)
