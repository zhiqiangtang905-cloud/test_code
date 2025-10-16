"""
数据库表初始化脚本
用于创建Redis降级服务所需的数据库表
"""
import logging
import sys
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from models import Base, RedisGatewayInfo

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_database(db_url: str, database_name: str):
    """
    创建数据库（如果不存在）
    
    Args:
        db_url: 数据库连接URL（不包含数据库名）
        database_name: 数据库名称
    """
    try:
        # 连接到MySQL（不指定数据库）
        engine = create_engine(db_url)
        
        with engine.connect() as conn:
            # 检查数据库是否存在
            result = conn.execute(
                text(f"SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = :db_name"),
                {"db_name": database_name}
            )
            
            if result.fetchone():
                logger.info(f"数据库 '{database_name}' 已存在")
            else:
                # 创建数据库
                conn.execute(
                    text(f"CREATE DATABASE `{database_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
                )
                conn.commit()
                logger.info(f"数据库 '{database_name}' 创建成功")
        
        engine.dispose()
        return True
        
    except SQLAlchemyError as e:
        logger.error(f"创建数据库失败: {e}")
        return False


def create_tables(db_url: str):
    """
    创建所有必要的表
    
    Args:
        db_url: 完整的数据库连接URL
    """
    try:
        # 创建引擎
        engine = create_engine(db_url, echo=True)
        
        logger.info("开始创建数据库表...")
        
        # 创建所有表
        Base.metadata.create_all(engine)
        
        logger.info("=" * 60)
        logger.info("数据库表创建成功！")
        logger.info("=" * 60)
        logger.info(f"表名: {RedisGatewayInfo.__tablename__}")
        logger.info("表结构:")
        logger.info("  - key: VARCHAR(255) [主键]")
        logger.info("  - name: VARCHAR(255) [主键]")
        logger.info("  - value: VARCHAR(65535)")
        logger.info("  - expire_time: DATETIME")
        logger.info("  - last_update_time: DATETIME")
        logger.info("=" * 60)
        
        engine.dispose()
        return True
        
    except SQLAlchemyError as e:
        logger.error(f"创建表失败: {e}", exc_info=True)
        return False


def verify_tables(db_url: str):
    """
    验证表是否创建成功
    
    Args:
        db_url: 完整的数据库连接URL
    """
    try:
        engine = create_engine(db_url)
        
        with engine.connect() as conn:
            # 检查表是否存在
            result = conn.execute(
                text(
                    "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
                    "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table_name"
                ),
                {"table_name": RedisGatewayInfo.__tablename__}
            )
            
            if result.fetchone():
                logger.info(f"✓ 表 '{RedisGatewayInfo.__tablename__}' 验证成功")
                
                # 查询表结构
                result = conn.execute(
                    text(f"DESCRIBE {RedisGatewayInfo.__tablename__}")
                )
                
                logger.info("\n表结构详情:")
                for row in result:
                    logger.info(f"  {row}")
                
                return True
            else:
                logger.error(f"✗ 表 '{RedisGatewayInfo.__tablename__}' 不存在")
                return False
        
    except SQLAlchemyError as e:
        logger.error(f"验证表失败: {e}")
        return False
    finally:
        engine.dispose()


def main():
    """
    主函数：创建数据库和表
    """
    print("=" * 60)
    print("Redis降级服务 - 数据库初始化工具")
    print("=" * 60)
    print()
    
    # 配置数据库连接参数
    # 请根据你的实际环境修改这些参数
    DB_HOST = "localhost"
    DB_PORT = 3306
    DB_USER = "root"
    DB_PASSWORD = "password"  # 请修改为实际密码
    DB_NAME = "redis_gateway_info"
    
    print("数据库连接配置:")
    print(f"  主机: {DB_HOST}")
    print(f"  端口: {DB_PORT}")
    print(f"  用户: {DB_USER}")
    print(f"  数据库: {DB_NAME}")
    print()
    
    # 提示用户确认
    response = input("是否继续？(y/n): ")
    if response.lower() != 'y':
        print("操作已取消")
        sys.exit(0)
    
    print()
    
    # 1. 创建数据库（如果不存在）
    logger.info("步骤1: 检查/创建数据库")
    db_url_without_db = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}"
    
    if not create_database(db_url_without_db, DB_NAME):
        logger.error("数据库创建失败，请检查连接参数和权限")
        sys.exit(1)
    
    print()
    
    # 2. 创建表
    logger.info("步骤2: 创建数据库表")
    db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    
    if not create_tables(db_url):
        logger.error("表创建失败")
        sys.exit(1)
    
    print()
    
    # 3. 验证表
    logger.info("步骤3: 验证表结构")
    if not verify_tables(db_url):
        logger.error("表验证失败")
        sys.exit(1)
    
    print()
    print("=" * 60)
    print("✓ 数据库初始化完成！")
    print("=" * 60)
    print()
    print("后续步骤:")
    print("1. 确保Redis服务正常运行")
    print("2. 在你的项目中导入并初始化Redis降级服务")
    print("3. 开始使用降级服务")
    print()
    print("示例代码:")
    print("  from init_service import quick_init")
    print("  from your_project import get_redis_cache_service, get_db_session")
    print("  redis_service = quick_init(get_redis_cache_service, get_db_session)")
    print()


if __name__ == "__main__":
    main()
