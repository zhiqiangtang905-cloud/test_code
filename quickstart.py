#!/usr/bin/env python3
"""
Redis降级MySQL快速启动脚本
"""

import sys
import logging
from redis_mysql_fallback import RedisGateway

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def check_dependencies():
    """检查依赖是否已安装"""
    required_modules = ['redis', 'sqlalchemy', 'schedule', 'pymysql']
    missing_modules = []
    
    for module in required_modules:
        try:
            __import__(module)
        except ImportError:
            missing_modules.append(module)
    
    if missing_modules:
        print(f"❌ 缺少以下依赖模块: {', '.join(missing_modules)}")
        print(f"请运行: pip install {' '.join(missing_modules)}")
        return False
    
    print("✅ 所有依赖已安装")
    return True


def test_redis_connection(redis_config):
    """测试Redis连接"""
    import redis
    try:
        client = redis.Redis(**redis_config)
        client.ping()
        print("✅ Redis连接成功")
        client.close()
        return True
    except Exception as e:
        print(f"⚠️  Redis连接失败: {e}")
        print("系统将在Redis不可用时自动降级到MySQL")
        return False


def test_mysql_connection(mysql_config):
    """测试MySQL连接"""
    from sqlalchemy import create_engine
    try:
        engine = create_engine(mysql_config['connection_string'])
        connection = engine.connect()
        connection.close()
        engine.dispose()
        print("✅ MySQL连接成功")
        return True
    except Exception as e:
        print(f"❌ MySQL连接失败: {e}")
        print("请检查MySQL配置和数据库是否已创建")
        return False


def initialize_gateway():
    """初始化Redis网关"""
    # 默认配置（需要根据实际环境修改）
    redis_config = {
        'host': 'localhost',
        'port': 6379,
        'db': 0,
        'password': None,
        'decode_responses': False,
        'socket_connect_timeout': 5,
        'socket_timeout': 5,
    }
    
    mysql_config = {
        'connection_string': 'mysql+pymysql://root:password@localhost:3306/redis_gateway_info?charset=utf8mb4'
    }
    
    print("\n" + "="*50)
    print("Redis降级MySQL - 快速启动")
    print("="*50 + "\n")
    
    # 检查依赖
    print("1. 检查依赖...")
    if not check_dependencies():
        return None
    print()
    
    # 测试连接
    print("2. 测试连接...")
    redis_ok = test_redis_connection(redis_config)
    mysql_ok = test_mysql_connection(mysql_config)
    print()
    
    if not mysql_ok:
        print("❌ MySQL连接失败，无法启动")
        print("\n请确保:")
        print("1. MySQL服务已启动")
        print("2. 已创建数据库: CREATE DATABASE redis_gateway_info;")
        print("3. 配置中的用户名和密码正确")
        return None
    
    # 初始化网关
    print("3. 初始化网关...")
    try:
        gateway = RedisGateway(redis_config, mysql_config)
        print("✅ 网关初始化成功")
        return gateway
    except Exception as e:
        print(f"❌ 网关初始化失败: {e}")
        return None


def run_interactive_demo(gateway):
    """运行交互式演示"""
    print("\n" + "="*50)
    print("交互式演示")
    print("="*50 + "\n")
    
    # 启动定时任务
    gateway.start_scheduled_tasks()
    
    commands = """
可用命令:
  1. set <key> <value>          - 设置键值
  2. get <key>                  - 获取键值
  3. hset <name> <key> <value>  - 设置Hash字段
  4. hget <name> <key>          - 获取Hash字段
  5. rpush <key> <value>        - 推入列表
  6. lrange <key> <start> <end> - 获取列表范围
  7. status                     - 查看Redis状态
  8. help                       - 显示帮助
  9. quit                       - 退出
  
提示: Redis故障时会自动降级到MySQL
"""
    print(commands)
    
    while True:
        try:
            cmd = input("\n> ").strip()
            if not cmd:
                continue
            
            parts = cmd.split()
            operation = parts[0].lower()
            
            if operation == 'quit' or operation == 'exit':
                print("再见!")
                break
            
            elif operation == 'help':
                print(commands)
            
            elif operation == 'status':
                status = "✅ 正常" if gateway.health_checker.redis_alive else "❌ 降级中"
                checking = "是" if gateway.health_checker.is_checking else "否"
                print(f"Redis状态: {status}")
                print(f"健康检查中: {checking}")
            
            elif operation == 'set':
                if len(parts) < 3:
                    print("用法: set <key> <value>")
                    continue
                key, value = parts[1], ' '.join(parts[2:])
                gateway.set(key, value)
                print(f"✅ 已设置: {key} = {value}")
            
            elif operation == 'get':
                if len(parts) < 2:
                    print("用法: get <key>")
                    continue
                key = parts[1]
                value = gateway.get(key)
                print(f"值: {value}")
            
            elif operation == 'hset':
                if len(parts) < 4:
                    print("用法: hset <name> <key> <value>")
                    continue
                name, key, value = parts[1], parts[2], ' '.join(parts[3:])
                gateway.hset(name, key, value)
                print(f"✅ 已设置: {name}.{key} = {value}")
            
            elif operation == 'hget':
                if len(parts) < 3:
                    print("用法: hget <name> <key>")
                    continue
                name, key = parts[1], parts[2]
                value = gateway.hget(name, key)
                print(f"值: {value}")
            
            elif operation == 'rpush':
                if len(parts) < 3:
                    print("用法: rpush <key> <value>")
                    continue
                key, value = parts[1], ' '.join(parts[2:])
                gateway.rpush(key, value)
                print(f"✅ 已推入: {key} <- {value}")
            
            elif operation == 'lrange':
                if len(parts) < 4:
                    print("用法: lrange <key> <start> <end>")
                    continue
                key, start, end = parts[1], int(parts[2]), int(parts[3])
                values = gateway.lrange(key, start, end)
                print(f"列表: {values}")
            
            else:
                print(f"未知命令: {operation}")
                print("输入 'help' 查看可用命令")
        
        except KeyboardInterrupt:
            print("\n\n再见!")
            break
        except Exception as e:
            print(f"错误: {e}")


def main():
    """主函数"""
    gateway = initialize_gateway()
    
    if not gateway:
        sys.exit(1)
    
    print("\n✨ 网关已就绪!")
    
    # 询问是否运行交互式演示
    print("\n是否运行交互式演示? (y/n): ", end='')
    choice = input().strip().lower()
    
    if choice == 'y' or choice == 'yes':
        try:
            run_interactive_demo(gateway)
        except KeyboardInterrupt:
            print("\n")
    
    # 清理
    print("\n关闭网关...")
    gateway.close()
    print("✅ 完成")


if __name__ == '__main__':
    main()
