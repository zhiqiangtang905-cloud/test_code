# -*- coding: utf-8 -*-
"""
快速测试脚本
用于验证Redis降级MySQL网关的基本功能
"""
import json
from redis_gateway import RedisGateway
from config import REDIS_CONFIG, MYSQL_CONFIG, HEALTH_CHECK_CONFIG


def test_basic_functionality():
    """
    测试基本功能
    """
    print("开始测试Redis降级MySQL网关...")
    print("-" * 60)
    
    try:
        # 初始化网关
        print("\n1. 初始化网关...")
        gateway = RedisGateway(REDIS_CONFIG, MYSQL_CONFIG, HEALTH_CHECK_CONFIG)
        print("✓ 网关初始化成功")
        
        # 测试基础操作
        print("\n2. 测试基础操作...")
        test_key = 'test:basic:key'
        test_value = json.dumps({'test': 'value', 'timestamp': '2024-01-01'})
        
        gateway.set_ex(test_key, test_value, ttl=300)
        print(f"✓ 写入成功: {test_key}")
        
        result = gateway.get(test_key)
        if result:
            print(f"✓ 读取成功: {json.loads(result)}")
        else:
            print("✗ 读取失败")
        
        exists = gateway.exists(test_key)
        print(f"✓ 存在性检查: {exists}")
        
        # 测试Hash操作
        print("\n3. 测试Hash操作...")
        hash_name = 'test:hash'
        gateway.hset(hash_name, 'field1', json.dumps('value1'))
        gateway.hset(hash_name, 'field2', json.dumps('value2'))
        print(f"✓ Hash写入成功: {hash_name}")
        
        field1 = gateway.hget(hash_name, 'field1')
        if field1:
            print(f"✓ Hash读取成功: field1 = {json.loads(field1)}")
        
        # 测试分布式锁
        print("\n4. 测试分布式锁...")
        lock_key = 'test:lock'
        try:
            with gateway.distributed_lock(lock_key, timeout=5):
                print("✓ 成功获取分布式锁")
        except Exception as e:
            print(f"✗ 分布式锁测试失败: {e}")
        
        # 清理测试数据
        print("\n5. 清理测试数据...")
        gateway.delete(test_key)
        gateway.delete(hash_name)
        print("✓ 清理完成")
        
        # 关闭网关
        print("\n6. 关闭网关...")
        gateway.close()
        print("✓ 网关已关闭")
        
        print("\n" + "=" * 60)
        print("所有测试通过！✓")
        print("=" * 60)
        
        return True
    
    except Exception as e:
        print(f"\n✗ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == '__main__':
    print("""
    ╔═══════════════════════════════════════════════════════════╗
    ║     Redis降级MySQL网关 - 快速测试                         ║
    ╚═══════════════════════════════════════════════════════════╝
    """)
    
    print("注意：请确保已完成以下准备工作：")
    print("1. ✓ Redis服务已启动")
    print("2. ✓ MySQL服务已启动")
    print("3. ✓ 已创建数据库: redis_gateway_info")
    print("4. ✓ config.py配置正确")
    print("5. ✓ 已安装依赖: pip install -r requirements.txt")
    print()
    
    input("按Enter键开始测试...")
    
    success = test_basic_functionality()
    
    if success:
        print("\n✓ 系统工作正常，可以开始使用！")
        print("\n下一步：")
        print("- 运行 python example.py 查看更多示例")
        print("- 阅读 README_CN.md 了解详细文档")
    else:
        print("\n✗ 测试未通过，请检查配置和服务状态")
        print("\n故障排查：")
        print("1. 检查Redis服务: redis-cli ping")
        print("2. 检查MySQL服务: mysql -u root -p")
        print("3. 检查配置文件: config.py")
        print("4. 查看错误日志")
