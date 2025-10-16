# -*- coding: utf-8 -*-
"""
Redis降级MySQL网关使用示例
演示如何使用RedisGateway进行Redis到MySQL的无缝降级
"""
import time
import json
from redis_gateway import RedisGateway
from config import REDIS_CONFIG, MYSQL_CONFIG, HEALTH_CHECK_CONFIG


def example_basic_operations():
    """
    示例1: 基础操作
    演示set_ex, get, exists, delete等基础操作
    """
    print("\n" + "="*50)
    print("示例1: 基础操作")
    print("="*50)
    
    # 初始化网关（使用上下文管理器自动关闭资源）
    with RedisGateway(REDIS_CONFIG, MYSQL_CONFIG, HEALTH_CHECK_CONFIG) as gateway:
        # 设置键值对，10秒后过期
        gateway.set_ex('user:1001', json.dumps({'name': '张三', 'age': 25}), ttl=10)
        print("✓ 设置 user:1001")
        
        # 获取值
        value = gateway.get('user:1001')
        if value:
            user = json.loads(value)
            print(f"✓ 获取 user:1001: {user}")
        
        # 检查是否存在
        exists = gateway.exists('user:1001')
        print(f"✓ user:1001 存在: {exists}")
        
        # 删除键
        gateway.delete('user:1001')
        print("✓ 删除 user:1001")
        
        # 再次检查
        exists = gateway.exists('user:1001')
        print(f"✓ user:1001 存在: {exists}")


def example_hash_operations():
    """
    示例2: Hash操作
    演示hset, hget, hdel等Hash操作
    """
    print("\n" + "="*50)
    print("示例2: Hash操作")
    print("="*50)
    
    with RedisGateway(REDIS_CONFIG, MYSQL_CONFIG, HEALTH_CHECK_CONFIG) as gateway:
        # 设置Hash字段
        gateway.hset('user:profile:1001', 'name', json.dumps('张三'))
        gateway.hset('user:profile:1001', 'age', json.dumps(25))
        gateway.hset('user:profile:1001', 'city', json.dumps('北京'))
        print("✓ 设置用户档案信息")
        
        # 获取Hash字段
        name = gateway.hget('user:profile:1001', 'name')
        age = gateway.hget('user:profile:1001', 'age')
        city = gateway.hget('user:profile:1001', 'city')
        
        if name and age and city:
            print(f"✓ 姓名: {json.loads(name)}")
            print(f"✓ 年龄: {json.loads(age)}")
            print(f"✓ 城市: {json.loads(city)}")
        
        # 删除Hash字段
        gateway.hdel('user:profile:1001', 'city')
        print("✓ 删除城市信息")
        
        # 再次获取
        city = gateway.hget('user:profile:1001', 'city')
        print(f"✓ 城市信息: {city}")


def example_list_operations():
    """
    示例3: List操作
    演示lpush, rpush, lrange等List操作
    """
    print("\n" + "="*50)
    print("示例3: List操作")
    print("="*50)
    
    with RedisGateway(REDIS_CONFIG, MYSQL_CONFIG, HEALTH_CHECK_CONFIG) as gateway:
        # 从左侧插入
        gateway.lpush('task:queue', json.dumps({'task_id': 1, 'type': 'send_email'}))
        gateway.lpush('task:queue', json.dumps({'task_id': 2, 'type': 'send_sms'}))
        print("✓ 左侧插入任务")
        
        # 从右侧插入
        gateway.rpush('task:queue', json.dumps({'task_id': 3, 'type': 'push_notification'}))
        print("✓ 右侧插入任务")
        
        # 获取列表
        tasks = gateway.lrange('task:queue', 0, -1)
        if tasks:
            print(f"✓ 任务列表:")
            for task in tasks:
                task_data = json.loads(task) if isinstance(task, str) else task
                print(f"  - {task_data}")
        
        # 删除列表元素
        gateway.lrem('task:queue', 1, json.dumps({'task_id': 2, 'type': 'send_sms'}))
        print("✓ 删除task_id=2的任务")


def example_distributed_lock():
    """
    示例4: 分布式锁
    演示如何使用分布式锁保证多实例场景下的互斥操作
    """
    print("\n" + "="*50)
    print("示例4: 分布式锁")
    print("="*50)
    
    with RedisGateway(REDIS_CONFIG, MYSQL_CONFIG, HEALTH_CHECK_CONFIG) as gateway:
        lock_key = 'critical_section_lock'
        
        print("尝试获取分布式锁...")
        try:
            # 使用分布式锁
            with gateway.distributed_lock(lock_key, timeout=10):
                print("✓ 成功获取锁，执行关键操作")
                
                # 模拟执行一些需要互斥的操作
                counter_key = 'global:counter'
                current = gateway.get(counter_key)
                
                if current:
                    count = int(json.loads(current))
                else:
                    count = 0
                
                count += 1
                gateway.set_ex(counter_key, json.dumps(count), ttl=3600)
                
                print(f"✓ 更新全局计数器: {count}")
                
                # 模拟耗时操作
                time.sleep(2)
                
                print("✓ 操作完成，释放锁")
        
        except RuntimeError as e:
            print(f"✗ 获取锁失败: {e}")


def example_expire_time():
    """
    示例5: 过期时间
    演示如何设置和更新过期时间
    """
    print("\n" + "="*50)
    print("示例5: 过期时间")
    print("="*50)
    
    with RedisGateway(REDIS_CONFIG, MYSQL_CONFIG, HEALTH_CHECK_CONFIG) as gateway:
        # 设置带过期时间的键
        gateway.set_ex('session:abc123', json.dumps({'user_id': 1001}), ttl=5)
        print("✓ 设置session，5秒后过期")
        
        # 检查是否存在
        exists = gateway.exists('session:abc123')
        print(f"✓ session存在: {exists}")
        
        # 更新过期时间
        gateway.expire('session:abc123', 10)
        print("✓ 更新过期时间为10秒")
        
        # 等待6秒
        print("等待6秒...")
        time.sleep(6)
        
        # 检查是否还存在（应该仍然存在，因为更新了过期时间）
        exists = gateway.exists('session:abc123')
        print(f"✓ session存在: {exists}")


def example_redis_failover():
    """
    示例6: Redis故障切换
    演示Redis不可用时自动切换到MySQL的过程
    """
    print("\n" + "="*50)
    print("示例6: Redis故障切换演示")
    print("="*50)
    
    with RedisGateway(REDIS_CONFIG, MYSQL_CONFIG, HEALTH_CHECK_CONFIG) as gateway:
        # 写入数据
        gateway.set_ex('test:key', json.dumps({'data': 'test_value'}), ttl=60)
        print("✓ 写入测试数据")
        
        # 正常读取
        value = gateway.get('test:key')
        print(f"✓ 从Redis读取: {value}")
        
        print("\n注意: 如果Redis不可用，系统会自动:")
        print("1. 检测到Redis故障")
        print("2. 标记Redis为不可用状态")
        print("3. 启动健康拨测（每10秒检测一次）")
        print("4. 读写操作自动切换到MySQL")
        print("5. Redis恢复后，自动同步MySQL数据到Redis")
        print("6. 恢复正常的Redis读写")


def example_multi_instance():
    """
    示例7: 多实例场景
    演示多实例共用数据库时，使用分布式锁保证只有一个实例执行任务
    """
    print("\n" + "="*50)
    print("示例7: 多实例场景")
    print("="*50)
    
    with RedisGateway(REDIS_CONFIG, MYSQL_CONFIG, HEALTH_CHECK_CONFIG) as gateway:
        task_lock_key = 'scheduled_task:cleanup:lock'
        
        print("模拟定时任务执行（多实例场景）...")
        
        try:
            # 尝试获取任务锁，超时10秒
            with gateway.distributed_lock(task_lock_key, timeout=10, blocking=False):
                print("✓ 当前实例获得执行权限")
                print("✓ 执行定时清理任务...")
                
                # 模拟任务执行
                time.sleep(3)
                
                print("✓ 任务执行完成")
        
        except RuntimeError:
            print("✗ 其他实例正在执行任务，本实例跳过")


def main():
    """
    主函数
    运行所有示例
    """
    print("\n" + "="*70)
    print("Redis降级MySQL网关 - 使用示例")
    print("="*70)
    
    try:
        # 运行各个示例
        example_basic_operations()
        example_hash_operations()
        example_list_operations()
        example_distributed_lock()
        example_expire_time()
        example_redis_failover()
        example_multi_instance()
        
        print("\n" + "="*70)
        print("所有示例执行完成！")
        print("="*70)
        
        # 提示
        print("\n注意事项:")
        print("1. 请确保Redis和MySQL服务已启动")
        print("2. 请在config.py中配置正确的连接信息")
        print("3. 系统会自动创建数据库表")
        print("4. 健康拨测和定时清理任务会在后台自动运行")
        print("5. 使用Ctrl+C可以安全退出程序")
    
    except KeyboardInterrupt:
        print("\n\n程序被用户中断")
    
    except Exception as e:
        print(f"\n\n程序执行出错: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
