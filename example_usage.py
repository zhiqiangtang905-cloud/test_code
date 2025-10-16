"""
Redis降级MySQL使用示例
"""

from redis_mysql_fallback import RedisGateway
import json
import time

# 配置信息
redis_config = {
    'host': 'localhost',
    'port': 6379,
    'db': 0,
    'password': None,  # 如果需要密码，在这里设置
    'decode_responses': False,  # 保持为False，我们手动处理编码
    'socket_connect_timeout': 5,
    'socket_timeout': 5,
}

mysql_config = {
    'connection_string': 'mysql+pymysql://username:password@localhost:3306/redis_gateway_info?charset=utf8mb4'
}

def main():
    # 初始化Redis网关
    gateway = RedisGateway(redis_config, mysql_config)
    
    # 启动定时任务（每60秒清理过期数据）
    gateway.start_scheduled_tasks()
    
    print("=== Redis降级MySQL示例 ===\n")
    
    # ==================== String操作示例 ====================
    print("1. String操作示例:")
    
    # 设置普通字符串
    gateway.set('user:name', 'Zhang San')
    print(f"设置 user:name = Zhang San")
    
    # 设置带过期时间的字符串
    gateway.set_ex('session:12345', {'user_id': 100, 'token': 'abc123'}, ttl=300)
    print(f"设置 session:12345 = {{'user_id': 100, 'token': 'abc123'}} (5分钟过期)")
    
    # 获取字符串
    name = gateway.get('user:name')
    print(f"获取 user:name = {name}")
    
    session = gateway.get('session:12345')
    print(f"获取 session:12345 = {session}\n")
    
    # ==================== Hash操作示例 ====================
    print("2. Hash操作示例:")
    
    # 设置Hash字段
    gateway.hset('user:1001', 'name', 'Li Si')
    gateway.hset('user:1001', 'age', '25')
    gateway.hset('user:1001', 'city', 'Beijing')
    print("设置 user:1001 的多个字段")
    
    # 获取Hash字段
    user_name = gateway.hget('user:1001', 'name')
    print(f"获取 user:1001.name = {user_name}")
    
    # 获取所有Hash字段
    user_info = gateway.hgetall('user:1001')
    print(f"获取 user:1001 所有字段 = {user_info}")
    
    # 删除Hash字段
    gateway.hdel('user:1001', 'age')
    print("删除 user:1001.age 字段\n")
    
    # ==================== List操作示例 ====================
    print("3. List操作示例:")
    
    # 右侧推入
    gateway.rpush('queue:tasks', 'task1', 'task2', 'task3')
    print("rpush queue:tasks: task1, task2, task3")
    
    # 左侧推入
    gateway.lpush('queue:tasks', 'urgent_task')
    print("lpush queue:tasks: urgent_task")
    
    # 获取列表范围
    tasks = gateway.lrange('queue:tasks', 0, -1)
    print(f"lrange queue:tasks 0 -1 = {tasks}")
    
    # 获取列表长度
    length = gateway.llen('queue:tasks')
    print(f"llen queue:tasks = {length}")
    
    # 从左侧弹出
    task = gateway.lpop('queue:tasks')
    print(f"lpop queue:tasks = {task}")
    
    # 获取指定索引元素
    second_task = gateway.lindex('queue:tasks', 1)
    print(f"lindex queue:tasks 1 = {second_task}")
    
    # 删除指定值
    gateway.rpush('queue:tasks', 'task2')  # 添加重复值
    removed = gateway.lrem('queue:tasks', 1, 'task2')
    print(f"lrem queue:tasks 删除1个 task2, 删除数量 = {removed}\n")
    
    # ==================== 其他操作示例 ====================
    print("4. 其他操作示例:")
    
    # 检查键是否存在
    exists = gateway.exists('user:name')
    print(f"exists user:name = {exists}")
    
    # 设置过期时间
    gateway.expire('user:name', 600)
    print("设置 user:name 过期时间为600秒")
    
    # 删除键
    gateway.delete('session:12345')
    print("删除 session:12345\n")
    
    # ==================== 分布式锁示例 ====================
    print("5. 分布式锁示例:")
    
    with gateway.distributed_lock('my_lock', timeout=10) as acquired:
        if acquired:
            print("成功获取分布式锁")
            # 执行需要加锁的业务逻辑
            time.sleep(1)
            print("业务逻辑执行完成，释放锁")
        else:
            print("获取分布式锁失败，其他实例正在执行")
    
    print("\n=== 示例完成 ===")
    
    # 模拟Redis故障
    print("\n=== 模拟Redis故障场景 ===")
    print("当Redis不可用时，所有操作会自动降级到MySQL")
    print("健康检查会每10秒检测Redis状态")
    print("Redis恢复后，会自动同步MySQL数据回Redis\n")
    
    # 保持程序运行以观察定时任务
    try:
        print("程序运行中... (Ctrl+C 退出)")
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        print("\n程序退出")
        gateway.close()


if __name__ == '__main__':
    main()
