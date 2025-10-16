# Redis降级MySQL解决方案

## 项目简介

这是一个完整的Redis降级MySQL实现方案，支持多实例环境部署。当Redis服务不可用时，自动降级到MySQL数据库，确保系统高可用性。

### 核心特性

1. **自动降级机制**：Redis故障时自动降级到MySQL，无需人工干预
2. **健康检查**：自动检测Redis状态，恢复后自动回写数据
3. **多实例支持**：通过分布式锁确保多实例环境下只有一个实例执行关键任务
4. **异步写入**：MySQL写入采用异步方式，不影响主流程性能
5. **自动清理**：定时清理过期数据，避免数据堆积
6. **完整覆盖**：支持String、Hash、List等常用Redis操作

## 系统架构

```
┌─────────────┐
│  应用层     │
└─────┬───────┘
      │
      ▼
┌─────────────────────────┐
│   RedisGateway          │
│  (Redis降级网关)         │
└───┬─────────────────┬───┘
    │                 │
    ▼                 ▼
┌─────────┐      ┌──────────┐
│  Redis  │      │  MySQL   │
│  (主存储)│      │ (备份存储)│
└─────────┘      └──────────┘
```

### 工作流程

1. **正常情况**：优先使用Redis，同时异步写入MySQL备份
2. **Redis故障**：自动切换到MySQL，启动健康检查
3. **Redis恢复**：同步MySQL数据回Redis，恢复正常流程

## 安装配置

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 创建数据库

```sql
CREATE DATABASE redis_gateway_info DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
```

数据库表结构会在首次运行时自动创建，包含以下字段：

| 字段名 | 类型 | 说明 |
|--------|------|------|
| key | VARCHAR(255) | Redis键（主键） |
| name | VARCHAR(255) | Redis名称/字段名（主键） |
| value | TEXT | JSON序列化后的值 |
| expire_time | DATETIME | 过期时间 |
| last_update_time | DATETIME | 最后更新时间 |

### 3. 配置连接

```python
# Redis配置
redis_config = {
    'host': 'localhost',
    'port': 6379,
    'db': 0,
    'password': None,
    'decode_responses': False,
    'socket_connect_timeout': 5,
    'socket_timeout': 5,
}

# MySQL配置
mysql_config = {
    'connection_string': 'mysql+pymysql://username:password@localhost:3306/redis_gateway_info?charset=utf8mb4'
}
```

## 使用说明

### 基本用法

```python
from redis_mysql_fallback import RedisGateway

# 初始化网关
gateway = RedisGateway(redis_config, mysql_config)

# 启动定时任务（每60秒清理过期数据）
gateway.start_scheduled_tasks()

# 使用网关操作数据
gateway.set('key', 'value')
value = gateway.get('key')
```

### 支持的操作

#### String操作

```python
# 设置值
gateway.set('user:name', 'Zhang San')

# 设置带过期时间的值
gateway.set_ex('session:token', 'abc123', ttl=300)

# 获取值
value = gateway.get('user:name')

# 删除键
gateway.delete('user:name')

# 检查键是否存在
exists = gateway.exists('user:name')

# 设置过期时间
gateway.expire('user:name', 600)
```

#### Hash操作

```python
# 设置Hash字段
gateway.hset('user:1001', 'name', 'Li Si')
gateway.hset('user:1001', 'age', '25')

# 获取Hash字段
name = gateway.hget('user:1001', 'name')

# 获取所有Hash字段
user_info = gateway.hgetall('user:1001')

# 删除Hash字段
gateway.hdel('user:1001', 'age', 'city')
```

#### List操作

```python
# 右侧推入
gateway.rpush('queue:tasks', 'task1', 'task2', 'task3')

# 左侧推入
gateway.lpush('queue:tasks', 'urgent_task')

# 获取列表范围
tasks = gateway.lrange('queue:tasks', 0, -1)

# 获取列表长度
length = gateway.llen('queue:tasks')

# 从左侧弹出
task = gateway.lpop('queue:tasks')

# 获取指定索引元素
element = gateway.lindex('queue:tasks', 0)

# 删除指定值
removed_count = gateway.lrem('queue:tasks', 1, 'task1')
```

#### 分布式锁

```python
# 使用上下文管理器获取分布式锁
with gateway.distributed_lock('my_lock', timeout=10) as acquired:
    if acquired:
        # 执行需要加锁的业务逻辑
        print("执行业务逻辑")
    else:
        print("其他实例正在执行")
```

## 核心机制

### 1. 健康检查机制

- **检查频率**：Redis故障后每10秒检查一次
- **分布式协调**：使用MySQL分布式锁确保只有一个实例执行检查
- **自动恢复**：Redis恢复后自动同步MySQL数据

```python
class RedisHealthChecker:
    def __init__(self, redis_client, mysql_engine, check_interval=10):
        self.redis_alive = True      # Redis存活状态
        self.is_checking = False      # 是否正在拨测
```

### 2. 降级策略

#### 写入流程

1. 检查Redis状态
2. 如果Redis可用，写入Redis
3. 异步写入MySQL作为备份
4. 如果Redis失败，标记Redis不可用，启动健康检查

#### 读取流程

1. 如果Redis可用，优先从Redis读取
2. 如果Redis失败或不可用，从MySQL读取
3. 读取MySQL前先清理过期数据

### 3. 数据一致性

- **写入一致性**：Redis写入成功后异步写入MySQL
- **过期处理**：MySQL记录过期时间，读取时自动过滤过期数据
- **数据同步**：Redis恢复时从MySQL全量同步未过期数据

### 4. 多实例协调

使用MySQL实现的分布式锁确保：

- 只有一个实例执行健康检查
- 只有一个实例执行数据同步
- 避免重复任务执行

```python
# 分布式锁实现
def _acquire_distributed_lock(self, lock_key: str, timeout: int = 10) -> bool:
    # 基于MySQL的分布式锁
    # 使用key和name作为联合主键保证唯一性
    # 使用expire_time实现锁超时
```

## 定时任务

系统使用`schedule`库实现定时任务，资源占用低：

```python
# 启动定时任务
gateway.start_scheduled_tasks()

# 每60秒自动执行：
# 1. 清理MySQL中的过期数据
# 2. 释放过期的分布式锁
```

## 数据格式说明

### MySQL存储格式

所有存入MySQL的数据都经过JSON序列化：

```python
# 写入时
value_str = json.dumps(value)

# 读取时
value = json.loads(value_str)
```

### 特殊标识

- `_string`：表示Redis String类型
- `_list_<index>`：表示Redis List类型的元素
- `_lock`：表示分布式锁

## 性能优化建议

1. **连接池配置**：调整MySQL连接池大小适应并发
   ```python
   pool_size=10,      # 连接池大小
   max_overflow=20,   # 最大溢出连接数
   ```

2. **异步写入**：MySQL写入在独立线程，不阻塞主流程

3. **批量操作**：尽量使用批量操作减少网络开销
   ```python
   gateway.rpush('queue', 'val1', 'val2', 'val3')  # 批量推入
   ```

4. **索引优化**：expire_time和last_update_time已建立索引

## 监控与日志

系统提供详细的日志输出：

```python
logger.info("Redis恢复完成，健康拨测停止")
logger.warning("Redis标记为不可用，启动健康拨测")
logger.error("MySQL写入失败: {error}")
```

建议配置日志级别和输出：

```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('redis_gateway.log'),
        logging.StreamHandler()
    ]
)
```

## 故障场景处理

### 场景1：Redis完全不可用

1. 系统自动标记Redis为不可用
2. 所有操作降级到MySQL
3. 每10秒检查Redis状态
4. Redis恢复后自动同步数据

### 场景2：Redis间歇性故障

1. 操作失败时标记Redis不可用
2. 启动健康检查
3. 检测到Redis可用后立即恢复

### 场景3：MySQL故障

1. Redis仍正常工作
2. 异步写入MySQL失败会记录日志
3. 不影响主流程

### 场景4：多实例竞争

1. 分布式锁确保只有一个实例执行关键任务
2. 其他实例等待或跳过
3. 锁超时自动释放

## 注意事项

1. **数据类型限制**：MySQL备份主要支持String、Hash、List，Set和ZSet需要额外实现
2. **过期精度**：MySQL过期时间精度为秒级
3. **性能影响**：频繁写入会增加MySQL负载，建议监控数据库性能
4. **锁超时**：分布式锁有超时机制，长时间任务需要适当增加超时时间
5. **数据量控制**：定期清理不需要的数据，避免MySQL表过大

## 完整示例

参考 `example_usage.py` 文件获取完整的使用示例。

## 测试建议

### 单元测试

```python
def test_fallback():
    # 1. 正常情况测试
    gateway.set('test_key', 'test_value')
    assert gateway.get('test_key') == 'test_value'
    
    # 2. 模拟Redis故障
    gateway.health_checker.redis_alive = False
    assert gateway.get('test_key') == 'test_value'  # 应从MySQL读取
    
    # 3. 恢复测试
    gateway.health_checker.redis_alive = True
    # 验证数据同步
```

### 压力测试

```python
import concurrent.futures

def stress_test():
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        futures = []
        for i in range(1000):
            future = executor.submit(gateway.set, f'key_{i}', f'value_{i}')
            futures.append(future)
        
        concurrent.futures.wait(futures)
```

## 故障排查

### 常见问题

1. **分布式锁获取失败**
   - 检查MySQL连接是否正常
   - 检查锁是否超时未释放

2. **数据同步失败**
   - 检查Redis连接状态
   - 检查MySQL表结构是否正确

3. **性能下降**
   - 检查MySQL连接池配置
   - 检查过期数据清理是否正常

## 许可证

MIT License
