# Redis降级MySQL服务

这是一个完整的Redis降级到MySQL的解决方案，适用于多实例共用一个数据库的场景。当Redis不可用时，服务会自动降级到MySQL进行数据读写，并在Redis恢复后自动同步数据。

## 功能特性

### 1. 完整的Redis操作支持
支持以下Redis操作的降级：
- **基础操作**: `set_ex`, `get`, `exists`, `delete`, `expire`
- **Hash操作**: `hset`, `hget`, `hgetall`, `hdel`
- **List操作**: `rpush`, `lpush`, `lrange`, `lpop`, `lindex`, `llen`, `lrem`
- **分布式锁**: 基于Redis的分布式锁，确保多实例环境下的互斥执行
- **信号量**: Redis信号量，用于限流和并发控制

### 2. 智能降级机制
- **自动降级**: Redis不可用时，自动切换到MySQL存储
- **健康拨测**: 每10秒检测Redis健康状态
- **自动恢复**: Redis恢复后，自动将MySQL数据同步回Redis
- **数据一致性**: 写操作同时写Redis和MySQL，确保数据一致

### 3. 多实例协调
- **分布式锁**: 使用Redis分布式锁确保只有一个实例执行关键任务
- **定时任务**: 使用schedule调度器，每60秒清理过期数据
- **线程安全**: 所有状态修改都有锁保护

### 4. 数据管理
- **过期时间**: 支持TTL设置，自动清理过期数据
- **JSON序列化**: 数据自动使用`json.dumps()`/`json.loads()`序列化
- **增量同步**: Redis恢复时只同步未过期的数据

## 文件结构

```
.
├── redis_models.py              # MySQL ORM模型定义
├── redis_health_checker.py      # Redis健康检查类
├── redis_fallback_service.py    # Redis降级服务主类
├── usage_example.py             # 使用示例
└── README.md                    # 本文档
```

## 数据库表结构

### redis_gateway_info 表

```sql
CREATE TABLE redis_gateway_info (
    `key` VARCHAR(255) NOT NULL COMMENT 'Redis键名',
    `name` VARCHAR(255) NOT NULL DEFAULT '' COMMENT 'Redis hash/list等的name字段',
    `value` TEXT COMMENT '存储json.dumps()后的值',
    `expire_time` DATETIME COMMENT '数据过期时间',
    `last_update_time` DATETIME COMMENT '最后更新时间',
    PRIMARY KEY (`key`, `name`),
    INDEX idx_expire_time (`expire_time`),
    INDEX idx_last_update (`last_update_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Redis数据降级存储表';
```

**注意**: `gateway`在表名中表示Redis和MySQL之间的网关/代理层，用于数据降级和恢复。

## 快速开始

### 1. 安装依赖

```bash
pip install sqlalchemy redis schedule
```

### 2. 创建数据库表

使用上面的SQL语句创建`redis_gateway_info`表。

### 3. 在你的项目中集成

```python
from redis_fallback_service import RedisFallbackService
from your_module import get_redis_cache_service, get_db_session

# 初始化服务
service = RedisFallbackService(get_redis_cache_service, get_db_session)

# 启动定时任务调度器
service.start_scheduler()

# 使用服务
service.set_ex("mykey", "myvalue", ttl=3600)
value = service.get("mykey")
```

## 使用示例

### 基础操作

```python
# 设置带过期时间的键值对
service.set_ex("user:1001", {"name": "张三", "age": 25}, ttl=3600)

# 获取值（自动降级）
user_data = service.get("user:1001")

# Hash操作
service.hset("user:profile:1001", "nickname", "小明")
nickname = service.hget("user:profile:1001", "nickname")
profile = service.hgetall("user:profile:1001")

# List操作
service.rpush("task:queue", "task1", "task2")
tasks = service.lrange("task:queue", 0, -1)
```

### 分布式锁

```python
# 确保多实例环境下只有一个实例执行
try:
    with service.distributed_lock("task:process_orders", timeout=60):
        # 执行你的业务逻辑
        process_orders()
except RuntimeError:
    # 其他实例正在执行
    pass
```

### 信号量

```python
# 限制并发数量
if service.acquire_semaphore("semaphore:api_calls", limit=3, timeout=30):
    try:
        # 执行API调用
        result = call_external_api()
    finally:
        service.release_semaphore("semaphore:api_calls", identifier)
```

## 工作原理

### 写入流程

1. 检查Redis健康状态
2. 如果Redis可用，写入Redis
3. 异步写入MySQL（无论Redis是否可用）
4. 如果Redis操作失败，标记Redis为不可用并启动健康拨测

### 读取流程

1. 检查Redis健康状态
2. 如果Redis可用，从Redis读取
3. 如果Redis不可用或读取失败，降级到MySQL读取
4. 读取MySQL前先清理该key的过期数据

### 健康拨测流程

1. Redis操作失败时，标记Redis为不可用
2. 启动健康拨测线程（每10秒执行一次）
3. 拨测成功后：
   - 将MySQL数据同步回Redis
   - 标记Redis为可用
   - 停止健康拨测
4. 恢复正常的Redis读写

### 定时清理流程

1. 启动时，创建schedule调度器
2. 每60秒执行一次清理任务
3. 使用分布式锁确保只有一个实例执行清理
4. 清理MySQL中expire_time小于当前时间的记录

## 注意事项

### 1. 多实例协调

- 关键任务使用`distributed_lock`确保互斥执行
- 定时清理任务自动使用分布式锁，无需手动处理

### 2. 性能考虑

- MySQL写入是异步的，不会阻塞Redis操作
- 健康拨测在独立线程中运行
- 使用索引优化MySQL查询性能

### 3. 数据一致性

- 写操作同时写Redis和MySQL
- Redis恢复时会同步MySQL数据
- 过期数据定期清理

### 4. List操作限制

由于MySQL不适合模拟Redis的List结构，以下操作在降级到MySQL时有限制：
- `lpop`: 不支持MySQL降级
- `lrem`: 不完全支持MySQL降级
- 建议：关键的List操作确保Redis可用

### 5. 变量命名

代码中谨慎使用"gateway"命名：
- **使用"gateway"的位置**: 仅在`RedisGatewayInfo`类名和表名中使用
- **原因**: 表示Redis和MySQL之间的网关/代理层，用于数据降级和恢复
- **其他位置**: 统一使用"service"命名

## 日志配置

```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
```

日志级别说明：
- `INFO`: 重要操作（降级、恢复、清理等）
- `DEBUG`: 详细操作日志（每次Redis操作）
- `WARNING`: 警告信息（Redis不可用、无法获取锁等）
- `ERROR`: 错误信息（操作失败）

## 扩展性

### 添加新的Redis操作

1. 在`RedisFallbackService`类中添加新方法
2. 遵循现有模式：
   - 检查Redis健康状态
   - 尝试Redis操作
   - 异常时调用`_handle_redis_error`
   - 异步写入MySQL

### 自定义健康拨测逻辑

重写`_check_redis_health`方法，实现自定义的健康检查逻辑。

### 自定义数据同步逻辑

重写`_sync_mysql_to_redis`方法，实现自定义的数据同步策略。

## 故障排除

### Redis持续不可用

检查：
1. Redis服务是否启动
2. 网络连接是否正常
3. Redis配置是否正确
4. 查看健康拨测日志

### MySQL数据未同步到Redis

检查：
1. Redis是否已恢复可用
2. 数据是否已过期
3. 查看`_sync_mysql_to_redis`的日志

### 分布式锁无法获取

检查：
1. 锁是否被其他实例持有
2. 锁的超时时间是否合理
3. Redis是否可用

### 定时清理未执行

检查：
1. 调度器是否已启动（`start_scheduler`）
2. 是否有其他实例持有清理锁
3. 查看调度器线程日志

## 最佳实践

1. **启动时初始化**: 在应用启动时创建`RedisFallbackService`实例并启动调度器
2. **单例模式**: 建议使用单例模式管理服务实例
3. **合理的TTL**: 为所有缓存数据设置合理的过期时间
4. **监控日志**: 关注降级和恢复的日志，及时发现问题
5. **压力测试**: 在生产环境前进行充分的压力测试
6. **分布式锁**: 关键任务务必使用分布式锁

## 许可证

本代码可自由使用和修改。

## 技术支持

如有问题，请查看代码中的详细注释或参考`usage_example.py`中的示例。
