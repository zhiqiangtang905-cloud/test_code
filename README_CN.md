# Redis降级MySQL网关系统

一个完整的Redis到MySQL无缝降级切换系统，支持多实例部署，具备自动故障检测、健康拨测和数据同步功能。

## 功能特性

### 核心功能
- ✅ **Redis操作降级**: 支持Redis常用操作自动降级到MySQL
- ✅ **读写分离**: 优先读Redis，失败自动切换MySQL；写入先Redis，异步写MySQL
- ✅ **健康拨测**: Redis故障时自动启动健康检查，恢复后自动同步数据
- ✅ **过期管理**: 支持TTL设置，定时清理过期数据
- ✅ **分布式锁**: 多实例场景下保证任务互斥执行
- ✅ **异步写入**: 使用线程池异步写入MySQL，不阻塞主流程

### 支持的Redis操作

#### 基础操作
- `set_ex(key, value, ttl)` - 设置键值对并指定过期时间
- `get(key)` - 获取键的值
- `exists(key)` - 检查键是否存在
- `delete(key)` - 删除键
- `expire(key, ttl)` - 设置过期时间

#### Hash操作
- `hset(name, key, value)` - 设置hash字段
- `hget(name, key)` - 获取hash字段值
- `hdel(name, key)` - 删除hash字段

#### List操作
- `lpush(name, value)` - 从列表左侧插入
- `rpush(name, value)` - 从列表右侧插入
- `lpop(key, count)` - 从列表左侧弹出
- `lrange(name, start, end)` - 获取列表范围
- `lindex(key, index)` - 获取列表指定索引的元素
- `lrem(key, count, value)` - 删除列表中的元素

#### 分布式锁
- `distributed_lock(lock_key, timeout)` - 上下文管理器式的分布式锁

## 系统架构

### 数据流向

```
写入流程:
用户代码 -> RedisGateway -> Redis (同步) -> MySQL (异步)
                           |
                           +---> 失败时 -> 标记Redis不可用 -> 启动健康拨测

读取流程:
用户代码 -> RedisGateway -> Redis (优先)
                           |
                           +---> 失败或不可用 -> MySQL (降级)
```

### 健康拨测流程

```
Redis故障检测 -> 设置redis_alive=False -> 启动拨测线程
                                           |
                                           v
                                      每10秒ping Redis
                                           |
                                           v
                                      Redis恢复? 
                                           |
                                           +---> 是 -> 同步MySQL数据到Redis
                                           |          -> 设置redis_alive=True
                                           |          -> 停止拨测
                                           |
                                           +---> 否 -> 继续拨测
```

### 数据库表结构

**表名**: `redis_gateway_info`

| 字段 | 类型 | 说明 |
|------|------|------|
| key | VARCHAR(255) | Redis的key（主键1） |
| name | VARCHAR(255) | Redis的name/field（主键2） |
| value | TEXT | 存储的值（JSON字符串） |
| expire_time | DATETIME | 过期时间 |
| last_update_time | DATETIME | 最后更新时间 |

## 安装使用

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置连接信息

编辑 `config.py` 文件：

```python
# Redis配置
REDIS_CONFIG = {
    'host': 'localhost',
    'port': 6379,
    'db': 0,
    'password': None,
    'decode_responses': True,
}

# MySQL配置
MYSQL_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'your_password',
    'database': 'redis_gateway_info',
}

# 健康拨测配置
HEALTH_CHECK_CONFIG = {
    'probe_interval': 10,      # Redis健康拨测间隔（秒）
    'cleanup_interval': 60,    # 过期数据清理间隔（秒）
    'lock_timeout': 300,       # 分布式锁超时时间（秒）
}
```

### 3. 创建数据库

```sql
CREATE DATABASE redis_gateway_info CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
```

### 4. 使用示例

```python
from redis_gateway import RedisGateway
from config import REDIS_CONFIG, MYSQL_CONFIG, HEALTH_CHECK_CONFIG
import json

# 方式1: 使用上下文管理器（推荐）
with RedisGateway(REDIS_CONFIG, MYSQL_CONFIG, HEALTH_CHECK_CONFIG) as gateway:
    # 写入数据
    gateway.set_ex('user:1001', json.dumps({'name': '张三', 'age': 25}), ttl=3600)
    
    # 读取数据
    value = gateway.get('user:1001')
    user = json.loads(value)
    print(user)
    
    # Hash操作
    gateway.hset('user:profile:1001', 'city', json.dumps('北京'))
    city = gateway.hget('user:profile:1001', 'city')
    
    # 分布式锁
    with gateway.distributed_lock('my_task_lock', timeout=10):
        # 执行需要互斥的操作
        pass

# 方式2: 手动管理
gateway = RedisGateway(REDIS_CONFIG, MYSQL_CONFIG, HEALTH_CHECK_CONFIG)
try:
    gateway.set_ex('key', json.dumps('value'), ttl=60)
finally:
    gateway.close()
```

### 5. 运行示例程序

```bash
python example.py
```

## 核心特性说明

### 1. 数据格式

所有写入Redis和MySQL的数据都需要是JSON字符串格式：

```python
import json

# 写入时
data = {'name': '张三', 'age': 25}
gateway.set_ex('user:1001', json.dumps(data), ttl=3600)

# 读取时
value = gateway.get('user:1001')
data = json.loads(value)
```

### 2. 多实例部署

在多实例场景下，使用分布式锁保证只有一个实例执行任务：

```python
# 定时任务示例
def cleanup_task():
    with gateway.distributed_lock('cleanup_lock', timeout=300):
        # 只有获得锁的实例会执行
        print("执行清理任务...")
        # ... 执行具体逻辑
```

系统的健康拨测和定时清理任务都使用了分布式锁，保证多实例部署时不会重复执行。

### 3. 故障恢复流程

1. **故障检测**: 任何Redis操作失败时，自动标记Redis不可用
2. **启动拨测**: 后台线程每10秒ping一次Redis检查是否恢复
3. **数据同步**: Redis恢复后，自动将MySQL中未过期的数据同步回Redis
4. **恢复服务**: 标记Redis可用，后续操作恢复正常

### 4. 过期数据清理

- **清理频率**: 每60秒清理一次MySQL中的过期数据
- **分布式锁**: 使用锁保证多实例时只有一个实例执行清理
- **自动运行**: 在RedisGateway初始化时自动启动清理任务

### 5. 异步写入MySQL

- 所有MySQL写入操作都是异步的，不会阻塞主流程
- 使用线程池管理，最多10个工作线程
- 失败时会记录日志，不影响Redis操作

## 注意事项

### 1. 数据一致性

- Redis和MySQL之间存在短暂的数据不一致窗口（异步写入）
- 读取操作优先从Redis读取，确保最新数据
- 故障切换时可能丢失未写入MySQL的数据

### 2. List操作限制

由于MySQL的表结构限制，以下List操作在MySQL模式下有限制：
- `lpop`: MySQL不支持，返回None
- `lindex`: MySQL不支持，返回None
- List元素的顺序在MySQL中可能与Redis不完全一致

建议：如果业务严重依赖List操作，应确保Redis高可用。

### 3. 性能考虑

- 异步写入MySQL会占用内存和线程资源
- 大量写入时注意数据库连接池设置
- 建议对MySQL表的key和name字段建立索引

### 4. 分布式锁

- Redis分布式锁在Redis可用时使用Redis实现
- Redis不可用时自动降级到MySQL实现（性能较低）
- 锁超时时间需要根据实际任务执行时间设置

## 项目结构

```
.
├── config.py                      # 配置文件
├── requirements.txt               # 依赖包
├── example.py                     # 使用示例
├── README_CN.md                   # 中文文档
└── redis_gateway/                 # 核心包
    ├── __init__.py               # 包初始化
    ├── models.py                 # 数据库模型
    ├── health_check.py           # 健康拨测类
    └── redis_mysql_gateway.py    # 核心网关类
```

## 日志说明

系统使用Python标准logging模块，日志级别为INFO：

- **INFO**: 重要操作日志（连接、故障、恢复等）
- **WARNING**: 警告信息（Redis不可用等）
- **ERROR**: 错误信息（操作失败等）
- **DEBUG**: 调试信息（每个操作的详细日志）

可以通过修改logging配置调整日志级别和输出格式。

## 故障排查

### Redis连接失败

检查：
1. Redis服务是否启动
2. config.py中的Redis配置是否正确
3. 网络连接是否正常
4. Redis是否需要密码认证

### MySQL连接失败

检查：
1. MySQL服务是否启动
2. 数据库是否已创建
3. config.py中的MySQL配置是否正确
4. 用户权限是否足够

### 数据同步问题

检查：
1. 查看日志中的错误信息
2. 检查MySQL表结构是否正确
3. 检查数据格式是否为JSON字符串
4. 检查过期时间设置是否合理

## 技术支持

如有问题，请检查：
1. 日志输出
2. Redis和MySQL连接状态
3. 配置文件是否正确
4. 数据格式是否符合要求

## 许可证

MIT License
