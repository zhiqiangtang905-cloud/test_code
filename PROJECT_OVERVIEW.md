# Redis降级MySQL网关系统 - 项目概览

## 项目简介

这是一个完整的Redis到MySQL无缝降级切换系统，专为多实例部署场景设计。当Redis服务不可用时，系统会自动切换到MySQL进行数据存储和读取，并在Redis恢复后自动同步数据。

## 核心设计理念

### 1. 高可用性
- Redis故障时自动降级到MySQL
- 健康拨测机制确保及时恢复
- 分布式锁保证多实例场景下的数据一致性

### 2. 性能优化
- 读取优先从Redis，保证低延迟
- 异步写入MySQL，不阻塞主流程
- 线程池管理并发写入

### 3. 数据安全
- 所有写入操作同时写Redis和MySQL
- 定时清理过期数据
- 自动数据同步和恢复

## 技术栈

- **Python 3.7+**: 主要编程语言
- **Redis**: 高性能缓存
- **MySQL**: 关系型数据库（降级存储）
- **SQLAlchemy**: ORM框架
- **Threading**: 多线程处理
- **Schedule**: 定时任务调度

## 核心组件

### 1. 数据库模型层 (models.py)

**功能**:
- 定义MySQL数据表结构
- 提供数据库连接管理
- 支持连接池和自动重连

**核心类**:
- `RedisGatewayInfo`: 数据表模型
- `DatabaseManager`: 数据库管理器

### 2. 健康拨测层 (health_check.py)

**功能**:
- 监控Redis健康状态
- 故障时启动自动拨测
- 恢复后同步数据
- 定时清理过期数据

**核心类**:
- `HealthCheck`: 健康拨测管理器

**关键方法**:
- `set_redis_down()`: 标记Redis不可用
- `set_redis_up()`: 标记Redis恢复
- `start_health_probe()`: 启动健康拨测
- `start_cleanup_task()`: 启动清理任务

### 3. 网关核心层 (redis_mysql_gateway.py)

**功能**:
- 统一的Redis操作接口
- 自动降级和故障转移
- 分布式锁支持
- 异步写入MySQL

**核心类**:
- `RedisGateway`: 网关主类

**支持的操作**:
```python
# 基础操作
set_ex(key, value, ttl)
get(key)
exists(key)
delete(key)
expire(key, ttl)

# Hash操作
hset(name, key, value)
hget(name, key)
hdel(name, key)

# List操作
lpush(name, value)
rpush(name, value)
lpop(key, count)
lrange(name, start, end)
lindex(key, index)
lrem(key, count, value)

# 分布式锁
distributed_lock(lock_key, timeout)
```

## 数据流转

### 写入流程

```
用户调用 -> RedisGateway
              |
              ├─> 检查Redis可用性
              |
              ├─> 写入Redis（同步）
              |     ├─> 成功：继续
              |     └─> 失败：标记Redis不可用
              |
              └─> 异步写入MySQL
                    └─> 线程池处理
```

### 读取流程

```
用户调用 -> RedisGateway
              |
              ├─> 检查Redis可用性
              |
              ├─> Redis可用？
              |     ├─> 是：从Redis读取
              |     |     ├─> 成功：返回数据
              |     |     └─> 失败：标记不可用，降级
              |     |
              |     └─> 否：从MySQL读取
              |           └─> 清理过期数据
              |           └─> 查询并返回
              |
              └─> 返回结果
```

### 故障恢复流程

```
Redis故障
  |
  ├─> 标记 redis_alive = False
  |
  ├─> 启动健康拨测线程
  |
  └─> 每10秒执行：
        |
        ├─> Ping Redis
        |
        ├─> 成功？
        |     ├─> 是：
        |     |   ├─> 从MySQL读取未过期数据
        |     |   ├─> 写入Redis
        |     |   ├─> 标记 redis_alive = True
        |     |   └─> 停止拨测
        |     |
        |     └─> 否：继续等待
        |
        └─> 循环
```

## 多实例部署

### 分布式锁机制

系统使用分布式锁确保多实例场景下的互斥操作：

1. **Redis可用时**：使用Redis分布式锁
2. **Redis不可用时**：降级到MySQL锁

### 定时任务协调

- 健康拨测任务：每个实例独立运行，使用锁避免重复拨测
- 清理任务：每60秒执行一次，使用锁保证只有一个实例执行

### 示例场景

```python
# 场景：3个实例同时运行定时任务

实例1: 获取锁成功 -> 执行清理 -> 释放锁
实例2: 获取锁失败 -> 跳过
实例3: 获取锁失败 -> 跳过
```

## 配置说明

### Redis配置

```python
REDIS_CONFIG = {
    'host': 'localhost',          # Redis服务器地址
    'port': 6379,                 # Redis端口
    'db': 0,                      # 数据库索引
    'password': None,             # 密码（如需要）
    'decode_responses': True,     # 自动解码响应
    'socket_timeout': 5,          # 连接超时
    'socket_connect_timeout': 5,  # 连接超时
}
```

### MySQL配置

```python
MYSQL_CONFIG = {
    'host': 'localhost',          # MySQL服务器地址
    'port': 3306,                 # MySQL端口
    'user': 'root',               # 用户名
    'password': 'your_password',  # 密码
    'database': 'redis_gateway_info',  # 数据库名
    'charset': 'utf8mb4',         # 字符集
}
```

### 健康拨测配置

```python
HEALTH_CHECK_CONFIG = {
    'probe_interval': 10,         # Redis健康拨测间隔（秒）
    'cleanup_interval': 60,       # 过期数据清理间隔（秒）
    'lock_timeout': 300,          # 分布式锁超时时间（秒）
    'lock_key': 'redis_gateway:health_check:lock',      # 拨测锁key
    'cleanup_lock_key': 'redis_gateway:cleanup:lock',   # 清理锁key
}
```

## 使用场景

### 场景1：用户会话管理

```python
# 存储用户会话，30分钟过期
session_id = generate_session_id()
session_data = {
    'user_id': 1001,
    'username': 'zhangsan',
    'login_time': datetime.now().isoformat()
}

gateway.set_ex(
    f'session:{session_id}',
    json.dumps(session_data),
    ttl=1800  # 30分钟
)

# 读取会话
session_json = gateway.get(f'session:{session_id}')
if session_json:
    session = json.loads(session_json)
```

### 场景2：用户资料缓存

```python
# 使用Hash存储用户资料
user_id = 1001
gateway.hset(f'user:profile:{user_id}', 'name', json.dumps('张三'))
gateway.hset(f'user:profile:{user_id}', 'email', json.dumps('zhangsan@example.com'))
gateway.hset(f'user:profile:{user_id}', 'phone', json.dumps('13800138000'))

# 读取资料
name = json.loads(gateway.hget(f'user:profile:{user_id}', 'name'))
email = json.loads(gateway.hget(f'user:profile:{user_id}', 'email'))
```

### 场景3：任务队列

```python
# 添加任务到队列
task = {
    'task_id': generate_task_id(),
    'type': 'send_email',
    'data': {...}
}

gateway.rpush('task:queue', json.dumps(task))

# 获取任务列表
tasks = gateway.lrange('task:queue', 0, 10)
for task_json in tasks:
    task = json.loads(task_json)
    process_task(task)
```

### 场景4：定时任务（多实例）

```python
# 多实例场景下的定时任务
def scheduled_cleanup():
    lock_key = 'scheduled:cleanup:lock'
    
    try:
        with gateway.distributed_lock(lock_key, timeout=300, blocking=False):
            # 只有获得锁的实例会执行
            print("执行清理任务...")
            perform_cleanup()
    except RuntimeError:
        print("其他实例正在执行")
```

## 监控和日志

### 日志级别

- **INFO**: 重要操作（初始化、故障、恢复）
- **WARNING**: 警告信息（Redis不可用）
- **ERROR**: 错误信息（操作失败）
- **DEBUG**: 详细调试信息

### 关键日志

```
# 正常操作
Redis set_ex成功: key=user:1001
MySQL写入成功: key=user:1001, name=

# 故障检测
Redis操作失败: Connection refused
检测到Redis不可用，开启健康拨测
健康拨测线程已启动

# 恢复过程
Redis健康检查成功，开始回写数据
数据同步完成，共同步 150 条记录
Redis已恢复可用
健康拨测线程已停止

# 清理任务
获取清理任务锁成功，开始清理过期数据
清理过期数据完成，共删除 25 条记录
```

## 性能考虑

### 读性能

- **Redis可用时**: ~0.1ms (内存读取)
- **MySQL降级时**: ~5-10ms (数据库查询)

### 写性能

- **同步写Redis**: ~0.1ms
- **异步写MySQL**: 不阻塞主流程
- **线程池大小**: 10个工作线程

### 优化建议

1. **连接池配置**: 根据并发量调整数据库连接池大小
2. **索引优化**: 确保MySQL表的key和name字段有索引
3. **过期时间**: 合理设置TTL，避免存储过多数据
4. **线程池大小**: 根据写入频率调整线程池大小

## 注意事项

### 1. 数据一致性

- Redis和MySQL之间存在短暂的数据延迟（异步写入）
- 读取优先从Redis，确保读取最新数据
- 故障切换时可能丢失未写入MySQL的数据

### 2. List操作限制

MySQL无法完美模拟Redis的List数据结构：
- `lpop`、`lindex`等操作在MySQL模式下功能受限
- 建议：关键业务避免过度依赖List操作

### 3. 内存管理

- 异步写入会占用内存队列
- 大量写入时注意监控内存使用
- 必要时调整线程池大小

### 4. 分布式锁

- 锁超时时间应大于任务执行时间
- Redis不可用时锁性能下降（MySQL实现）
- 避免长时间持有锁

## 文件清单

```
/workspace/
├── config.py                      # 配置文件
├── requirements.txt               # 依赖包列表
├── example.py                     # 完整使用示例
├── quick_test.py                  # 快速测试脚本
├── README.md                      # 英文文档
├── README_CN.md                   # 中文文档
├── PROJECT_OVERVIEW.md            # 项目概览（本文件）
└── redis_gateway/                 # 核心包目录
    ├── __init__.py               # 包初始化
    ├── models.py                 # 数据库模型（460行）
    ├── health_check.py           # 健康拨测类（390行）
    └── redis_mysql_gateway.py    # 核心网关类（850行）
```

## 快速开始

```bash
# 1. 安装依赖
pip install -r requirements.txt

# 2. 配置连接（编辑config.py）
# 3. 创建数据库
mysql -u root -p -e "CREATE DATABASE redis_gateway_info CHARACTER SET utf8mb4"

# 4. 运行快速测试
python quick_test.py

# 5. 查看完整示例
python example.py
```

## 技术支持

如遇问题，请按以下步骤排查：

1. ✓ 检查Redis服务：`redis-cli ping`
2. ✓ 检查MySQL服务：`mysql -u root -p`
3. ✓ 检查配置文件：`config.py`
4. ✓ 查看日志输出
5. ✓ 阅读文档：`README_CN.md`

## 未来扩展

可能的扩展方向：

1. **支持更多Redis数据类型**: Set、Sorted Set等
2. **增强监控**: Prometheus metrics、健康检查API
3. **性能优化**: 批量写入MySQL、缓存预热
4. **高可用**: Redis Sentinel支持、MySQL主从
5. **配置中心**: 支持动态配置更新

## 总结

这是一个生产级的Redis降级方案，具有以下特点：

✅ **完整性**: 涵盖常用Redis操作  
✅ **可靠性**: 自动故障检测和恢复  
✅ **可扩展**: 支持多实例部署  
✅ **易用性**: 简洁的API设计  
✅ **文档完善**: 详细的使用说明和示例  

适合用于需要Redis高可用保障的生产环境。
