# Redis降级MySQL解决方案

[![Python](https://img.shields.io/badge/Python-3.7+-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

一个完整的Redis降级MySQL实现方案，支持多实例环境部署。当Redis服务不可用时，自动降级到MySQL数据库，确保系统高可用性。

## ✨ 核心特性

- **🔄 自动降级**: Redis故障时自动降级到MySQL，无需人工干预
- **💓 健康检查**: 自动检测Redis状态，恢复后自动回写数据
- **🔒 多实例支持**: 通过分布式锁确保多实例环境下只有一个实例执行关键任务
- **⚡ 异步写入**: MySQL写入采用异步方式，不影响主流程性能
- **🧹 自动清理**: 定时清理过期数据，避免数据堆积
- **📦 完整覆盖**: 支持String、Hash、List等常用Redis操作

## 📋 目录结构

```
.
├── redis_mysql_fallback.py    # 核心实现代码
├── config.py                   # 配置文件模板
├── example_usage.py            # 基础使用示例
├── advanced_example.py         # 高级使用示例
├── test_redis_fallback.py      # 单元测试
├── quickstart.py               # 快速启动脚本
├── requirements.txt            # 依赖列表
└── README_REDIS_FALLBACK.md    # 详细文档
```

## 🚀 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 创建数据库

```sql
CREATE DATABASE redis_gateway_info DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
```

### 3. 配置连接

编辑 `config.py` 文件，配置Redis和MySQL连接信息：

```python
REDIS_CONFIG = {
    'host': 'localhost',
    'port': 6379,
    'db': 0,
    'password': None,
}

MYSQL_CONFIG = {
    'connection_string': 'mysql+pymysql://username:password@localhost:3306/redis_gateway_info?charset=utf8mb4'
}
```

### 4. 运行快速启动脚本

```bash
python quickstart.py
```

## 💡 使用示例

### 基础用法

```python
from redis_mysql_fallback import RedisGateway

# 初始化网关
gateway = RedisGateway(redis_config, mysql_config)

# 启动定时任务
gateway.start_scheduled_tasks()

# String操作
gateway.set('user:name', 'Zhang San')
name = gateway.get('user:name')

# Hash操作
gateway.hset('user:1001', 'age', '25')
age = gateway.hget('user:1001', 'age')

# List操作
gateway.rpush('queue:tasks', 'task1', 'task2')
tasks = gateway.lrange('queue:tasks', 0, -1)

# 分布式锁
with gateway.distributed_lock('my_lock', timeout=10) as acquired:
    if acquired:
        # 执行需要加锁的业务逻辑
        pass
```

### 高级功能

查看 `advanced_example.py` 了解更多高级功能：

- 用户缓存管理
- 会话管理
- 任务队列
- 配置管理
- 限流器
- 分布式任务协调

## 🏗️ 系统架构

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

1. **正常情况**: 优先使用Redis，同时异步写入MySQL备份
2. **Redis故障**: 自动切换到MySQL，启动健康检查
3. **Redis恢复**: 同步MySQL数据回Redis，恢复正常流程

## 📊 支持的Redis操作

### String操作
- `set(key, value)` - 设置值
- `set_ex(key, value, ttl)` - 设置带过期时间的值
- `get(key)` - 获取值
- `delete(key)` - 删除键
- `exists(key)` - 检查键是否存在
- `expire(key, ttl)` - 设置过期时间

### Hash操作
- `hset(name, key, value)` - 设置Hash字段
- `hget(name, key)` - 获取Hash字段
- `hdel(name, *keys)` - 删除Hash字段
- `hgetall(name)` - 获取所有Hash字段

### List操作
- `rpush(name, *values)` - 从右侧推入
- `lpush(name, *values)` - 从左侧推入
- `lrange(name, start, end)` - 获取列表范围
- `lpop(key, count)` - 从左侧弹出
- `lindex(key, index)` - 获取指定索引元素
- `llen(key)` - 获取列表长度
- `lrem(key, count, value)` - 删除指定值

### 分布式锁
- `distributed_lock(lock_key, timeout)` - 分布式锁上下文管理器

## 🔍 核心机制

### 健康检查机制

- **检查频率**: Redis故障后每10秒检查一次
- **分布式协调**: 使用MySQL分布式锁确保只有一个实例执行检查
- **自动恢复**: Redis恢复后自动同步MySQL数据

### 降级策略

**写入流程**:
1. 检查Redis状态
2. 如果Redis可用，写入Redis
3. 异步写入MySQL作为备份
4. 如果Redis失败，标记Redis不可用，启动健康检查

**读取流程**:
1. 如果Redis可用，优先从Redis读取
2. 如果Redis失败或不可用，从MySQL读取
3. 读取MySQL前先清理过期数据

### 数据一致性

- **写入一致性**: Redis写入成功后异步写入MySQL
- **过期处理**: MySQL记录过期时间，读取时自动过滤过期数据
- **数据同步**: Redis恢复时从MySQL全量同步未过期数据

### 多实例协调

使用MySQL实现的分布式锁确保：
- 只有一个实例执行健康检查
- 只有一个实例执行数据同步
- 避免重复任务执行

## 📈 性能优化建议

1. **连接池配置**: 调整MySQL连接池大小适应并发
2. **异步写入**: MySQL写入在独立线程，不阻塞主流程
3. **批量操作**: 尽量使用批量操作减少网络开销
4. **索引优化**: expire_time和last_update_time已建立索引

## 🧪 测试

运行单元测试：

```bash
python test_redis_fallback.py
```

## 📝 文档

- [详细文档](README_REDIS_FALLBACK.md) - 完整的使用文档和API说明
- [基础示例](example_usage.py) - 基本操作示例
- [高级示例](advanced_example.py) - 高级功能和最佳实践

## ⚠️ 注意事项

1. **数据类型限制**: MySQL备份主要支持String、Hash、List
2. **过期精度**: MySQL过期时间精度为秒级
3. **性能影响**: 频繁写入会增加MySQL负载
4. **锁超时**: 分布式锁有超时机制
5. **数据量控制**: 定期清理不需要的数据

## 🤝 贡献

欢迎提交Issue和Pull Request！

## 📄 许可证

MIT License

## 🔗 相关资源

- [Redis官方文档](https://redis.io/documentation)
- [SQLAlchemy文档](https://docs.sqlalchemy.org/)
- [PyMySQL文档](https://pymysql.readthedocs.io/)

---

**提示**: 这是一个多实例共用数据库的解决方案，通过分布式锁确保关键任务只被一个实例执行，适用于微服务和分布式系统场景。