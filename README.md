# Redis降级MySQL服务

一个完整的Redis到MySQL自动降级解决方案，支持多实例部署场景，确保在Redis不可用时系统仍能正常运行。

## 功能特性

### 1. 自动降级与恢复
- Redis可用时：优先使用Redis，异步写入MySQL作为备份
- Redis不可用时：自动降级到MySQL，保证服务不中断
- Redis恢复后：自动将MySQL数据回写到Redis，恢复正常流程

### 2. 支持的Redis操作

#### 基础操作
- `set(key, value)` - 设置key-value
- `get(key)` - 获取value
- `set_ex(key, value, ttl)` - 设置带过期时间的key
- `delete(key)` - 删除key
- `exists(key)` - 检查key是否存在
- `expire(key, ttl)` - 设置过期时间

#### Hash操作
- `hset(name, key, value)` - 设置hash字段
- `hget(name, key)` - 获取hash字段
- `hdel(name, key)` - 删除hash字段
- `hgetall(name)` - 获取所有hash字段

#### List操作
- `lpush(name, *values)` - 从左侧插入
- `rpush(name, *values)` - 从右侧插入
- `lpop(key, count)` - 从左侧弹出
- `lrange(name, start, end)` - 获取范围元素
- `lindex(key, index)` - 获取指定索引元素
- `llen(key)` - 获取列表长度
- `lrem(key, count, value)` - 删除指定元素

#### 分布式特性
- `lock(key, ttl)` - 分布式锁（上下文管理器）
- `acquire_semaphore(key, limit, ttl)` - 信号量

### 3. 健康检查与监控

#### 自动健康拨测
- Redis不可用时，每10秒自动拨测一次
- 检测到Redis恢复后自动回写数据
- 使用分布式锁确保多实例场景下只有一个实例执行拨测

#### 定时清理
- 每60秒自动清理MySQL中的过期数据
- 使用schedule调度器节省资源
- 分布式锁保证多实例场景下只有一个实例执行清理

### 4. 多实例支持

使用分布式锁机制确保：
- 健康拨测只有一个实例执行
- 数据清理只有一个实例执行
- 避免重复操作和资源浪费

### 5. 数据序列化

- 所有写入的数据自动使用`json.dumps()`序列化
- 所有读取的数据自动使用`json.loads()`反序列化
- 无缝支持Python对象存储

## 快速开始

### 方式一：最简单的方式（推荐）

```bash
# 1. 安装依赖
pip install -r requirements.txt

# 2. 配置环境变量（复制并修改.env.example）
cp .env.example .env
# 编辑.env文件，设置MySQL密码等配置

# 3. 初始化数据库（自动创建数据库和表）
python create_tables.py

# 4. 在你的项目中使用（一行代码完成初始化）
from init_service import quick_init
from your_project import get_redis_cache_service, get_db_session

redis_service = quick_init(get_redis_cache_service, get_db_session)

# 5. 开始使用（与原生Redis API完全一致）
redis_service.set("user:1001", {"name": "张三", "age": 25})
user = redis_service.get("user:1001")
```

### 方式二：使用配置文件

```python
# 1. 修改config.py中的配置
from config import config

# 2. 使用配置初始化（参考production_example.py）
from production_example import init_redis_fallback_service

redis_service = init_redis_fallback_service()
```

### 方式三：手动初始化（完全控制）

```python
from health_check import get_health_check
from redis_fallback_service import get_redis_fallback_service

# 假设你的项目中已有这两个函数
from your_project import get_redis_cache_service, get_db_session

# 初始化健康检查服务
health_check = get_health_check(get_redis_cache_service, get_db_session)
health_check.start_schedule()

# 获取降级服务实例
redis_service = get_redis_fallback_service(
    get_redis_cache_service,
    get_db_session,
    health_check
)

# 使用降级服务（与使用原生Redis一样）
redis_service.set("user:1001", {"name": "张三", "age": 25})
user = redis_service.get("user:1001")
```

## 使用示例

### 基础操作

```python
# 设置和获取
redis_service.set("key1", {"data": "value"})
value = redis_service.get("key1")

# 带过期时间
redis_service.set_ex("session:123", {"user_id": 1001}, ttl=3600)

# 检查存在
if redis_service.exists("key1"):
    print("key存在")

# 删除
redis_service.delete("key1")
```

### Hash操作

```python
# 设置hash字段
redis_service.hset("user:profile:1001", "name", "张三")
redis_service.hset("user:profile:1001", "age", 25)

# 获取单个字段
name = redis_service.hget("user:profile:1001", "name")

# 获取所有字段
profile = redis_service.hgetall("user:profile:1001")
```

### List操作

```python
# 插入元素
redis_service.rpush("queue", "msg1", "msg2", "msg3")
redis_service.lpush("queue", "urgent_msg")

# 获取列表
messages = redis_service.lrange("queue", 0, -1)
length = redis_service.llen("queue")

# 弹出元素
msg = redis_service.lpop("queue")
```

### 分布式锁

```python
# 使用上下文管理器
with redis_service.lock("critical_section", ttl=30) as acquired:
    if acquired:
        # 执行需要加锁的操作
        process_critical_task()
    else:
        print("获取锁失败")
```

### 信号量

```python
# 限制并发数
if redis_service.acquire_semaphore("task:limit5", limit=5, ttl=60):
    try:
        # 执行任务
        execute_task()
    finally:
        # 任务完成后会自动过期释放
        pass
```

## 架构设计

### 数据库表结构

```sql
CREATE TABLE redis_gateway_info (
    `key` VARCHAR(255) NOT NULL COMMENT 'Redis的key',
    `name` VARCHAR(255) NOT NULL DEFAULT '' COMMENT 'Redis的hash name或list name',
    `value` VARCHAR(65535) NULL COMMENT '存储的值(JSON序列化)',
    `expire_time` DATETIME NULL COMMENT '过期时间',
    `last_update_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间',
    PRIMARY KEY (`key`, `name`),
    INDEX idx_expire_time (`expire_time`),
    INDEX idx_last_update_time (`last_update_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

### 核心组件

1. **models.py** - 数据库ORM模型
2. **health_check.py** - 健康检查服务
   - 监控Redis状态
   - 执行健康拨测
   - 定时清理过期数据
   - 分布式锁协调

3. **redis_fallback_service.py** - 降级服务
   - 提供统一的Redis API
   - 自动降级和恢复
   - 异步写入MySQL
   - 同步降级逻辑

### 工作流程

#### 写入流程
```
Redis可用:
  1. 写入Redis
  2. 异步写入MySQL（不影响响应速度）

Redis不可用:
  1. 直接写入MySQL
  2. 触发健康拨测
```

#### 读取流程
```
Redis可用:
  1. 从Redis读取
  2. 如果失败，降级到MySQL

Redis不可用:
  1. 清理MySQL过期数据
  2. 从MySQL读取
```

#### 恢复流程
```
1. 健康拨测检测到Redis恢复
2. 获取分布式锁（确保只有一个实例执行）
3. 将MySQL中未过期的数据回写到Redis
4. 标记Redis为可用状态
5. 停止健康拨测
```

## 最佳实践

### 1. 合理设置过期时间
- 根据数据重要性设置合适的TTL
- 临时数据建议设置较短的过期时间
- 重要数据可以不设置过期时间

### 2. 监控和日志
- 配置适当的日志级别
- 监控Redis和MySQL的性能指标
- 关注健康拨测和数据清理的执行情况

### 3. 资源优化
- 合理配置数据库连接池大小
- 控制异步写入的线程数量
- 定期清理不需要的历史数据

### 4. 多实例部署
- 依赖分布式锁机制协调多实例
- 确保数据库连接配置正确
- 监控各实例的健康状态

## 注意事项

1. **变量命名规范**
   - 本项目使用"service"而非"gateway"
   - 这样更符合服务层的概念
   - 避免与API网关等概念混淆

2. **数据一致性**
   - Redis和MySQL的数据最终一致
   - 异步写入有极小的延迟
   - 降级场景下直接操作MySQL保证强一致性

3. **性能考虑**
   - 异步写入不影响读写性能
   - MySQL作为备份，查询效率相对较低
   - 建议优化数据库索引提升查询速度

4. **安全性**
   - 确保数据库连接使用加密传输
   - 合理设置数据库访问权限
   - 定期备份MySQL数据

## 故障排查

### Redis连接失败
- 检查Redis服务是否运行
- 验证连接配置（host、port、密码等）
- 查看防火墙和网络配置

### MySQL连接失败
- 检查MySQL服务状态
- 验证数据库连接字符串
- 确认数据库用户权限

### 数据不一致
- 检查异步写入线程是否正常
- 查看日志中的错误信息
- 验证数据序列化和反序列化逻辑

### 多实例问题
- 确认分布式锁是否正常工作
- 检查各实例的Redis可用状态
- 查看健康拨测的执行日志

## 贡献指南

欢迎提交Issue和Pull Request来改进这个项目。

## 许可证

MIT License

## 文件说明

### 核心文件
- **models.py** - 数据库ORM模型定义
- **health_check.py** - 健康检查和定时任务服务
- **redis_fallback_service.py** - Redis降级服务主逻辑
- **init_service.py** - 服务初始化和管理器
- **config.py** - 配置管理

### 工具文件
- **create_tables.py** - 数据库表初始化脚本
- **requirements.txt** - Python依赖包列表
- **.env.example** - 环境变量配置示例

### 示例文件
- **simple_example.py** - 最简单的使用示例
- **example_usage.py** - 详细的使用示例
- **production_example.py** - 生产环境使用示例

## 项目集成指南

### 如何集成到现有项目

假设你的项目结构如下：
```
your_project/
├── app/
│   ├── __init__.py
│   ├── models.py
│   ├── redis_client.py  # 你原有的Redis客户端
│   └── database.py      # 你原有的数据库连接
└── ...
```

集成步骤：

1. **复制文件到项目**
```bash
# 创建降级服务目录
mkdir your_project/redis_fallback/

# 复制核心文件
cp models.py health_check.py redis_fallback_service.py init_service.py config.py your_project/redis_fallback/

# 创建__init__.py
touch your_project/redis_fallback/__init__.py
```

2. **初始化数据库**
```bash
python create_tables.py
```

3. **在应用启动时初始化服务**
```python
# your_project/app/__init__.py

from redis_fallback.init_service import quick_init
from .redis_client import get_redis_cache_service
from .database import get_db_session

# 初始化Redis降级服务
redis_fallback_service = quick_init(
    get_redis_cache_service,
    get_db_session
)

# 导出供其他模块使用
__all__ = ['redis_fallback_service']
```

4. **在业务代码中使用**
```python
# your_project/app/user_service.py

from app import redis_fallback_service as redis

def get_user_cache(user_id):
    return redis.get(f"user:{user_id}")

def set_user_cache(user_id, user_data):
    redis.set_ex(f"user:{user_id}", user_data, ttl=3600)
```

### 零侵入集成方案

如果你不想修改现有的Redis调用代码，可以使用别名：

```python
# 替换原有的Redis客户端
from redis_fallback.init_service import quick_init
from your_old_module import get_redis_cache_service, get_db_session

# 用降级服务替换原有的Redis客户端
redis_client = quick_init(get_redis_cache_service, get_db_session)

# 现有代码无需修改，直接使用
redis_client.set("key", "value")
redis_client.get("key")
```

## 性能优化建议

### 1. 使用连接池
```python
import redis

# Redis连接池
redis_pool = redis.ConnectionPool(
    host='localhost',
    port=6379,
    max_connections=50
)

def get_redis_cache_service():
    return redis.Redis(connection_pool=redis_pool)
```

### 2. 批量操作
```python
# 使用pipeline进行批量操作
# 注意：当前版本需要在业务层实现pipeline逻辑
def batch_set_users(users):
    for user in users:
        redis_service.hset(f"user:{user['id']}", "data", user)
```

### 3. 合理设置TTL
```python
# 热数据：短TTL（5分钟）
redis_service.set_ex("hot:data", value, ttl=300)

# 温数据：中等TTL（1小时）
redis_service.set_ex("warm:data", value, ttl=3600)

# 冷数据：长TTL（24小时）
redis_service.set_ex("cold:data", value, ttl=86400)
```

## 监控和告警

### 推荐监控指标

1. **Redis可用性**
```python
from init_service import get_manager

manager = get_manager()
is_alive = manager.is_redis_alive()
# 监控此指标，Redis不可用时发送告警
```

2. **MySQL表大小**
```sql
-- 定期检查表大小，及时清理
SELECT 
    table_name,
    ROUND(((data_length + index_length) / 1024 / 1024), 2) AS size_mb
FROM information_schema.TABLES
WHERE table_schema = 'redis_gateway_info';
```

3. **过期数据数量**
```sql
-- 检查过期但未清理的数据
SELECT COUNT(*) 
FROM redis_gateway_info 
WHERE expire_time IS NOT NULL 
AND expire_time <= NOW();
```

## FAQ

### Q1: Redis恢复后，MySQL的数据会自动同步回Redis吗？
A: 是的，健康拨测检测到Redis恢复后，会自动将MySQL中未过期的数据回写到Redis。

### Q2: 多个实例同时运行会不会导致重复操作？
A: 不会。使用分布式锁机制确保健康拨测和数据清理任务只有一个实例在执行。

### Q3: 异步写入MySQL失败怎么办？
A: 异步写入失败会记录日志，不会影响主流程。Redis降级时会同步写入MySQL，确保数据不丢失。

### Q4: 如何处理大量数据的回写？
A: 回写操作在Redis恢复后自动执行，建议在业务低峰期进行。如果数据量太大，可以考虑分批回写。

### Q5: 支持哪些Redis数据类型？
A: 目前支持String、Hash、List类型，以及分布式锁和信号量。Sorted Set、Set等类型可以根据需要扩展。

### Q6: 性能开销有多大？
A: 
- Redis可用时：异步写入MySQL，对性能影响极小（< 1ms）
- Redis不可用时：直接操作MySQL，性能取决于MySQL配置和索引优化

### Q7: 如何升级和迁移？
A: 
1. 停止旧服务
2. 备份MySQL数据
3. 部署新版本代码
4. 运行数据库迁移脚本（如有）
5. 启动新服务

## 版本历史

### v1.0.0 (2025-10-16)
- 首次发布
- 支持基础的Redis操作降级
- 实现健康检查和自动恢复
- 支持多实例分布式协调
- 提供完整的配置和示例

## 路线图

### 计划中的功能
- [ ] 支持更多Redis数据类型（Set、Sorted Set等）
- [ ] 提供Web管理界面
- [ ] 增加Prometheus监控指标导出
- [ ] 支持数据预热策略
- [ ] 提供性能分析工具

## 贡献指南

欢迎提交Issue和Pull Request！

### 开发环境搭建
```bash
# 克隆项目
git clone <repository_url>

# 安装开发依赖
pip install -r requirements.txt

# 运行测试
pytest tests/

# 代码风格检查
flake8 .
```

## 许可证

MIT License

## 联系方式

如有问题或建议，请通过Issue联系。
