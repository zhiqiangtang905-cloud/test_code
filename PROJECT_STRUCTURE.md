# 项目结构说明

## 目录结构

```
redis-fallback-service/
├── models.py                    # 数据库ORM模型
├── health_check.py              # 健康检查服务
├── redis_fallback_service.py    # Redis降级服务
├── init_service.py              # 服务初始化管理器
├── config.py                    # 配置管理
├── create_tables.py             # 数据库初始化脚本
├── requirements.txt             # 依赖包列表
├── .env.example                 # 环境变量配置示例
├── README.md                    # 项目说明文档
├── PROJECT_STRUCTURE.md         # 本文件
├── simple_example.py            # 简单使用示例
├── example_usage.py             # 详细使用示例
└── production_example.py        # 生产环境示例
```

## 核心模块详解

### 1. models.py - 数据库模型

**功能：**
- 定义MySQL表结构
- 使用SQLAlchemy ORM

**主要类：**
- `RedisGatewayInfo`: Redis网关信息表模型
  - `key`: Redis键（主键）
  - `name`: Hash name或List name（主键）
  - `value`: JSON序列化的值
  - `expire_time`: 过期时间
  - `last_update_time`: 最后更新时间

**使用场景：**
- 数据库表定义
- ORM查询操作

### 2. health_check.py - 健康检查服务

**功能：**
- 监控Redis存活状态
- 执行健康拨测（Redis不可用时每10秒）
- 定时清理过期数据（每60秒）
- 管理分布式锁

**主要类：**
- `RedisHealthCheck`: 健康检查服务类
  - 单例模式
  - 支持多实例协调
  - 自动回写数据

**核心方法：**
- `start_schedule()`: 启动定时任务
- `mark_redis_down()`: 标记Redis不可用
- `_health_probe_loop()`: 健康拨测循环
- `_sync_mysql_to_redis()`: 同步MySQL到Redis
- `_cleanup_expired_data()`: 清理过期数据

**使用场景：**
- 应用启动时初始化
- 自动在后台运行
- 不需要手动干预

### 3. redis_fallback_service.py - 降级服务

**功能：**
- 提供统一的Redis API
- 自动降级和恢复
- 异步写入MySQL
- 支持多种数据类型

**主要类：**
- `RedisFallbackService`: Redis降级服务类

**核心方法分类：**

#### 基础操作
- `set(key, value)`: 设置键值
- `get(key)`: 获取值
- `set_ex(key, value, ttl)`: 设置带过期时间的键值
- `delete(key)`: 删除键
- `exists(key)`: 检查键是否存在
- `expire(key, ttl)`: 设置过期时间

#### Hash操作
- `hset(name, key, value)`: 设置hash字段
- `hget(name, key)`: 获取hash字段
- `hdel(name, key)`: 删除hash字段
- `hgetall(name)`: 获取所有hash字段

#### List操作
- `lpush(name, *values)`: 左侧插入
- `rpush(name, *values)`: 右侧插入
- `lpop(key, count)`: 左侧弹出
- `lrange(name, start, end)`: 获取范围
- `lindex(key, index)`: 获取指定索引
- `llen(key)`: 获取长度
- `lrem(key, count, value)`: 删除元素

#### 分布式特性
- `lock(key, ttl)`: 分布式锁（上下文管理器）
- `acquire_semaphore(key, limit, ttl)`: 获取信号量
- `release_semaphore(key, identifier)`: 释放信号量

**数据流程：**

```
写入流程：
Redis可用 → 写Redis → 异步写MySQL
Redis不可用 → 直接写MySQL → 触发拨测

读取流程：
Redis可用 → 读Redis → 失败则读MySQL
Redis不可用 → 清理过期 → 读MySQL
```

### 4. init_service.py - 初始化管理器

**功能：**
- 简化服务初始化
- 提供便捷接口
- 管理服务生命周期

**主要类：**
- `RedisFallbackServiceManager`: 服务管理器
  - 封装初始化逻辑
  - 提供状态查询
  - 支持优雅关闭

**核心方法：**
- `initialize()`: 初始化服务
- `get_service()`: 获取服务实例
- `get_health_check()`: 获取健康检查实例
- `stop()`: 停止服务
- `is_redis_alive()`: 检查Redis状态
- `force_check_redis()`: 强制健康检查

**便捷函数：**
- `quick_init()`: 一行代码初始化（推荐使用）

**使用场景：**
- 应用启动时初始化
- 获取服务实例
- 查询服务状态

### 5. config.py - 配置管理

**功能：**
- 集中管理配置
- 支持环境变量
- 多环境配置

**主要类：**
- `Config`: 基础配置类
- `DevelopmentConfig`: 开发环境配置
- `ProductionConfig`: 生产环境配置
- `TestingConfig`: 测试环境配置

**配置项：**

```python
# Redis配置
REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD
REDIS_SOCKET_TIMEOUT, REDIS_SOCKET_CONNECT_TIMEOUT

# MySQL配置
MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD
MYSQL_DATABASE, MYSQL_CHARSET
MYSQL_POOL_SIZE, MYSQL_MAX_OVERFLOW

# 健康检查配置
HEALTH_CHECK_INTERVAL  # 拨测间隔
CLEANUP_INTERVAL       # 清理间隔
LOCK_TTL               # 锁超时时间

# 性能配置
ASYNC_MYSQL_WRITE      # 是否异步写入
ASYNC_WRITE_TIMEOUT    # 异步写入超时
```

**使用方法：**
```python
from config import config

# 获取MySQL连接URL
db_url = config.get_mysql_url()

# 获取Redis连接参数
redis_kwargs = config.get_redis_connection_kwargs()

# 验证配置
config.validate()

# 打印配置
config.print_config()
```

### 6. create_tables.py - 数据库初始化

**功能：**
- 自动创建数据库
- 创建表结构
- 验证表创建成功

**使用方法：**
```bash
# 修改脚本中的数据库连接参数后运行
python create_tables.py
```

## 工作流程图

### 初始化流程
```
应用启动
    ↓
调用 quick_init() 或 initialize()
    ↓
创建 RedisHealthCheck 实例
    ↓
启动定时任务（每60秒清理过期数据）
    ↓
创建 RedisFallbackService 实例
    ↓
服务就绪，可以使用
```

### 写入流程
```
调用 redis_service.set()
    ↓
检查 Redis 是否可用？
    ├─ 是 → 写入 Redis
    │         ↓
    │      异步写入 MySQL（后台线程）
    │         ↓
    │      返回成功
    │
    └─ 否 → 同步写入 MySQL
              ↓
           触发健康拨测
              ↓
           返回成功
```

### 读取流程
```
调用 redis_service.get()
    ↓
检查 Redis 是否可用？
    ├─ 是 → 尝试从 Redis 读取
    │         ├─ 成功 → 返回数据
    │         └─ 失败 → 标记 Redis 不可用
    │                    ↓
    │                 从 MySQL 读取
    │
    └─ 否 → 清理 MySQL 过期数据
              ↓
           从 MySQL 读取
              ↓
           返回数据
```

### 恢复流程
```
Redis 不可用
    ↓
健康拨测线程启动（每10秒）
    ↓
尝试获取分布式锁
    ├─ 失败 → 跳过（其他实例在执行）
    │
    └─ 成功 → 检测 Redis 是否恢复？
                ├─ 否 → 等待10秒后重试
                │
                └─ 是 → 读取 MySQL 未过期数据
                         ↓
                      回写到 Redis
                         ↓
                      标记 Redis 可用
                         ↓
                      停止健康拨测
                         ↓
                      释放锁
```

## 数据库设计

### redis_gateway_info 表

```sql
CREATE TABLE `redis_gateway_info` (
  `key` varchar(255) NOT NULL COMMENT 'Redis的key',
  `name` varchar(255) NOT NULL DEFAULT '' COMMENT 'Redis的hash name或list name',
  `value` varchar(65535) DEFAULT NULL COMMENT '存储的值(JSON序列化)',
  `expire_time` datetime DEFAULT NULL COMMENT '过期时间',
  `last_update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间',
  PRIMARY KEY (`key`,`name`),
  KEY `idx_expire_time` (`expire_time`),
  KEY `idx_last_update_time` (`last_update_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

**字段说明：**
- `key` + `name`: 联合主键
  - 普通key-value: `key`为键名，`name`为空字符串
  - Hash类型: `key`为字段名，`name`为Hash名称
  - List类型: `key`为列表名，`name`为`__list__`
  - 锁/信号量: `name`为`__lock__`或`__semaphore__`

**索引设计：**
- 主键索引: (`key`, `name`) - 用于快速查找
- `idx_expire_time`: 加速过期数据清理
- `idx_last_update_time`: 用于数据分析

## 多实例协调机制

### 分布式锁

**实现方式：**
1. Redis可用时：使用Redis的`SET NX EX`命令
2. Redis不可用时：使用MySQL表记录锁

**锁的类型：**
- `redis_service:health_check_lock`: 健康拨测锁
- `redis_service:cleanup_lock`: 数据清理锁
- 业务自定义锁：通过`lock()`方法

**锁的生命周期：**
```
获取锁 → 执行任务 → 释放锁
   ↓
 失败（锁已被占用）→ 跳过任务
```

### 定时任务协调

**场景1：数据清理**
```
实例A: 获取cleanup_lock成功 → 执行清理
实例B: 获取cleanup_lock失败 → 跳过清理
实例C: 获取cleanup_lock失败 → 跳过清理
```

**场景2：健康拨测**
```
实例A: 获取health_check_lock成功 → 执行拨测 → 回写数据
实例B: 获取health_check_lock失败 → 跳过拨测
实例C: 获取health_check_lock失败 → 跳过拨测
```

## 性能特性

### 异步写入机制

**实现原理：**
```python
def _async_write_mysql(...):
    def _write():
        # MySQL写入逻辑
        pass
    
    # 在新线程中执行
    threading.Thread(target=_write, daemon=True).start()
```

**性能对比：**
- 同步写入: 响应时间 = Redis写入时间 + MySQL写入时间
- 异步写入: 响应时间 ≈ Redis写入时间

### 连接池优化

**Redis连接池：**
```python
redis_pool = redis.ConnectionPool(
    max_connections=50,
    ...
)
```

**MySQL连接池：**
```python
engine = create_engine(
    url,
    pool_size=10,
    max_overflow=20,
    pool_recycle=3600,
    pool_pre_ping=True
)
```

## 监控建议

### 关键指标

1. **Redis可用性**
   - 指标：`redis_alive` (boolean)
   - 告警：Redis不可用超过5分钟

2. **降级状态持续时间**
   - 指标：降级开始时间到恢复时间
   - 告警：降级超过30分钟

3. **MySQL表大小**
   - 指标：`redis_gateway_info`表的大小
   - 告警：表大小超过阈值

4. **过期数据数量**
   - 指标：过期但未清理的记录数
   - 告警：过期数据超过1000条

5. **清理任务执行频率**
   - 指标：每小时清理次数
   - 告警：清理次数少于预期

### 日志监控

**重要日志：**
- `Redis标记为不可用` - Redis降级
- `Redis已恢复可用` - Redis恢复
- `清理过期数据完成` - 数据清理
- `MySQL数据回写到Redis完成` - 数据回写

## 扩展开发指南

### 添加新的Redis操作

1. 在`RedisFallbackService`类中添加方法
2. 实现Redis操作逻辑
3. 实现MySQL降级逻辑
4. 添加异常处理
5. 更新文档

**示例：添加INCR操作**
```python
def incr(self, key: str, amount: int = 1) -> int:
    """增加计数器"""
    if self.health_check.redis_alive:
        try:
            redis_service = self.get_redis_service()
            result = redis_service.incr(key, amount)
            # 异步写入MySQL
            self._async_write_mysql(key, '', str(result), None)
            return result
        except Exception as e:
            self._handle_redis_error('incr', e)
            # MySQL降级实现
            return self._incr_from_mysql(key, amount)
    else:
        return self._incr_from_mysql(key, amount)
```

### 添加新的数据类型支持

1. 设计MySQL存储方案
2. 实现写入逻辑
3. 实现读取逻辑
4. 实现数据回写逻辑
5. 添加测试用例

## 最佳实践

1. **在应用启动时初始化一次**
   ```python
   # 应用入口
   redis_service = quick_init(get_redis_cache_service, get_db_session)
   ```

2. **使用连接池**
   - Redis连接池
   - MySQL连接池

3. **合理设置TTL**
   - 热数据：短TTL
   - 冷数据：长TTL

4. **监控关键指标**
   - Redis可用性
   - MySQL表大小
   - 降级状态

5. **定期备份MySQL数据**
   ```bash
   mysqldump redis_gateway_info > backup.sql
   ```

6. **压测验证**
   - 测试降级场景
   - 测试恢复场景
   - 测试多实例协调

## 常见问题排查

### 问题1：服务初始化失败
- 检查MySQL连接配置
- 检查Redis连接配置
- 查看日志错误信息

### 问题2：数据不一致
- 检查异步写入是否正常
- 检查是否有写入异常日志
- 验证数据回写逻辑

### 问题3：多实例重复执行
- 检查分布式锁是否正常
- 查看锁获取和释放日志
- 验证MySQL锁实现

### 问题4：性能下降
- 检查MySQL慢查询
- 优化数据库索引
- 调整连接池大小

## 总结

本项目实现了一个完整的Redis降级方案，具有以下特点：

✓ **高可用**: Redis不可用时自动降级，保证服务不中断
✓ **自动恢复**: Redis恢复后自动回写数据
✓ **多实例支持**: 使用分布式锁协调多实例
✓ **易于集成**: 提供简单的初始化接口
✓ **完善的文档**: 包含详细的使用说明和示例

通过本文档，你应该能够：
- 理解项目的整体架构
- 掌握各个模块的功能
- 快速集成到现有项目
- 进行扩展开发
- 排查常见问题
