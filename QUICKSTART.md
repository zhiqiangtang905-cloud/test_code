# Redis降级MySQL服务 - 快速开始指南

## 📋 概述

这是一个完整的Redis降级到MySQL的解决方案，具有以下特点：

- ✅ **自动降级**: Redis不可用时自动切换到MySQL
- ✅ **健康拨测**: 每10秒检测Redis状态，恢复后自动同步数据
- ✅ **多实例安全**: 使用分布式锁确保多实例环境下任务不重复执行
- ✅ **定时清理**: 每60秒自动清理过期数据
- ✅ **完整的Redis操作**: 支持30+个Redis操作方法
- ✅ **线程安全**: 所有关键操作都有锁保护

## 📁 文件说明

```
workspace/
├── redis_models.py              # MySQL ORM模型（数据表定义）
├── redis_health_checker.py      # 健康检查器（监控Redis状态）
├── redis_fallback_service.py    # 降级服务主类（核心功能）
├── config_template.py           # 配置模板（复制后修改）
├── init_database.py            # 数据库初始化脚本
├── test_service.py             # 测试脚本
├── usage_example.py            # 使用示例
├── integration_guide.py        # 集成指南
├── requirements.txt            # 依赖包列表
├── README.md                   # 详细文档
└── QUICKSTART.md              # 本文件
```

## 🚀 5分钟快速开始

### 步骤1: 安装依赖

```bash
pip install -r requirements.txt
```

### 步骤2: 配置连接

复制配置模板并修改为你的实际配置：

```bash
cp config_template.py config.py
```

编辑`config.py`，修改Redis和MySQL连接信息：

```python
class RedisConfig:
    HOST = 'localhost'      # 修改为你的Redis地址
    PORT = 6379
    DB = 0
    PASSWORD = None         # 如果有密码请填写

class MySQLConfig:
    HOST = 'localhost'      # 修改为你的MySQL地址
    PORT = 3306
    USER = 'root'           # 修改为你的用户名
    PASSWORD = 'your_password'  # 修改为你的密码
    DATABASE = 'mydb'       # 修改为你的数据库名
```

### 步骤3: 测试连接

```bash
python config.py
```

确保看到：
```
✓ Redis连接成功
✓ MySQL连接成功
✓ 数据库会话管理器正常
```

### 步骤4: 初始化数据库

```bash
python init_database.py
```

按提示输入`yes`创建`redis_gateway_info`表。

### 步骤5: 在你的代码中使用

```python
from redis_fallback_service import RedisFallbackService
from config import get_redis_cache_service, get_db_session

# 初始化服务
service = RedisFallbackService(
    get_redis_cache_service,
    get_db_session
)

# 启动定时任务
service.start_scheduler()

# 使用服务（完全兼容Redis接口）
service.set_ex("mykey", "myvalue", ttl=3600)
value = service.get("mykey")

# 使用分布式锁（多实例环境）
with service.distributed_lock("my_task", timeout=60):
    # 只有一个实例会执行这里的代码
    do_important_task()
```

## 🎯 核心功能示例

### 1. 基础Key-Value操作

```python
# 设置值（带过期时间）
service.set_ex("user:1001", {"name": "张三", "age": 25}, ttl=3600)

# 获取值（自动降级）
user = service.get("user:1001")

# 检查是否存在
exists = service.exists("user:1001")

# 设置过期时间
service.expire("user:1001", 7200)

# 删除
service.delete("user:1001")
```

### 2. Hash操作

```python
# 设置Hash字段
service.hset("user:profile:1001", "nickname", "小明")
service.hset("user:profile:1001", "email", "test@example.com")

# 获取单个字段
nickname = service.hget("user:profile:1001", "nickname")

# 获取所有字段
profile = service.hgetall("user:profile:1001")

# 删除字段
service.hdel("user:profile:1001", "email")
```

### 3. List操作

```python
# 添加元素
service.rpush("task:queue", "task1", "task2", "task3")
service.lpush("task:queue", "urgent_task")

# 获取列表
tasks = service.lrange("task:queue", 0, -1)

# 获取长度
length = service.llen("task:queue")

# 弹出元素
task = service.lpop("task:queue")

# 获取指定位置元素
task = service.lindex("task:queue", 0)
```

### 4. 分布式锁（重要！多实例协调）

```python
# 使用with语句自动管理锁
try:
    with service.distributed_lock("task:process_orders", timeout=60):
        # 只有获取到锁的实例会执行这里
        process_orders()
        print("任务完成")
except RuntimeError:
    # 其他实例持有锁
    print("其他实例正在处理，跳过")
```

### 5. 信号量（限制并发）

```python
# 限制最多3个实例同时执行
if service.acquire_semaphore("api:calls", limit=3, timeout=30):
    try:
        # 执行API调用
        result = call_external_api()
    finally:
        service.release_semaphore("api:calls", identifier)
else:
    print("并发已满，稍后重试")
```

## 🔄 自动降级工作流程

### 正常情况（Redis可用）
```
写入: 应用 → Redis ✓ → MySQL（异步）✓
读取: 应用 → Redis ✓ → 返回数据
```

### Redis故障
```
1. Redis操作失败
2. 自动标记Redis不可用
3. 启动健康拨测（每10秒）
4. 所有操作降级到MySQL
```

### Redis恢复
```
1. 健康拨测检测到Redis可用
2. 自动同步MySQL数据到Redis
3. 恢复正常Redis读写
4. 停止健康拨测
```

## 📊 多实例环境使用

在多实例环境下，使用分布式锁确保任务不重复执行：

```python
# 定时任务示例
def daily_report_task():
    """每日报表任务 - 多实例环境下只执行一次"""
    try:
        # 使用分布式锁
        with service.distributed_lock("task:daily_report", timeout=300):
            print(f"[实例{os.getpid()}] 获取锁成功，开始执行任务")
            
            # 执行业务逻辑
            generate_daily_report()
            
            print(f"[实例{os.getpid()}] 任务完成")
    except RuntimeError:
        print(f"[实例{os.getpid()}] 其他实例正在执行，跳过")

# 定时调度（每个实例都运行，但只有一个会获取锁）
schedule.every().day.at("00:00").do(daily_report_task)
```

## 🎨 与主流框架集成

### Flask集成

```python
from flask import Flask
from redis_fallback_service import RedisFallbackService
from config import get_redis_cache_service, get_db_session

app = Flask(__name__)
redis_service = None

@app.before_first_request
def init():
    global redis_service
    redis_service = RedisFallbackService(
        get_redis_cache_service,
        get_db_session
    )
    redis_service.start_scheduler()

@app.route('/api/user/<user_id>')
def get_user(user_id):
    user = redis_service.get(f"user:{user_id}")
    return {"user": user}
```

### Django集成

```python
# apps.py
from django.apps import AppConfig

class MyAppConfig(AppConfig):
    def ready(self):
        from redis_fallback_service import RedisFallbackService
        from config import get_redis_cache_service, get_db_session
        
        service = RedisFallbackService(
            get_redis_cache_service,
            get_db_session
        )
        service.start_scheduler()
```

### Celery集成

```python
from celery import Celery
from redis_fallback_service import RedisFallbackService
from config import get_redis_cache_service, get_db_session

app = Celery('tasks')
service = RedisFallbackService(get_redis_cache_service, get_db_session)
service.start_scheduler()

@app.task
def process_orders():
    with service.distributed_lock("task:process_orders"):
        orders = service.lrange("orders:pending", 0, -1)
        for order in orders:
            process_order(order)
```

## ⚙️ 支持的Redis操作

### 基础操作
- `set_ex(key, value, ttl)` - 设置带过期时间的键值
- `get(key)` - 获取值
- `exists(key)` - 检查是否存在
- `delete(*keys)` - 删除键
- `expire(key, ttl)` - 设置过期时间

### Hash操作
- `hset(name, key, value)` - 设置字段
- `hget(name, key)` - 获取字段
- `hgetall(name)` - 获取所有字段
- `hdel(name, *keys)` - 删除字段

### List操作
- `rpush(name, *values)` - 右侧推入
- `lpush(name, *values)` - 左侧推入
- `lrange(name, start, end)` - 获取范围
- `lpop(key, count)` - 左侧弹出
- `lindex(key, index)` - 获取指定位置
- `llen(key)` - 获取长度
- `lrem(key, count, value)` - 删除元素

### 高级功能
- `distributed_lock(key, timeout)` - 分布式锁
- `acquire_semaphore(key, limit, timeout)` - 获取信号量
- `release_semaphore(key, identifier)` - 释放信号量

## 📝 注意事项

### 1. 数据一致性
- 所有写操作会同时写Redis和MySQL
- Redis恢复时会自动同步MySQL数据
- 使用TTL自动清理过期数据

### 2. 性能考虑
- MySQL写入是异步的，不影响Redis性能
- 健康拨测在独立线程运行
- 使用连接池减少连接开销

### 3. 多实例协调
- 关键任务必须使用分布式锁
- 定时清理自动使用分布式锁
- 只有一个实例会执行健康拨测

### 4. List操作限制
- `lpop`和`lrem`在MySQL降级时功能受限
- 建议关键List操作确保Redis可用

## 🔧 故障排查

### Redis连接失败
```bash
# 检查Redis是否运行
redis-cli ping

# 检查端口
netstat -an | grep 6379

# 查看日志
tail -f /var/log/redis/redis-server.log
```

### MySQL连接失败
```bash
# 测试连接
mysql -h localhost -u root -p

# 检查数据库
SHOW DATABASES;
```

### 表未创建
```bash
# 重新初始化
python init_database.py
```

### 分布式锁无法获取
- 检查是否有其他实例持有锁
- 适当增加timeout时间
- 确保Redis可用

## 📚 更多文档

- **详细文档**: 查看 `README.md`
- **使用示例**: 查看 `usage_example.py`
- **集成指南**: 查看 `integration_guide.py`
- **测试脚本**: 运行 `python test_service.py`

## 🆘 获取帮助

如果遇到问题：

1. 查看日志输出（设置日志级别为DEBUG）
2. 运行测试脚本验证功能
3. 检查Redis和MySQL连接
4. 查看代码注释了解详细逻辑

## ✨ 最佳实践

1. **启动时初始化**: 应用启动时创建服务实例
2. **使用单例**: 全局使用同一个服务实例
3. **合理TTL**: 所有缓存数据设置过期时间
4. **分布式锁**: 关键任务务必使用锁
5. **监控日志**: 关注降级和恢复日志
6. **压力测试**: 生产前充分测试

---

**开始使用吧！** 🎉

如有问题，请查看详细文档或运行测试脚本。
