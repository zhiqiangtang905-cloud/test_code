# Redis降级MySQL服务 - 快速开始指南

## 5分钟快速上手

### 第一步：安装依赖（1分钟）

```bash
pip install -r requirements.txt
```

### 第二步：配置环境变量（1分钟）

```bash
# 复制配置文件
cp .env.example .env

# 编辑.env文件，修改MySQL密码
vim .env  # 或使用其他编辑器
```

至少需要修改：
```ini
MYSQL_PASSWORD=your_password_here
```

### 第三步：初始化数据库（1分钟）

```bash
python create_tables.py
```

按提示输入 `y` 确认创建数据库和表。

### 第四步：集成到你的项目（2分钟）

**方式A：最简单的方式（推荐）**

```python
# 在你的应用入口文件中
from init_service import quick_init

# 假设你的项目中已有这两个函数
from your_project import get_redis_cache_service, get_db_session

# 一行代码完成初始化
redis_service = quick_init(get_redis_cache_service, get_db_session)

# 立即使用（API与原生Redis完全一致）
redis_service.set("key", "value")
value = redis_service.get("key")
```

**方式B：如果你的项目还没有Redis和MySQL连接**

```python
from init_service import quick_init
import redis
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager

# 创建Redis连接函数
def get_redis_cache_service():
    return redis.Redis(
        host='localhost',
        port=6379,
        db=0,
        decode_responses=True
    )

# 创建MySQL连接函数
def get_db_session():
    engine = create_engine(
        'mysql+pymysql://root:password@localhost:3306/redis_gateway_info'
    )
    SessionLocal = sessionmaker(bind=engine)
    
    @contextmanager
    def session_scope():
        session = SessionLocal()
        try:
            yield session
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    return session_scope()

# 初始化服务
redis_service = quick_init(get_redis_cache_service, get_db_session)
```

### 完成！开始使用

```python
# 基础操作
redis_service.set("user:1001", {"name": "张三", "age": 25})
user = redis_service.get("user:1001")
print(user)  # {'name': '张三', 'age': 25}

# Hash操作
redis_service.hset("user:profile:1001", "city", "北京")
city = redis_service.hget("user:profile:1001", "city")
print(city)  # 北京

# List操作
redis_service.rpush("tasks", "任务1", "任务2")
tasks = redis_service.lrange("tasks", 0, -1)
print(tasks)  # ['任务1', '任务2']

# 分布式锁
with redis_service.lock("my_lock", ttl=30) as acquired:
    if acquired:
        print("获取锁成功，执行业务逻辑")
        # 你的业务代码
```

## 验证服务是否正常工作

### 测试1：基础读写

```python
# 写入测试数据
redis_service.set("test:hello", "world")

# 读取测试数据
result = redis_service.get("test:hello")
print(f"测试结果: {result}")  # 应该输出: 测试结果: world

# 清理测试数据
redis_service.delete("test:hello")
```

### 测试2：降级功能（可选）

```python
from init_service import get_manager

# 查看Redis状态
manager = get_manager()
print(f"Redis状态: {'可用' if manager.is_redis_alive() else '不可用'}")

# 手动标记Redis为不可用（测试降级）
manager.get_health_check().mark_redis_down()

# 此时仍然可以正常读写（自动降级到MySQL）
redis_service.set("test:fallback", "works")
result = redis_service.get("test:fallback")
print(f"降级测试: {result}")  # 应该输出: 降级测试: works
```

### 测试3：查看MySQL数据（可选）

```sql
-- 连接到MySQL
mysql -u root -p

-- 切换到数据库
USE redis_gateway_info;

-- 查看存储的数据
SELECT * FROM redis_gateway_info LIMIT 10;
```

## 常见使用场景

### 场景1：缓存用户信息

```python
def get_user_info(user_id):
    """获取用户信息（带缓存）"""
    cache_key = f"user:info:{user_id}"
    
    # 先从缓存读取
    user_info = redis_service.get(cache_key)
    
    if user_info is None:
        # 缓存未命中，从数据库加载
        user_info = load_user_from_db(user_id)
        
        # 写入缓存（1小时过期）
        redis_service.set_ex(cache_key, user_info, ttl=3600)
    
    return user_info
```

### 场景2：消息队列

```python
# 生产者：添加任务到队列
def add_task(task_data):
    redis_service.rpush("task:queue", task_data)

# 消费者：从队列获取任务
def process_tasks():
    while True:
        task = redis_service.lpop("task:queue")
        if task:
            # 处理任务
            handle_task(task)
        else:
            time.sleep(1)  # 队列为空，等待
```

### 场景3：分布式锁防止重复执行

```python
def run_daily_report():
    """执行每日报告（确保只运行一次）"""
    lock_key = f"report:daily:{datetime.now().strftime('%Y%m%d')}"
    
    with redis_service.lock(lock_key, ttl=3600) as acquired:
        if acquired:
            # 成功获取锁，执行报告生成
            generate_report()
            print("报告生成完成")
        else:
            # 其他实例正在执行
            print("报告正在生成中，跳过")
```

### 场景4：限流（使用信号量）

```python
def limited_api_call():
    """限制并发调用（最多3个）"""
    if redis_service.acquire_semaphore("api:limit", limit=3, ttl=60):
        try:
            # 执行API调用
            result = call_external_api()
            return result
        finally:
            # 信号量会自动过期释放
            pass
    else:
        raise Exception("并发数已达上限，请稍后再试")
```

## 核心特性说明

### ✓ 自动降级
- Redis可用时：优先使用Redis，异步备份到MySQL
- Redis不可用时：自动切换到MySQL，保证服务不中断

### ✓ 自动恢复
- Redis恢复后：自动检测并回写数据
- 无需人工干预：完全自动化

### ✓ 多实例支持
- 使用分布式锁：确保多个实例协同工作
- 避免重复操作：健康检查和数据清理只有一个实例执行

### ✓ 数据一致性
- 异步写入：不影响性能
- 降级场景：同步写入MySQL，保证数据不丢失

### ✓ 定时清理
- 每60秒：自动清理过期数据
- 节省存储：及时释放空间

## 监控建议

### 方法1：通过代码监控

```python
from init_service import get_manager

# 定期检查Redis状态
manager = get_manager()
if not manager.is_redis_alive():
    # 发送告警
    send_alert("Redis降级到MySQL")
```

### 方法2：通过日志监控

关注以下日志关键词：
- `Redis标记为不可用` - Redis降级
- `Redis已恢复可用` - Redis恢复
- `清理过期数据完成` - 数据清理执行

### 方法3：通过数据库监控

```sql
-- 检查表大小
SELECT 
    ROUND(((data_length + index_length) / 1024 / 1024), 2) AS size_mb
FROM information_schema.TABLES
WHERE table_schema = 'redis_gateway_info'
AND table_name = 'redis_gateway_info';

-- 检查记录数
SELECT COUNT(*) FROM redis_gateway_info;

-- 检查过期数据
SELECT COUNT(*) FROM redis_gateway_info 
WHERE expire_time IS NOT NULL AND expire_time <= NOW();
```

## 性能优化提示

### 1. 使用连接池
```python
# Redis连接池
redis_pool = redis.ConnectionPool(max_connections=50)

def get_redis_cache_service():
    return redis.Redis(connection_pool=redis_pool)
```

### 2. 合理设置TTL
```python
# 热数据：5分钟
redis_service.set_ex("hot:data", value, ttl=300)

# 温数据：1小时
redis_service.set_ex("warm:data", value, ttl=3600)

# 冷数据：24小时
redis_service.set_ex("cold:data", value, ttl=86400)
```

### 3. 避免存储大对象
```python
# ❌ 不好的做法
redis_service.set("big:data", huge_dict_with_10mb_data)

# ✓ 好的做法：分拆存储
for chunk_id, chunk_data in split_data(huge_data):
    redis_service.set(f"data:{chunk_id}", chunk_data)
```

## 故障排查

### 问题1：初始化失败

**症状：**
```
Error: Can't connect to MySQL server
```

**解决方法：**
1. 检查MySQL是否运行：`systemctl status mysql`
2. 检查连接配置：查看.env文件
3. 测试连接：`mysql -h localhost -u root -p`

### 问题2：Redis连接超时

**症状：**
```
redis.exceptions.TimeoutError
```

**解决方法：**
1. 检查Redis是否运行：`redis-cli ping`
2. 检查防火墙设置
3. 增加超时时间：在.env中设置`REDIS_SOCKET_TIMEOUT=5`

### 问题3：数据读取为None

**可能原因：**
- 数据已过期
- key不存在
- Redis和MySQL都没有数据

**排查步骤：**
```python
# 1. 检查key是否存在
exists = redis_service.exists("your:key")
print(f"Key存在: {exists}")

# 2. 直接查询MySQL
# SELECT * FROM redis_gateway_info WHERE `key` = 'your:key';

# 3. 检查过期时间
# SELECT expire_time FROM redis_gateway_info WHERE `key` = 'your:key';
```

## 下一步

1. **查看完整文档**: 阅读 README.md
2. **了解架构设计**: 阅读 PROJECT_STRUCTURE.md
3. **查看示例代码**: 
   - simple_example.py - 简单示例
   - production_example.py - 生产环境示例
4. **生产部署**: 根据实际环境调整配置

## 获取帮助

- 查看文档：README.md、PROJECT_STRUCTURE.md
- 查看示例：example_usage.py、production_example.py
- 查看源码：redis_fallback_service.py、health_check.py

## 总结

通过以上步骤，你应该已经：
- ✓ 成功安装和配置了服务
- ✓ 了解了基本使用方法
- ✓ 知道了如何集成到项目
- ✓ 掌握了常见使用场景

现在你可以开始在项目中使用Redis降级服务了！

**记住：这个服务完全透明，你只需要像使用普通Redis一样使用它，所有的降级和恢复都是自动的！**
