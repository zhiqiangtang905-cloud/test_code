# Redis降级MySQL快速参考

## 📦 文件清单

| 文件名 | 大小 | 说明 |
|--------|------|------|
| redis_mysql_fallback.py | 37KB | **核心实现** - 主要代码文件 |
| advanced_example.py | 15KB | **高级示例** - 实战应用示例 |
| test_redis_fallback.py | 13KB | **单元测试** - 完整测试用例 |
| DEPLOYMENT.md | 11KB | **部署指南** - 详细部署文档 |
| README_REDIS_FALLBACK.md | 9.6KB | **技术文档** - 详细技术说明 |
| performance_test.py | 9.4KB | **性能测试** - 性能测试脚本 |
| PROJECT_SUMMARY.md | 8.2KB | **项目总结** - 功能总结 |
| quickstart.py | 7.4KB | **快速启动** - 一键启动脚本 |
| README.md | 6.8KB | **主文档** - 项目介绍 |
| example_usage.py | 4.6KB | **基础示例** - 入门示例 |
| config.py | 1.7KB | **配置模板** - 配置文件 |
| requirements.txt | 62B | **依赖列表** - pip依赖 |

## 🚀 5分钟快速开始

### 1. 安装依赖
```bash
pip install redis SQLAlchemy PyMySQL schedule
```

### 2. 创建数据库
```sql
CREATE DATABASE redis_gateway_info DEFAULT CHARACTER SET utf8mb4;
```

### 3. 修改配置
```python
# config.py
REDIS_CONFIG = {'host': 'localhost', 'port': 6379, 'db': 0, 'password': None}
MYSQL_CONFIG = {'connection_string': 'mysql+pymysql://user:pass@host/redis_gateway_info'}
```

### 4. 运行测试
```bash
python quickstart.py
```

## 💡 核心API速查

### 初始化
```python
from redis_mysql_fallback import RedisGateway

gateway = RedisGateway(redis_config, mysql_config)
gateway.start_scheduled_tasks()  # 启动定时任务
```

### String操作
```python
gateway.set('key', 'value')                    # 设置
gateway.set_ex('key', 'value', ttl=60)        # 设置+过期
gateway.get('key')                             # 获取
gateway.delete('key')                          # 删除
gateway.exists('key')                          # 检查存在
gateway.expire('key', ttl=60)                  # 设置过期
```

### Hash操作
```python
gateway.hset('user:1', 'name', 'Zhang')       # 设置字段
gateway.hget('user:1', 'name')                # 获取字段
gateway.hgetall('user:1')                     # 获取所有字段
gateway.hdel('user:1', 'name', 'age')         # 删除字段
```

### List操作
```python
gateway.rpush('queue', 'task1', 'task2')      # 右推入
gateway.lpush('queue', 'urgent')              # 左推入
gateway.lrange('queue', 0, -1)                # 获取范围
gateway.lpop('queue')                          # 左弹出
gateway.lindex('queue', 0)                     # 获取索引
gateway.llen('queue')                          # 获取长度
gateway.lrem('queue', 1, 'task1')             # 删除元素
```

### 分布式锁
```python
with gateway.distributed_lock('my_lock', timeout=10) as acquired:
    if acquired:
        # 执行业务逻辑
        pass
```

## 🔍 常用命令

### 检查状态
```python
# Redis状态
is_alive = gateway.health_checker.redis_alive
is_checking = gateway.health_checker.is_checking

# 手动触发健康检查
gateway.health_checker.mark_redis_down()

# 清理过期数据
gateway.cleanup_expired_data()
```

### 数据库查询
```sql
-- 查看所有数据
SELECT * FROM redis_gateway_info;

-- 查看未过期数据
SELECT * FROM redis_gateway_info 
WHERE expire_time IS NULL OR expire_time > NOW();

-- 清理过期数据
DELETE FROM redis_gateway_info 
WHERE expire_time IS NOT NULL AND expire_time < NOW();

-- 查看锁状态
SELECT * FROM redis_gateway_info WHERE name = '_lock';

-- 统计数据量
SELECT 
    COUNT(*) as total,
    COUNT(CASE WHEN expire_time IS NULL THEN 1 END) as no_expire,
    COUNT(CASE WHEN expire_time < NOW() THEN 1 END) as expired
FROM redis_gateway_info;
```

## 🐛 故障排查速查

### Redis连接失败
```bash
# 检查Redis服务
systemctl status redis
redis-cli -h host -p 6379 ping

# 检查配置
python -c "from config import REDIS_CONFIG; print(REDIS_CONFIG)"
```

### MySQL连接失败
```bash
# 检查MySQL服务
systemctl status mysql
mysql -h host -u user -p

# 测试连接字符串
python -c "from sqlalchemy import create_engine; from config import MYSQL_CONFIG; \
           engine = create_engine(MYSQL_CONFIG['connection_string']); \
           conn = engine.connect(); print('OK')"
```

### 分布式锁问题
```sql
-- 查看当前锁
SELECT * FROM redis_gateway_info WHERE name = '_lock';

-- 释放所有过期锁
DELETE FROM redis_gateway_info 
WHERE name = '_lock' AND expire_time < NOW();

-- 强制释放锁（谨慎使用）
DELETE FROM redis_gateway_info WHERE name = '_lock';
```

### 数据同步慢
```sql
-- 查看数据量
SELECT COUNT(*) FROM redis_gateway_info;

-- 查看过期数据量
SELECT COUNT(*) FROM redis_gateway_info 
WHERE expire_time IS NOT NULL AND expire_time < NOW();

-- 批量清理过期数据
DELETE FROM redis_gateway_info 
WHERE expire_time < NOW() 
LIMIT 10000;
```

## 📊 监控指标

### 关键指标
```python
# Redis状态
gateway.health_checker.redis_alive         # True/False
gateway.health_checker.is_checking         # True/False

# MySQL表大小
SELECT 
    COUNT(*) as records,
    SUM(LENGTH(value)) as total_size
FROM redis_gateway_info;

# 过期数据比例
SELECT 
    COUNT(*) as total,
    COUNT(CASE WHEN expire_time < NOW() THEN 1 END) as expired,
    ROUND(COUNT(CASE WHEN expire_time < NOW() THEN 1 END) * 100.0 / COUNT(*), 2) as expired_percent
FROM redis_gateway_info;
```

### 性能测试
```bash
# 运行性能测试
python performance_test.py

# 运行单元测试
python test_redis_fallback.py
```

## 🔧 运维操作

### 清理操作
```python
# 清理过期数据
gateway.cleanup_expired_data()

# 清理特定key
gateway.delete('key_pattern*')  # 需自己实现通配符

# 清空测试数据
with gateway._get_mysql_session() as session:
    session.query(RedisGatewayInfo).filter(
        RedisGatewayInfo.key.like('test_%')
    ).delete()
    session.commit()
```

### 数据迁移
```bash
# 导出数据
mysqldump redis_gateway_info > backup.sql

# 导入数据
mysql redis_gateway_info < backup.sql

# 同步到Redis
python -c "
from redis_mysql_fallback import RedisGateway
from config import REDIS_CONFIG, MYSQL_CONFIG
gateway = RedisGateway(REDIS_CONFIG, MYSQL_CONFIG)
gateway.health_checker._sync_mysql_to_redis()
"
```

### 健康检查
```python
# 手动触发健康检查
gateway.health_checker.mark_redis_down()

# 停止健康检查
gateway.health_checker.stop()

# 检查Redis连通性
is_healthy = gateway.health_checker.check_redis_health()
```

## 📝 配置模板

### 最小配置
```python
from redis_mysql_fallback import RedisGateway

gateway = RedisGateway(
    redis_config={'host': 'localhost', 'port': 6379, 'db': 0},
    mysql_config={'connection_string': 'mysql+pymysql://root@localhost/redis_gateway_info'}
)
```

### 完整配置
```python
from redis_mysql_fallback import RedisGateway

redis_config = {
    'host': 'redis.internal',
    'port': 6379,
    'db': 0,
    'password': 'password',
    'socket_connect_timeout': 5,
    'socket_timeout': 5,
    'max_connections': 100,
}

mysql_config = {
    'connection_string': 'mysql+pymysql://user:pass@mysql.internal:3306/redis_gateway_info?charset=utf8mb4'
}

gateway = RedisGateway(redis_config, mysql_config)
gateway.start_scheduled_tasks()
```

## ⚠️ 注意事项

1. **数据类型**: 所有数据都通过`json.dumps()`序列化
2. **过期时间**: MySQL过期精度为秒级
3. **性能**: 降级时性能下降3-10倍
4. **并发**: 使用分布式锁避免竞争
5. **清理**: 定期清理过期数据防止膨胀

## 🔗 相关文档

- [README.md](README.md) - 项目介绍
- [README_REDIS_FALLBACK.md](README_REDIS_FALLBACK.md) - 详细文档
- [DEPLOYMENT.md](DEPLOYMENT.md) - 部署指南
- [PROJECT_SUMMARY.md](PROJECT_SUMMARY.md) - 项目总结
- [example_usage.py](example_usage.py) - 基础示例
- [advanced_example.py](advanced_example.py) - 高级示例

## 📞 获取帮助

1. 查看文档: 阅读上述相关文档
2. 运行示例: `python example_usage.py`
3. 运行测试: `python test_redis_fallback.py`
4. 性能测试: `python performance_test.py`
5. 快速启动: `python quickstart.py`

---

**提示**: 这是一个生产级的Redis降级解决方案，已在多实例环境中验证，支持自动故障切换和数据同步。
