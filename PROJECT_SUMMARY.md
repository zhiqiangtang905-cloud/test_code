# Redis降级MySQL项目总结

## 📁 项目文件说明

### 核心文件

1. **redis_mysql_fallback.py** - 核心实现
   - `RedisGatewayInfo` - 数据库模型
   - `RedisHealthChecker` - 健康检查类
   - `RedisGateway` - Redis网关主类

2. **config.py** - 配置文件模板
   - Redis连接配置
   - MySQL连接配置
   - 健康检查配置
   - 分布式锁配置

### 示例文件

3. **example_usage.py** - 基础使用示例
   - String操作示例
   - Hash操作示例
   - List操作示例
   - 分布式锁示例

4. **advanced_example.py** - 高级功能示例
   - `CacheService` - 缓存服务封装
   - 用户缓存管理
   - 会话管理
   - 任务队列
   - 配置管理
   - 限流器
   - 分布式任务协调

5. **quickstart.py** - 快速启动脚本
   - 依赖检查
   - 连接测试
   - 交互式演示

### 测试文件

6. **test_redis_fallback.py** - 单元测试
   - String操作测试
   - Hash操作测试
   - List操作测试
   - 分布式锁测试
   - 健康检查测试
   - 数据同步测试

7. **performance_test.py** - 性能测试
   - 基础操作性能
   - 并发性能
   - 故障切换性能

### 文档文件

8. **README.md** - 项目主文档
9. **README_REDIS_FALLBACK.md** - 详细技术文档
10. **DEPLOYMENT.md** - 部署指南
11. **requirements.txt** - 依赖列表

## 🎯 核心功能实现

### 1. Redis操作降级

#### String操作
- ✅ `set(key, value)` - 设置值
- ✅ `set_ex(key, value, ttl)` - 设置带过期时间的值
- ✅ `get(key)` - 获取值
- ✅ `delete(key)` - 删除键
- ✅ `exists(key)` - 检查键是否存在
- ✅ `expire(key, ttl)` - 设置过期时间

#### Hash操作
- ✅ `hset(name, key, value)` - 设置Hash字段
- ✅ `hget(name, key)` - 获取Hash字段
- ✅ `hdel(name, *keys)` - 删除Hash字段
- ✅ `hgetall(name)` - 获取所有Hash字段

#### List操作
- ✅ `rpush(name, *values)` - 从右侧推入
- ✅ `lpush(name, *values)` - 从左侧推入
- ✅ `lrange(name, start, end)` - 获取列表范围
- ✅ `lpop(key, count)` - 从左侧弹出
- ✅ `lindex(key, index)` - 获取指定索引元素
- ✅ `llen(key)` - 获取列表长度
- ✅ `lrem(key, count, value)` - 删除指定值

#### 分布式锁
- ✅ `distributed_lock(lock_key, timeout)` - 分布式锁上下文管理器

### 2. 健康检查机制

- ✅ Redis状态监控
- ✅ 自动故障检测
- ✅ 定期健康拨测（每10秒）
- ✅ 自动恢复和数据同步
- ✅ 分布式锁确保单实例执行

### 3. 数据同步

- ✅ 写入时异步同步到MySQL
- ✅ Redis恢复时从MySQL回写
- ✅ 过期时间同步
- ✅ 支持JSON序列化

### 4. 多实例支持

- ✅ MySQL分布式锁
- ✅ 健康检查互斥
- ✅ 数据同步互斥
- ✅ 业务层分布式锁

### 5. 定时任务

- ✅ 定期清理过期数据（60秒）
- ✅ Schedule调度器
- ✅ 独立线程运行

## 📊 技术架构

### 数据流向

```
写入流程:
应用 → RedisGateway.set() → Redis (同步)
                            ↓
                        MySQL (异步)

读取流程 (正常):
应用 → RedisGateway.get() → Redis → 返回数据

读取流程 (降级):
应用 → RedisGateway.get() → MySQL → 返回数据
                            ↑
                    (清理过期数据)

恢复流程:
健康检查 → 检测到Redis恢复 → 从MySQL同步数据 → Redis
```

### 数据库设计

**表名**: `redis_gateway_info`

| 字段 | 类型 | 说明 | 索引 |
|------|------|------|------|
| key | VARCHAR(255) | Redis键 | 主键 |
| name | VARCHAR(255) | Redis名称/字段名 | 主键 |
| value | TEXT | JSON序列化的值 | - |
| expire_time | DATETIME | 过期时间 | ✓ |
| last_update_time | DATETIME | 最后更新时间 | ✓ |

**特殊name值**:
- `_string` - String类型数据
- `_list_<index>` - List类型元素
- `_lock` - 分布式锁
- 其他 - Hash字段名

## 🚀 使用场景

### 1. 缓存降级
```python
# 用户信息缓存
gateway.hset('user:1001', 'name', 'Zhang San')
gateway.hset('user:1001', 'age', '25')

# Redis故障时自动从MySQL读取
user_name = gateway.hget('user:1001', 'name')
```

### 2. 会话管理
```python
# 创建会话
session_data = {'user_id': 123, 'role': 'admin'}
gateway.set_ex('session:abc123', json.dumps(session_data), ttl=1800)

# 降级后仍可读取
session = gateway.get('session:abc123')
```

### 3. 任务队列
```python
# 添加任务
gateway.rpush('queue:email', json.dumps(task))

# 处理任务（支持降级）
task = gateway.lpop('queue:email')
```

### 4. 分布式协调
```python
# 确保只有一个实例执行
with gateway.distributed_lock('task:daily_report', timeout=300) as acquired:
    if acquired:
        # 执行任务
        run_daily_report()
```

### 5. 限流控制
```python
# 检查请求频率
def check_rate_limit(user_id):
    key = f'rate_limit:{user_id}'
    count = gateway.get(key)
    
    if count and int(count) > 100:
        return False
    
    if count:
        gateway.set_ex(key, str(int(count) + 1), ttl=60)
    else:
        gateway.set_ex(key, '1', ttl=60)
    
    return True
```

## ⚙️ 配置示例

### 开发环境
```python
REDIS_CONFIG = {
    'host': 'localhost',
    'port': 6379,
    'db': 0,
    'password': None,
}

MYSQL_CONFIG = {
    'connection_string': 'mysql+pymysql://root:root@localhost:3306/redis_gateway_info?charset=utf8mb4'
}
```

### 生产环境
```python
REDIS_CONFIG = {
    'host': 'redis-cluster.internal',
    'port': 6379,
    'db': 0,
    'password': 'secure_password',
    'socket_connect_timeout': 5,
    'socket_timeout': 5,
    'max_connections': 100,
}

MYSQL_CONFIG = {
    'connection_string': 'mysql+pymysql://redis_gw:password@mysql.internal:3306/redis_gateway_info?charset=utf8mb4'
}
```

## 📈 性能指标

### 预期性能

**正常情况 (Redis)**:
- Set操作: < 5ms
- Get操作: < 3ms
- Hash操作: < 5ms
- List操作: < 10ms

**降级情况 (MySQL)**:
- Get操作: 10-50ms
- 性能下降: 3-10倍

**并发性能**:
- 支持数百并发连接
- 吞吐量取决于Redis/MySQL性能

### 优化建议

1. **连接池**: 根据并发量调整pool_size
2. **批量操作**: 使用rpush多值、hset批量
3. **索引优化**: 确保expire_time和last_update_time有索引
4. **定期清理**: 配置自动清理过期数据
5. **监控告警**: 监控Redis状态和MySQL性能

## 🔧 运维指南

### 启动服务
```bash
# 开发环境
python quickstart.py

# 生产环境
python main.py
```

### 健康检查
```bash
# 查看Redis状态
redis-cli -h host -p 6379 ping

# 查看MySQL连接
mysql -h host -u user -p -e "SELECT COUNT(*) FROM redis_gateway_info;"
```

### 监控指标
- Redis连接状态
- MySQL表大小
- 降级切换次数
- 健康检查频率
- 数据同步耗时

### 故障处理

**Redis故障**:
1. 系统自动降级到MySQL
2. 启动健康检查
3. Redis恢复后自动同步

**MySQL故障**:
1. Redis继续正常工作
2. 异步写入失败会记录日志
3. 修复MySQL后数据会继续同步

**数据不一致**:
1. 重启健康检查流程
2. 手动触发数据同步
3. 检查分布式锁状态

## 🐛 已知限制

1. **数据类型**: 主要支持String、Hash、List
2. **过期精度**: MySQL过期时间为秒级
3. **性能**: 降级时性能下降3-10倍
4. **存储**: MySQL不适合大量临时数据
5. **一致性**: 异步写入有短暂延迟

## 🔜 后续优化方向

1. **支持更多Redis操作**: Set、ZSet等
2. **性能优化**: 批量写入、缓存预热
3. **监控增强**: Prometheus指标导出
4. **管理界面**: Web管理后台
5. **自动扩缩容**: 基于负载的实例调整

## 📞 技术支持

- 文档: [README_REDIS_FALLBACK.md](README_REDIS_FALLBACK.md)
- 部署: [DEPLOYMENT.md](DEPLOYMENT.md)
- 示例: [example_usage.py](example_usage.py), [advanced_example.py](advanced_example.py)
- 测试: [test_redis_fallback.py](test_redis_fallback.py)

## 📝 版本信息

**当前版本**: v1.0.0

**更新日志**:
- v1.0.0: 初始版本，实现核心功能

---

**总结**: 这是一个完整的Redis降级MySQL解决方案，支持多实例部署，具备自动故障检测、健康检查、数据同步等功能。通过分布式锁确保多实例环境下的任务唯一性，适用于微服务和分布式系统场景。
