# Redis降级MySQL网关 - 部署指南

## 系统要求

### 软件依赖
- Python 3.7+
- Redis 5.0+
- MySQL 5.7+ / MariaDB 10.3+

### Python包依赖
- redis >= 4.5.0
- SQLAlchemy >= 2.0.0
- PyMySQL >= 1.0.0
- schedule >= 1.2.0
- cryptography >= 41.0.0

## 部署步骤

### 第1步：环境准备

#### 1.1 安装Python依赖

```bash
cd /workspace
pip install -r requirements.txt

# 或者使用虚拟环境（推荐）
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

#### 1.2 启动Redis服务

```bash
# 检查Redis是否已安装
redis-cli --version

# 启动Redis（如果未运行）
redis-server

# 测试Redis连接
redis-cli ping
# 应该返回: PONG
```

#### 1.3 启动MySQL服务

```bash
# 检查MySQL是否已安装
mysql --version

# 启动MySQL（如果未运行）
sudo systemctl start mysql
# 或者
sudo service mysql start

# 登录MySQL
mysql -u root -p
```

### 第2步：创建数据库

```sql
-- 创建数据库
CREATE DATABASE redis_gateway_info 
CHARACTER SET utf8mb4 
COLLATE utf8mb4_unicode_ci;

-- 验证数据库创建
SHOW DATABASES LIKE 'redis_gateway_info';

-- 授权（如需要）
GRANT ALL PRIVILEGES ON redis_gateway_info.* TO 'your_user'@'localhost';
FLUSH PRIVILEGES;

-- 退出MySQL
EXIT;
```

### 第3步：配置系统

编辑 `config.py` 文件，根据实际环境修改配置：

```python
# Redis配置
REDIS_CONFIG = {
    'host': 'localhost',          # 改为你的Redis服务器地址
    'port': 6379,                 # 改为你的Redis端口
    'db': 0,
    'password': None,             # 如果Redis设置了密码，在这里填写
    'decode_responses': True,
    'socket_timeout': 5,
    'socket_connect_timeout': 5,
}

# MySQL配置
MYSQL_CONFIG = {
    'host': 'localhost',          # 改为你的MySQL服务器地址
    'port': 3306,                 # 改为你的MySQL端口
    'user': 'root',               # 改为你的MySQL用户名
    'password': 'your_password',  # 改为你的MySQL密码
    'database': 'redis_gateway_info',
    'charset': 'utf8mb4',
}

# 健康拨测配置（可选）
HEALTH_CHECK_CONFIG = {
    'probe_interval': 10,         # Redis拨测间隔，建议保持默认
    'cleanup_interval': 60,       # 清理间隔，建议保持默认
    'lock_timeout': 300,          # 锁超时时间，根据任务执行时间调整
    'lock_key': 'redis_gateway:health_check:lock',
    'cleanup_lock_key': 'redis_gateway:cleanup:lock',
}
```

### 第4步：测试系统

#### 4.1 运行快速测试

```bash
python3 quick_test.py
```

期望输出：
```
✓ 网关初始化成功
✓ 写入成功: test:basic:key
✓ 读取成功: {'test': 'value', 'timestamp': '2024-01-01'}
✓ 存在性检查: True
...
所有测试通过！✓
```

#### 4.2 查看数据库表

```bash
mysql -u root -p redis_gateway_info -e "SHOW TABLES;"
```

应该看到 `redis_gateway_info` 表已创建。

#### 4.3 运行完整示例

```bash
python3 example.py
```

### 第5步：集成到项目

#### 5.1 基本使用

```python
from redis_gateway import RedisGateway
from config import REDIS_CONFIG, MYSQL_CONFIG, HEALTH_CHECK_CONFIG
import json

# 初始化网关
gateway = RedisGateway(REDIS_CONFIG, MYSQL_CONFIG, HEALTH_CHECK_CONFIG)

# 写入数据
gateway.set_ex('mykey', json.dumps({'data': 'value'}), ttl=3600)

# 读取数据
value = gateway.get('mykey')
if value:
    data = json.loads(value)
    print(data)

# 关闭网关
gateway.close()
```

#### 5.2 使用上下文管理器（推荐）

```python
with RedisGateway(REDIS_CONFIG, MYSQL_CONFIG, HEALTH_CHECK_CONFIG) as gateway:
    gateway.set_ex('mykey', json.dumps({'data': 'value'}), ttl=3600)
    value = gateway.get('mykey')
# 自动关闭资源
```

## 多实例部署

### 场景说明
在多实例部署场景下，多个应用实例共享同一个Redis和MySQL，需要确保：
1. 健康拨测只由一个实例执行
2. 定时清理只由一个实例执行
3. 业务逻辑使用分布式锁保证互斥

### 部署架构

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  实例1      │    │  实例2      │    │  实例3      │
│  Gateway    │    │  Gateway    │    │  Gateway    │
└──────┬──────┘    └──────┬──────┘    └──────┬──────┘
       │                  │                  │
       ├──────────────────┼──────────────────┤
       │                  │                  │
    ┌──▼──────────────────▼──────────────────▼──┐
    │            Redis (主)                     │
    └───────────────────────────────────────────┘
       │                  │                  │
    ┌──▼──────────────────▼──────────────────▼──┐
    │            MySQL (主)                     │
    └───────────────────────────────────────────┘
```

### 部署步骤

1. **在每台服务器上部署代码**
```bash
# 服务器1
cd /app/instance1
git clone <repo>
pip install -r requirements.txt

# 服务器2
cd /app/instance2
git clone <repo>
pip install -r requirements.txt

# 服务器3
cd /app/instance3
git clone <repo>
pip install -r requirements.txt
```

2. **配置相同的Redis和MySQL连接**
所有实例的 `config.py` 应指向同一个Redis和MySQL服务器。

3. **启动实例**
```bash
# 每台服务器上
python3 your_app.py
```

4. **验证分布式锁**
```python
# 在每个实例中运行此测试
def test_distributed():
    with gateway.distributed_lock('test_lock', timeout=10):
        print(f"实例 {instance_id} 获得锁")
        time.sleep(5)
        print(f"实例 {instance_id} 释放锁")
```

只有一个实例能同时获得锁。

## 生产环境建议

### 1. 连接池配置

编辑 `redis_gateway/models.py`，调整连接池参数：

```python
self.engine = create_engine(
    db_url,
    pool_size=20,        # 增加连接池大小
    max_overflow=40,     # 增加最大溢出连接数
    pool_pre_ping=True,
    pool_recycle=3600,
    echo=False
)
```

### 2. 线程池配置

编辑 `redis_gateway/redis_mysql_gateway.py`，调整线程池大小：

```python
self.executor = ThreadPoolExecutor(
    max_workers=20,  # 增加工作线程数
    thread_name_prefix="MySQLWriter"
)
```

### 3. 日志配置

根据需要调整日志级别：

```python
# 在应用入口处
import logging

logging.basicConfig(
    level=logging.INFO,  # 生产环境使用INFO或WARNING
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('redis_gateway.log'),  # 写入文件
        logging.StreamHandler()  # 同时输出到控制台
    ]
)
```

### 4. 监控指标

建议监控以下指标：

- Redis连接状态
- MySQL连接池使用率
- 健康拨测频率
- 过期数据清理数量
- 异步写入队列长度
- 分布式锁获取失败率

### 5. 定时任务配置

如果使用schedule，建议：

```python
import schedule
import time

def job():
    # 使用分布式锁包装
    with gateway.distributed_lock('scheduled_job', timeout=300, blocking=False):
        # 执行定时任务
        pass

# 每小时执行一次
schedule.every().hour.do(job)

while True:
    schedule.run_pending()
    time.sleep(60)
```

### 6. 高可用配置

#### Redis高可用（Sentinel）

```python
from redis.sentinel import Sentinel

sentinel = Sentinel([
    ('sentinel1', 26379),
    ('sentinel2', 26379),
    ('sentinel3', 26379)
], socket_timeout=0.1)

REDIS_CONFIG = {
    'master_name': 'mymaster',
    'sentinel': sentinel,
    # ... 其他配置
}
```

#### MySQL主从配置

```python
# 主库（写）
MYSQL_CONFIG_MASTER = {
    'host': 'mysql-master',
    'port': 3306,
    # ...
}

# 从库（读）
MYSQL_CONFIG_SLAVE = {
    'host': 'mysql-slave',
    'port': 3306,
    # ...
}
```

## 性能优化

### 1. 数据库索引

确保创建了必要的索引：

```sql
USE redis_gateway_info;

-- 检查索引
SHOW INDEX FROM redis_gateway_info;

-- 如果没有，手动创建
CREATE INDEX idx_expire_time ON redis_gateway_info(expire_time);
CREATE INDEX idx_last_update_time ON redis_gateway_info(last_update_time);
```

### 2. MySQL配置优化

编辑 `/etc/mysql/my.cnf`：

```ini
[mysqld]
# InnoDB配置
innodb_buffer_pool_size = 1G
innodb_log_file_size = 256M
innodb_flush_log_at_trx_commit = 2

# 连接配置
max_connections = 500
```

### 3. Redis配置优化

编辑 `/etc/redis/redis.conf`：

```ini
# 内存配置
maxmemory 2gb
maxmemory-policy allkeys-lru

# 持久化配置
save 900 1
save 300 10
save 60 10000
```

## 故障排查

### 问题1：Redis连接失败

**症状**：
```
ERROR - Redis操作失败: Connection refused
WARNING - 检测到Redis不可用，开启健康拨测
```

**解决方案**：
1. 检查Redis是否运行：`redis-cli ping`
2. 检查防火墙：`sudo ufw status`
3. 检查配置：确保 `config.py` 中的地址和端口正确
4. 检查网络：`telnet redis_host 6379`

### 问题2：MySQL连接失败

**症状**：
```
ERROR - MySQL写入失败: Access denied
```

**解决方案**：
1. 检查MySQL是否运行：`sudo systemctl status mysql`
2. 检查用户权限：
```sql
SHOW GRANTS FOR 'your_user'@'localhost';
```
3. 检查密码：尝试手动登录
```bash
mysql -h localhost -u your_user -p
```

### 问题3：数据不同步

**症状**：Redis和MySQL数据不一致

**解决方案**：
1. 检查异步写入队列：查看日志中是否有写入失败
2. 手动触发同步：
```python
gateway.health_check._sync_mysql_to_redis()
```
3. 检查过期时间设置

### 问题4：分布式锁获取失败

**症状**：
```
RuntimeError: 无法获取分布式锁
```

**解决方案**：
1. 检查锁是否过期：查看Redis中的锁key
```bash
redis-cli GET redis_gateway:cleanup:lock
```
2. 如果锁卡住，手动释放：
```bash
redis-cli DEL redis_gateway:cleanup:lock
```
3. 调整锁超时时间

### 问题5：内存占用过高

**症状**：进程内存持续增长

**解决方案**：
1. 减少线程池大小
2. 增加过期数据清理频率
3. 限制写入速率
4. 检查是否有内存泄漏

## 维护指南

### 日常维护

1. **监控日志**
```bash
tail -f redis_gateway.log | grep ERROR
```

2. **检查数据库大小**
```sql
SELECT 
    COUNT(*) as total_records,
    SUM(LENGTH(value)) as total_size_bytes
FROM redis_gateway_info;
```

3. **检查过期数据**
```sql
SELECT COUNT(*) 
FROM redis_gateway_info 
WHERE expire_time < NOW() AND expire_time IS NOT NULL;
```

### 备份策略

1. **Redis备份**
```bash
redis-cli BGSAVE
cp /var/lib/redis/dump.rdb /backup/redis_$(date +%Y%m%d).rdb
```

2. **MySQL备份**
```bash
mysqldump -u root -p redis_gateway_info > backup_$(date +%Y%m%d).sql
```

### 升级指南

1. 备份数据
2. 停止应用
3. 更新代码
4. 运行测试：`python3 quick_test.py`
5. 重启应用
6. 验证功能

## 安全建议

1. **网络安全**
   - Redis和MySQL只监听内网地址
   - 使用防火墙限制访问
   - 启用SSL/TLS连接

2. **认证安全**
   - Redis设置强密码
   - MySQL使用独立账号
   - 定期更换密码

3. **数据安全**
   - 敏感数据加密存储
   - 定期备份
   - 设置合理的过期时间

## 支持与反馈

如遇到问题：
1. 查看日志文件
2. 阅读文档：README_CN.md
3. 查看项目概览：PROJECT_OVERVIEW.md
4. 运行测试：quick_test.py

---

部署完成！祝使用顺利！
