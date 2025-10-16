# Redis降级MySQL部署指南

## 部署前准备

### 1. 环境要求

- Python 3.7+
- Redis (推荐3.0+)
- MySQL 5.7+ 或 MariaDB 10.2+
- 操作系统：Linux/macOS/Windows

### 2. 安装Python依赖

```bash
pip install -r requirements.txt
```

依赖说明：
- `redis>=4.5.0` - Redis客户端
- `SQLAlchemy>=1.4.0` - ORM框架
- `PyMySQL>=1.0.0` - MySQL驱动
- `schedule>=1.1.0` - 任务调度器

## 数据库配置

### 1. 创建MySQL数据库

```sql
-- 创建数据库
CREATE DATABASE redis_gateway_info DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- 创建用户（可选）
CREATE USER 'redis_gateway'@'%' IDENTIFIED BY 'your_password';
GRANT ALL PRIVILEGES ON redis_gateway_info.* TO 'redis_gateway'@'%';
FLUSH PRIVILEGES;
```

### 2. 表结构

表结构会在首次运行时自动创建，包含以下字段：

```sql
CREATE TABLE redis_gateway_info (
    `key` VARCHAR(255) NOT NULL COMMENT 'Redis键',
    `name` VARCHAR(255) NOT NULL COMMENT 'Redis名称',
    `value` TEXT COMMENT 'JSON序列化后的值',
    `expire_time` DATETIME COMMENT '过期时间',
    `last_update_time` DATETIME NOT NULL COMMENT '最后更新时间',
    PRIMARY KEY (`key`, `name`),
    INDEX idx_expire_time (expire_time),
    INDEX idx_last_update_time (last_update_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

## 配置文件

### 1. 编辑config.py

```python
# Redis配置
REDIS_CONFIG = {
    'host': 'your-redis-host',      # Redis主机地址
    'port': 6379,                    # Redis端口
    'db': 0,                         # 数据库编号
    'password': 'your-password',     # 密码（如果有）
    'socket_connect_timeout': 5,     # 连接超时
    'socket_timeout': 5,             # 操作超时
}

# MySQL配置
MYSQL_CONFIG = {
    'connection_string': 'mysql+pymysql://用户名:密码@主机:端口/数据库名?charset=utf8mb4'
}
```

### 2. 配置示例

**本地开发环境**:
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

**生产环境**:
```python
REDIS_CONFIG = {
    'host': 'redis-cluster.internal',
    'port': 6379,
    'db': 0,
    'password': 'prod_password',
    'socket_connect_timeout': 5,
    'socket_timeout': 5,
    'max_connections': 100,
}

MYSQL_CONFIG = {
    'connection_string': 'mysql+pymysql://redis_gateway:secure_pass@mysql.internal:3306/redis_gateway_info?charset=utf8mb4'
}
```

## 单机部署

### 1. 基础部署

```bash
# 安装依赖
pip install -r requirements.txt

# 配置数据库连接
vim config.py

# 运行快速启动脚本检查配置
python quickstart.py

# 集成到应用
from redis_mysql_fallback import RedisGateway
from config import REDIS_CONFIG, MYSQL_CONFIG

gateway = RedisGateway(REDIS_CONFIG, MYSQL_CONFIG)
gateway.start_scheduled_tasks()
```

### 2. 以服务方式运行

创建systemd服务文件 `/etc/systemd/system/redis-gateway.service`:

```ini
[Unit]
Description=Redis Gateway Service
After=network.target mysql.service redis.service

[Service]
Type=simple
User=www-data
WorkingDirectory=/path/to/your/app
ExecStart=/usr/bin/python3 /path/to/your/app/main.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

启动服务：
```bash
sudo systemctl daemon-reload
sudo systemctl enable redis-gateway
sudo systemctl start redis-gateway
sudo systemctl status redis-gateway
```

## 多实例部署

### 1. 部署架构

```
┌─────────────┐   ┌─────────────┐   ┌─────────────┐
│  实例 1     │   │  实例 2     │   │  实例 3     │
│ (Gateway)   │   │ (Gateway)   │   │ (Gateway)   │
└──────┬──────┘   └──────┬──────┘   └──────┬──────┘
       │                 │                 │
       └─────────────────┼─────────────────┘
                         │
            ┌────────────┴────────────┐
            │                         │
        ┌───▼────┐              ┌─────▼─────┐
        │ Redis  │              │   MySQL   │
        └────────┘              └───────────┘
```

### 2. 关键配置

所有实例使用相同的Redis和MySQL配置：

```python
# 所有实例共享的配置
REDIS_CONFIG = {'host': 'redis.internal', ...}
MYSQL_CONFIG = {'connection_string': 'mysql+pymysql://...'}
```

### 3. 分布式锁机制

系统自动通过MySQL分布式锁确保：
- ✅ 只有一个实例执行健康检查
- ✅ 只有一个实例执行数据同步
- ✅ 业务层分布式锁支持

### 4. 实例启动

在每个服务器上：
```bash
# 实例1
python main.py

# 实例2  
python main.py

# 实例3
python main.py
```

## Docker部署

### 1. 创建Dockerfile

```dockerfile
FROM python:3.9-slim

WORKDIR /app

# 安装依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制代码
COPY . .

# 运行应用
CMD ["python", "main.py"]
```

### 2. 创建docker-compose.yml

```yaml
version: '3.8'

services:
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data
    command: redis-server --appendonly yes

  mysql:
    image: mysql:8.0
    environment:
      MYSQL_ROOT_PASSWORD: rootpass
      MYSQL_DATABASE: redis_gateway_info
      MYSQL_USER: redis_gateway
      MYSQL_PASSWORD: gateway123
    ports:
      - "3306:3306"
    volumes:
      - mysql_data:/var/lib/mysql

  app:
    build: .
    depends_on:
      - redis
      - mysql
    environment:
      REDIS_HOST: redis
      REDIS_PORT: 6379
      MYSQL_HOST: mysql
      MYSQL_PORT: 3306
      MYSQL_USER: redis_gateway
      MYSQL_PASSWORD: gateway123
    deploy:
      replicas: 3  # 3个实例

volumes:
  redis_data:
  mysql_data:
```

### 3. 启动

```bash
docker-compose up -d
docker-compose ps
docker-compose logs -f app
```

## Kubernetes部署

### 1. ConfigMap配置

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: redis-gateway-config
data:
  config.py: |
    REDIS_CONFIG = {
        'host': 'redis-service',
        'port': 6379,
        'db': 0,
    }
    MYSQL_CONFIG = {
        'connection_string': 'mysql+pymysql://user:pass@mysql-service:3306/redis_gateway_info?charset=utf8mb4'
    }
```

### 2. Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: redis-gateway
spec:
  replicas: 3
  selector:
    matchLabels:
      app: redis-gateway
  template:
    metadata:
      labels:
        app: redis-gateway
    spec:
      containers:
      - name: gateway
        image: your-registry/redis-gateway:latest
        volumeMounts:
        - name: config
          mountPath: /app/config.py
          subPath: config.py
      volumes:
      - name: config
        configMap:
          name: redis-gateway-config
```

## 监控和告警

### 1. 日志监控

系统输出详细日志，建议配置日志收集：

```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/var/log/redis-gateway/app.log'),
        logging.StreamHandler()
    ]
)
```

### 2. 关键指标

监控以下指标：
- Redis连接状态
- MySQL连接状态  
- 健康检查频率
- 降级切换次数
- 数据同步耗时
- MySQL表大小

### 3. 告警规则

建议配置以下告警：
- Redis不可用超过5分钟
- MySQL连接失败
- 数据同步失败
- 锁超时频繁
- MySQL表大小超过阈值

## 性能优化

### 1. MySQL优化

```sql
-- 定期分析表
ANALYZE TABLE redis_gateway_info;

-- 查看索引使用情况
SHOW INDEX FROM redis_gateway_info;

-- 清理过期数据（可以配置定时任务）
DELETE FROM redis_gateway_info 
WHERE expire_time IS NOT NULL 
AND expire_time < NOW()
LIMIT 10000;
```

### 2. 连接池优化

```python
MYSQL_CONFIG = {
    'connection_string': '...',
    'pool_size': 20,        # 根据并发调整
    'max_overflow': 40,     # 最大溢出连接
    'pool_pre_ping': True,  # 连接前测试
    'pool_recycle': 3600,   # 1小时回收连接
}
```

### 3. Redis优化

```python
REDIS_CONFIG = {
    'max_connections': 100,     # 连接池大小
    'socket_keepalive': True,   # 保持连接
    'health_check_interval': 30,# 健康检查间隔
}
```

## 故障排查

### 常见问题

**1. MySQL连接失败**
```bash
# 检查MySQL服务
systemctl status mysql

# 检查连接
mysql -h host -u user -p

# 检查防火墙
telnet mysql-host 3306
```

**2. Redis连接失败**
```bash
# 检查Redis服务
systemctl status redis

# 测试连接
redis-cli -h host -p 6379 ping
```

**3. 分布式锁超时**
```sql
-- 查看锁状态
SELECT * FROM redis_gateway_info 
WHERE name = '_lock';

-- 清理过期锁
DELETE FROM redis_gateway_info 
WHERE name = '_lock' 
AND expire_time < NOW();
```

**4. 数据同步慢**
```sql
-- 检查表大小
SELECT 
    COUNT(*) as total_records,
    COUNT(CASE WHEN expire_time IS NULL THEN 1 END) as no_expire,
    COUNT(CASE WHEN expire_time < NOW() THEN 1 END) as expired
FROM redis_gateway_info;

-- 清理过期数据
DELETE FROM redis_gateway_info 
WHERE expire_time < NOW();
```

## 安全建议

1. **网络安全**
   - Redis和MySQL不暴露到公网
   - 使用VPC或内网通信
   - 配置防火墙规则

2. **认证安全**
   - Redis配置密码
   - MySQL使用强密码
   - 限制用户权限

3. **数据安全**
   - 敏感数据加密存储
   - 定期备份MySQL
   - 配置Redis持久化

4. **代码安全**
   - 配置文件不提交到代码仓库
   - 使用环境变量管理敏感信息
   - 定期更新依赖版本

## 升级维护

### 版本升级

```bash
# 备份数据
mysqldump redis_gateway_info > backup.sql

# 更新代码
git pull origin main

# 更新依赖
pip install -r requirements.txt --upgrade

# 重启服务
systemctl restart redis-gateway
```

### 数据迁移

```bash
# 导出数据
mysqldump -h old-host redis_gateway_info > data.sql

# 导入到新环境
mysql -h new-host redis_gateway_info < data.sql
```

## 回滚计划

1. 保留上一个稳定版本
2. 备份数据库
3. 准备回滚脚本
4. 测试回滚流程

```bash
# 回滚脚本示例
#!/bin/bash
git checkout v1.0.0
pip install -r requirements.txt
mysql redis_gateway_info < backup_v1.0.0.sql
systemctl restart redis-gateway
```

## 联系支持

如遇问题，请检查：
1. [详细文档](README_REDIS_FALLBACK.md)
2. [故障排查](#故障排查)
3. 提交Issue到GitHub
