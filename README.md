# Redis to MySQL Fallback Gateway System

[中文文档](README_CN.md)

A complete Redis to MySQL seamless fallback system with automatic failure detection, health probing, and data synchronization. Supports multi-instance deployment.

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Connection

Edit `config.py`:

```python
REDIS_CONFIG = {
    'host': 'localhost',
    'port': 6379,
    'db': 0,
    'password': None,
}

MYSQL_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'your_password',
    'database': 'redis_gateway_info',
}
```

### 3. Create Database

```sql
CREATE DATABASE redis_gateway_info CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
```

### 4. Usage Example

```python
from redis_gateway import RedisGateway
from config import REDIS_CONFIG, MYSQL_CONFIG, HEALTH_CHECK_CONFIG
import json

# Use context manager (recommended)
with RedisGateway(REDIS_CONFIG, MYSQL_CONFIG, HEALTH_CHECK_CONFIG) as gateway:
    # Write data
    gateway.set_ex('user:1001', json.dumps({'name': 'John', 'age': 25}), ttl=3600)
    
    # Read data
    value = gateway.get('user:1001')
    user = json.loads(value)
    
    # Hash operations
    gateway.hset('user:profile:1001', 'city', json.dumps('Beijing'))
    city = gateway.hget('user:profile:1001', 'city')
    
    # Distributed lock
    with gateway.distributed_lock('my_task_lock', timeout=10):
        # Execute mutually exclusive operations
        pass
```

### 5. Run Example

```bash
python example.py
```

## Key Features

### Core Features
- ✅ **Redis Operation Fallback**: Auto fallback to MySQL for common Redis operations
- ✅ **Read-Write Separation**: Read from Redis first, fallback to MySQL on failure; Write to Redis then async write to MySQL
- ✅ **Health Probing**: Auto health check when Redis fails, auto data sync when recovered
- ✅ **Expiration Management**: Support TTL setting, scheduled cleanup of expired data
- ✅ **Distributed Lock**: Ensure task mutual exclusion in multi-instance scenarios
- ✅ **Async Write**: Use thread pool for async MySQL writes, non-blocking

### Supported Redis Operations

#### Basic Operations
- `set_ex(key, value, ttl)` - Set key-value with expiration
- `get(key)` - Get key value
- `exists(key)` - Check if key exists
- `delete(key)` - Delete key
- `expire(key, ttl)` - Set expiration time

#### Hash Operations
- `hset(name, key, value)` - Set hash field
- `hget(name, key)` - Get hash field value
- `hdel(name, key)` - Delete hash field

#### List Operations
- `lpush(name, value)` - Insert from left
- `rpush(name, value)` - Insert from right
- `lpop(key, count)` - Pop from left
- `lrange(name, start, end)` - Get list range
- `lindex(key, index)` - Get element at index
- `lrem(key, count, value)` - Remove elements

#### Distributed Lock
- `distributed_lock(lock_key, timeout)` - Context manager style distributed lock

## System Architecture

### Data Flow

```
Write Flow:
User Code -> RedisGateway -> Redis (sync) -> MySQL (async)
                           |
                           +---> On failure -> Mark Redis down -> Start health probe

Read Flow:
User Code -> RedisGateway -> Redis (priority)
                           |
                           +---> On failure/unavailable -> MySQL (fallback)
```

### Health Probe Flow

```
Redis Failure Detection -> Set redis_alive=False -> Start probe thread
                                                     |
                                                     v
                                                Ping Redis every 10s
                                                     |
                                                     v
                                                Redis recovered?
                                                     |
                                                     +---> Yes -> Sync MySQL data to Redis
                                                     |          -> Set redis_alive=True
                                                     |          -> Stop probe
                                                     |
                                                     +---> No -> Continue probing
```

## Important Notes

### 1. Data Consistency
- Short data inconsistency window between Redis and MySQL (async write)
- Read operations prioritize Redis for latest data
- May lose data not written to MySQL during failover

### 2. List Operation Limitations
Due to MySQL table structure constraints:
- `lpop`: Not supported in MySQL, returns None
- `lindex`: Not supported in MySQL, returns None
- List element order in MySQL may differ from Redis

### 3. Multi-Instance Deployment
Use distributed locks to ensure only one instance executes tasks:

```python
def cleanup_task():
    with gateway.distributed_lock('cleanup_lock', timeout=300):
        # Only instance with lock will execute
        print("Executing cleanup...")
```

## Project Structure

```
.
├── config.py                      # Configuration
├── requirements.txt               # Dependencies
├── example.py                     # Usage examples
├── README.md                      # English documentation
├── README_CN.md                   # Chinese documentation
└── redis_gateway/                 # Core package
    ├── __init__.py               # Package init
    ├── models.py                 # Database models
    ├── health_check.py           # Health check class
    └── redis_mysql_gateway.py    # Core gateway class
```

## License

MIT License
