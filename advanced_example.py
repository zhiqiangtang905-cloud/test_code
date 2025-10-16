"""
高级使用示例：在实际项目中集成Redis降级MySQL
"""

import logging
from redis_mysql_fallback import RedisGateway
from config import REDIS_CONFIG, MYSQL_CONFIG, HEALTH_CHECK_CONFIG, LOGGING_CONFIG
import time
import json
from datetime import datetime
from threading import Thread

# 配置日志
logging.basicConfig(
    level=getattr(logging, LOGGING_CONFIG['level']),
    format=LOGGING_CONFIG['format'],
    handlers=[
        logging.FileHandler(LOGGING_CONFIG['file']),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class CacheService:
    """缓存服务类 - 封装Redis网关，提供业务级别的缓存操作"""
    
    def __init__(self):
        """初始化缓存服务"""
        self.gateway = RedisGateway(REDIS_CONFIG, MYSQL_CONFIG)
        self.gateway.start_scheduled_tasks()
        logger.info("缓存服务初始化完成")
    
    # ==================== 用户缓存 ====================
    
    def cache_user(self, user_id: int, user_data: dict, ttl: int = 3600):
        """
        缓存用户数据
        
        Args:
            user_id: 用户ID
            user_data: 用户数据字典
            ttl: 缓存时间（秒）
        """
        cache_key = f'user:{user_id}'
        
        # 存储完整用户信息
        self.gateway.set_ex(cache_key, json.dumps(user_data), ttl=ttl)
        
        # 同时存储用户名索引
        if 'username' in user_data:
            index_key = f'user:index:{user_data["username"]}'
            self.gateway.set_ex(index_key, str(user_id), ttl=ttl)
        
        logger.info(f"用户数据已缓存: user_id={user_id}")
    
    def get_user(self, user_id: int) -> dict:
        """
        获取缓存的用户数据
        
        Args:
            user_id: 用户ID
            
        Returns:
            用户数据字典，不存在返回None
        """
        cache_key = f'user:{user_id}'
        data = self.gateway.get(cache_key)
        
        if data:
            return json.loads(data)
        return None
    
    def get_user_by_username(self, username: str) -> dict:
        """
        通过用户名获取用户数据
        
        Args:
            username: 用户名
            
        Returns:
            用户数据字典
        """
        index_key = f'user:index:{username}'
        user_id = self.gateway.get(index_key)
        
        if user_id:
            return self.get_user(int(user_id))
        return None
    
    def invalidate_user(self, user_id: int):
        """
        使用户缓存失效
        
        Args:
            user_id: 用户ID
        """
        cache_key = f'user:{user_id}'
        self.gateway.delete(cache_key)
        logger.info(f"用户缓存已失效: user_id={user_id}")
    
    # ==================== 会话缓存 ====================
    
    def create_session(self, session_id: str, user_id: int, ttl: int = 1800):
        """
        创建会话
        
        Args:
            session_id: 会话ID
            user_id: 用户ID
            ttl: 会话有效期（秒）
        """
        session_key = f'session:{session_id}'
        session_data = {
            'user_id': user_id,
            'created_at': datetime.now().isoformat(),
            'last_access': datetime.now().isoformat()
        }
        self.gateway.set_ex(session_key, json.dumps(session_data), ttl=ttl)
        logger.info(f"会话已创建: session_id={session_id}, user_id={user_id}")
    
    def get_session(self, session_id: str) -> dict:
        """
        获取会话信息
        
        Args:
            session_id: 会话ID
            
        Returns:
            会话数据字典
        """
        session_key = f'session:{session_id}'
        data = self.gateway.get(session_key)
        
        if data:
            session_data = json.loads(data)
            # 更新最后访问时间
            session_data['last_access'] = datetime.now().isoformat()
            self.gateway.set_ex(session_key, json.dumps(session_data), ttl=1800)
            return session_data
        return None
    
    def delete_session(self, session_id: str):
        """
        删除会话
        
        Args:
            session_id: 会话ID
        """
        session_key = f'session:{session_id}'
        self.gateway.delete(session_key)
        logger.info(f"会话已删除: session_id={session_id}")
    
    # ==================== 任务队列 ====================
    
    def enqueue_task(self, queue_name: str, task_data: dict, priority: str = 'normal'):
        """
        添加任务到队列
        
        Args:
            queue_name: 队列名称
            task_data: 任务数据
            priority: 优先级 (high/normal/low)
        """
        queue_key = f'queue:{queue_name}'
        task = {
            'id': f"{int(time.time() * 1000)}",
            'data': task_data,
            'priority': priority,
            'enqueued_at': datetime.now().isoformat()
        }
        
        if priority == 'high':
            # 高优先级任务从左侧推入
            self.gateway.lpush(queue_key, json.dumps(task))
        else:
            # 普通优先级任务从右侧推入
            self.gateway.rpush(queue_key, json.dumps(task))
        
        logger.info(f"任务已入队: queue={queue_name}, task_id={task['id']}, priority={priority}")
    
    def dequeue_task(self, queue_name: str) -> dict:
        """
        从队列取出任务
        
        Args:
            queue_name: 队列名称
            
        Returns:
            任务数据字典
        """
        queue_key = f'queue:{queue_name}'
        task_json = self.gateway.lpop(queue_key)
        
        if task_json:
            task = json.loads(task_json)
            logger.info(f"任务已出队: queue={queue_name}, task_id={task['id']}")
            return task
        return None
    
    def get_queue_length(self, queue_name: str) -> int:
        """
        获取队列长度
        
        Args:
            queue_name: 队列名称
            
        Returns:
            队列长度
        """
        queue_key = f'queue:{queue_name}'
        return self.gateway.llen(queue_key)
    
    # ==================== 配置缓存 ====================
    
    def set_config(self, config_key: str, config_value: any, ttl: int = None):
        """
        设置配置项
        
        Args:
            config_key: 配置键
            config_value: 配置值
            ttl: 过期时间（秒），None表示永不过期
        """
        cache_key = f'config:{config_key}'
        value = json.dumps(config_value)
        
        if ttl:
            self.gateway.set_ex(cache_key, value, ttl=ttl)
        else:
            self.gateway.set(cache_key, value)
        
        logger.info(f"配置已设置: key={config_key}")
    
    def get_config(self, config_key: str, default=None):
        """
        获取配置项
        
        Args:
            config_key: 配置键
            default: 默认值
            
        Returns:
            配置值
        """
        cache_key = f'config:{config_key}'
        value = self.gateway.get(cache_key)
        
        if value:
            return json.loads(value)
        return default
    
    # ==================== 计数器 ====================
    
    def increment_counter(self, counter_name: str, increment: int = 1):
        """
        增加计数器
        
        Args:
            counter_name: 计数器名称
            increment: 增量
        """
        counter_key = f'counter:{counter_name}'
        current = self.gateway.get(counter_key)
        
        if current:
            new_value = int(current) + increment
        else:
            new_value = increment
        
        self.gateway.set(counter_key, str(new_value))
        logger.info(f"计数器更新: {counter_name} = {new_value}")
    
    def get_counter(self, counter_name: str) -> int:
        """
        获取计数器值
        
        Args:
            counter_name: 计数器名称
            
        Returns:
            计数器值
        """
        counter_key = f'counter:{counter_name}'
        value = self.gateway.get(counter_key)
        return int(value) if value else 0
    
    # ==================== 限流器 ====================
    
    def check_rate_limit(self, identifier: str, max_requests: int, window: int) -> bool:
        """
        检查是否超过限流
        
        Args:
            identifier: 标识符（如用户ID、IP等）
            max_requests: 时间窗口内最大请求数
            window: 时间窗口（秒）
            
        Returns:
            是否允许请求（True表示允许）
        """
        key = f'rate_limit:{identifier}'
        
        # 使用分布式锁确保原子性
        with self.gateway.distributed_lock(f'{key}:lock', timeout=2) as acquired:
            if not acquired:
                return False
            
            current = self.gateway.get(key)
            
            if current is None:
                # 首次请求
                self.gateway.set_ex(key, '1', ttl=window)
                return True
            
            count = int(current)
            if count >= max_requests:
                logger.warning(f"限流触发: {identifier} 超过 {max_requests} 次请求")
                return False
            
            # 增加计数
            self.gateway.set_ex(key, str(count + 1), ttl=window)
            return True
    
    # ==================== 分布式任务协调 ====================
    
    def execute_once(self, task_name: str, task_func, *args, **kwargs):
        """
        确保任务在多实例环境下只执行一次
        
        Args:
            task_name: 任务名称
            task_func: 任务函数
            *args, **kwargs: 任务函数参数
        """
        lock_key = f'task_lock:{task_name}'
        
        with self.gateway.distributed_lock(lock_key, timeout=300) as acquired:
            if acquired:
                logger.info(f"获取任务锁成功，开始执行: {task_name}")
                try:
                    result = task_func(*args, **kwargs)
                    logger.info(f"任务执行完成: {task_name}")
                    return result
                except Exception as e:
                    logger.error(f"任务执行失败: {task_name}, 错误: {e}", exc_info=True)
                    raise
            else:
                logger.info(f"其他实例正在执行任务: {task_name}")
                return None
    
    def close(self):
        """关闭缓存服务"""
        self.gateway.close()
        logger.info("缓存服务已关闭")


# ==================== 使用示例 ====================

def example_user_cache():
    """用户缓存示例"""
    cache_service = CacheService()
    
    # 缓存用户数据
    user_data = {
        'id': 1001,
        'username': 'zhangsan',
        'email': 'zhangsan@example.com',
        'nickname': '张三'
    }
    cache_service.cache_user(1001, user_data, ttl=3600)
    
    # 获取用户数据
    user = cache_service.get_user(1001)
    print(f"用户数据: {user}")
    
    # 通过用户名查找
    user = cache_service.get_user_by_username('zhangsan')
    print(f"通过用户名查找: {user}")
    
    cache_service.close()


def example_task_queue():
    """任务队列示例"""
    cache_service = CacheService()
    
    # 添加任务
    cache_service.enqueue_task('email_queue', {
        'to': 'user@example.com',
        'subject': '欢迎注册',
        'body': '感谢您的注册！'
    }, priority='high')
    
    cache_service.enqueue_task('email_queue', {
        'to': 'admin@example.com',
        'subject': '系统通知',
        'body': '新用户注册'
    }, priority='normal')
    
    # 查看队列长度
    length = cache_service.get_queue_length('email_queue')
    print(f"队列长度: {length}")
    
    # 处理任务
    task = cache_service.dequeue_task('email_queue')
    if task:
        print(f"处理任务: {task}")
    
    cache_service.close()


def example_rate_limit():
    """限流示例"""
    cache_service = CacheService()
    
    user_id = 'user_12345'
    
    # 模拟10次请求，限制为5次/10秒
    for i in range(10):
        allowed = cache_service.check_rate_limit(user_id, max_requests=5, window=10)
        print(f"请求 {i+1}: {'允许' if allowed else '拒绝（限流）'}")
        time.sleep(0.5)
    
    cache_service.close()


def example_distributed_task():
    """分布式任务示例"""
    cache_service = CacheService()
    
    def heavy_task(task_name):
        """模拟重任务"""
        print(f"开始执行重任务: {task_name}")
        time.sleep(3)
        print(f"重任务完成: {task_name}")
        return "success"
    
    # 在多个线程中尝试执行（模拟多实例）
    def worker(worker_id):
        result = cache_service.execute_once(
            'daily_report',
            heavy_task,
            'daily_report_2024'
        )
        if result:
            print(f"Worker {worker_id} 成功执行任务")
        else:
            print(f"Worker {worker_id} 任务被其他实例执行")
    
    threads = []
    for i in range(3):
        t = Thread(target=worker, args=(i,))
        t.start()
        threads.append(t)
    
    for t in threads:
        t.join()
    
    cache_service.close()


def example_config_management():
    """配置管理示例"""
    cache_service = CacheService()
    
    # 设置系统配置
    cache_service.set_config('system.maintenance', False)
    cache_service.set_config('system.max_upload_size', 10 * 1024 * 1024)  # 10MB
    cache_service.set_config('feature.new_ui', True, ttl=3600)  # 1小时后过期
    
    # 读取配置
    maintenance = cache_service.get_config('system.maintenance', default=False)
    max_size = cache_service.get_config('system.max_upload_size', default=5*1024*1024)
    new_ui = cache_service.get_config('feature.new_ui', default=False)
    
    print(f"维护模式: {maintenance}")
    print(f"最大上传大小: {max_size} bytes")
    print(f"新UI功能: {new_ui}")
    
    cache_service.close()


if __name__ == '__main__':
    print("=== Redis降级MySQL高级示例 ===\n")
    
    print("1. 用户缓存示例")
    example_user_cache()
    print()
    
    print("2. 任务队列示例")
    example_task_queue()
    print()
    
    print("3. 限流示例")
    example_rate_limit()
    print()
    
    print("4. 分布式任务示例")
    example_distributed_task()
    print()
    
    print("5. 配置管理示例")
    example_config_management()
    print()
    
    print("=== 示例完成 ===")
