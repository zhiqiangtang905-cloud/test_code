"""
集成指南 - 如何将Redis降级服务集成到现有项目中
"""

# ==================== 方案1: 直接在现有代码中使用 ====================

def integration_method_1():
    """
    方法1: 在现有项目中直接使用
    
    假设你的项目中已有：
    - from your_module import get_redis_cache_service, get_db_session
    """
    
    # 步骤1: 导入服务
    from redis_fallback_service import RedisFallbackService
    
    # 步骤2: 在项目启动时初始化（例如在app.py或main.py中）
    # 假设你已经有get_redis_cache_service和get_db_session
    # from your_module import get_redis_cache_service, get_db_session
    
    # redis_service = RedisFallbackService(
    #     get_redis_cache_service,
    #     get_db_session
    # )
    
    # 步骤3: 启动定时任务调度器
    # redis_service.start_scheduler()
    
    # 步骤4: 在业务代码中使用
    # redis_service.set_ex("mykey", "myvalue", ttl=3600)
    # value = redis_service.get("mykey")
    
    pass


# ==================== 方案2: 创建单例服务 ====================

class RedisServiceSingleton:
    """
    方法2: 创建单例模式的服务
    确保整个应用中只有一个服务实例
    """
    
    _instance = None
    _service = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def initialize(self, get_redis_func, get_db_session_func):
        """初始化服务"""
        if self._service is None:
            from redis_fallback_service import RedisFallbackService
            self._service = RedisFallbackService(get_redis_func, get_db_session_func)
            self._service.start_scheduler()
    
    def get_service(self):
        """获取服务实例"""
        if self._service is None:
            raise RuntimeError("服务未初始化，请先调用initialize()")
        return self._service


def integration_method_2():
    """
    使用单例服务
    """
    
    # 在应用启动时初始化
    # from your_module import get_redis_cache_service, get_db_session
    # singleton = RedisServiceSingleton()
    # singleton.initialize(get_redis_cache_service, get_db_session)
    
    # 在业务代码中使用
    # singleton = RedisServiceSingleton()
    # service = singleton.get_service()
    # service.set_ex("mykey", "myvalue", ttl=3600)
    
    pass


# ==================== 方案3: 作为装饰器使用 ====================

def with_redis_fallback(lock_key: str = None, timeout: int = 30):
    """
    方法3: 装饰器模式
    用于需要分布式锁的函数
    
    Args:
        lock_key: 分布式锁的键名，None表示不使用锁
        timeout: 锁超时时间
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            # 获取服务实例
            singleton = RedisServiceSingleton()
            service = singleton.get_service()
            
            if lock_key:
                # 使用分布式锁
                try:
                    with service.distributed_lock(lock_key, timeout):
                        return func(*args, **kwargs)
                except RuntimeError:
                    # 无法获取锁
                    import logging
                    logging.warning(f"无法获取锁: {lock_key}，跳过执行")
                    return None
            else:
                # 不使用锁，直接执行
                return func(*args, **kwargs)
        
        return wrapper
    return decorator


# 使用装饰器的示例
@with_redis_fallback(lock_key="task:daily_report", timeout=300)
def daily_report_task():
    """
    每日报表任务
    使用分布式锁确保多实例环境下只有一个实例执行
    """
    print("执行每日报表任务...")
    # 你的业务逻辑
    pass


# ==================== 方案4: Flask集成示例 ====================

def integration_with_flask():
    """
    方法4: 与Flask框架集成
    """
    
    # 示例代码（不会实际运行）
    example_code = '''
    from flask import Flask
    from redis_fallback_service import RedisFallbackService
    from your_module import get_redis_cache_service, get_db_session
    
    app = Flask(__name__)
    
    # 全局服务实例
    redis_service = None
    
    @app.before_first_request
    def initialize_services():
        """应用启动时初始化服务"""
        global redis_service
        redis_service = RedisFallbackService(
            get_redis_cache_service,
            get_db_session
        )
        redis_service.start_scheduler()
        app.logger.info("Redis降级服务已初始化")
    
    @app.route('/api/user/<user_id>')
    def get_user(user_id):
        """获取用户信息"""
        # 从Redis获取（自动降级）
        user_data = redis_service.get(f"user:{user_id}")
        if user_data:
            return {"user": user_data}
        return {"error": "用户不存在"}, 404
    
    @app.route('/api/user/<user_id>', methods=['POST'])
    def update_user(user_id):
        """更新用户信息"""
        from flask import request
        user_data = request.json
        
        # 写入Redis（自动同步到MySQL）
        redis_service.set_ex(f"user:{user_id}", user_data, ttl=3600)
        return {"success": True}
    
    @app.teardown_appcontext
    def cleanup(error):
        """应用退出时清理"""
        if error:
            app.logger.error(f"应用错误: {error}")
    
    if __name__ == '__main__':
        app.run()
    '''
    
    print(example_code)


# ==================== 方案5: Django集成示例 ====================

def integration_with_django():
    """
    方法5: 与Django框架集成
    """
    
    # 示例代码（不会实际运行）
    example_code = '''
    # 在Django的settings.py中配置
    
    # settings.py
    REDIS_FALLBACK_SERVICE = None
    
    def initialize_redis_service():
        from redis_fallback_service import RedisFallbackService
        from your_module import get_redis_cache_service, get_db_session
        
        global REDIS_FALLBACK_SERVICE
        if REDIS_FALLBACK_SERVICE is None:
            REDIS_FALLBACK_SERVICE = RedisFallbackService(
                get_redis_cache_service,
                get_db_session
            )
            REDIS_FALLBACK_SERVICE.start_scheduler()
    
    # 在apps.py中初始化
    from django.apps import AppConfig
    
    class MyAppConfig(AppConfig):
        default_auto_field = 'django.db.models.BigAutoField'
        name = 'myapp'
        
        def ready(self):
            """应用启动时初始化"""
            from django.conf import settings
            settings.initialize_redis_service()
    
    # 在views.py中使用
    from django.http import JsonResponse
    from django.conf import settings
    
    def get_user_view(request, user_id):
        """获取用户视图"""
        service = settings.REDIS_FALLBACK_SERVICE
        user_data = service.get(f"user:{user_id}")
        
        if user_data:
            return JsonResponse({"user": user_data})
        return JsonResponse({"error": "用户不存在"}, status=404)
    '''
    
    print(example_code)


# ==================== 方案6: 定时任务集成 ====================

def integration_with_celery():
    """
    方法6: 与Celery定时任务集成
    """
    
    # 示例代码（不会实际运行）
    example_code = '''
    # celery_tasks.py
    from celery import Celery
    from redis_fallback_service import RedisFallbackService
    from your_module import get_redis_cache_service, get_db_session
    
    app = Celery('tasks')
    
    # 初始化服务
    redis_service = RedisFallbackService(
        get_redis_cache_service,
        get_db_session
    )
    redis_service.start_scheduler()
    
    @app.task
    def process_orders():
        """处理订单任务"""
        # 使用分布式锁确保只有一个worker执行
        try:
            with redis_service.distributed_lock("task:process_orders", timeout=300):
                # 获取待处理订单
                orders = redis_service.lrange("orders:pending", 0, -1)
                
                for order in orders:
                    # 处理订单
                    process_single_order(order)
                    
                    # 从队列中移除
                    redis_service.lrem("orders:pending", 1, order)
                
                return {"processed": len(orders)}
        except RuntimeError:
            # 其他worker正在处理
            return {"skipped": True}
    
    @app.task
    def cache_user_data(user_id, user_data):
        """缓存用户数据"""
        redis_service.set_ex(f"user:{user_id}", user_data, ttl=3600)
        return {"cached": True}
    '''
    
    print(example_code)


# ==================== 完整的应用示例 ====================

def complete_application_example():
    """
    完整应用示例：展示如何在真实项目中使用
    """
    
    print("""
    # ============================================================
    # 完整应用示例
    # ============================================================
    
    # 1. 项目结构
    myproject/
    ├── app.py                          # 主应用
    ├── redis_models.py                 # 数据库模型
    ├── redis_health_checker.py         # 健康检查
    ├── redis_fallback_service.py       # 降级服务
    ├── config.py                       # 配置文件
    └── requirements.txt                # 依赖
    
    # 2. config.py - 配置文件
    class Config:
        # Redis配置
        REDIS_HOST = 'localhost'
        REDIS_PORT = 6379
        REDIS_DB = 0
        
        # MySQL配置
        MYSQL_HOST = 'localhost'
        MYSQL_PORT = 3306
        MYSQL_USER = 'root'
        MYSQL_PASSWORD = 'password'
        MYSQL_DATABASE = 'mydb'
        
        # SQLAlchemy配置
        SQLALCHEMY_DATABASE_URI = (
            f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}"
            f"@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
        )
    
    # 3. app.py - 主应用
    from flask import Flask
    import redis
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from contextlib import contextmanager
    
    from config import Config
    from redis_fallback_service import RedisFallbackService
    
    app = Flask(__name__)
    app.config.from_object(Config)
    
    # Redis连接
    redis_client = redis.Redis(
        host=Config.REDIS_HOST,
        port=Config.REDIS_PORT,
        db=Config.REDIS_DB,
        decode_responses=True
    )
    
    # 数据库连接
    engine = create_engine(Config.SQLALCHEMY_DATABASE_URI)
    SessionLocal = sessionmaker(bind=engine)
    
    # 获取Redis服务的函数
    def get_redis_cache_service():
        return redis_client
    
    # 获取数据库会话的函数
    @contextmanager
    def get_db_session():
        session = SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    # 初始化Redis降级服务
    redis_service = RedisFallbackService(
        get_redis_cache_service,
        get_db_session
    )
    
    @app.before_first_request
    def initialize():
        # 启动定时任务调度器
        redis_service.start_scheduler()
        app.logger.info("Redis降级服务已启动")
    
    @app.route('/api/user/<user_id>')
    def get_user(user_id):
        # 使用降级服务获取用户数据
        user_data = redis_service.get(f"user:{user_id}")
        if user_data:
            return {"user": user_data}
        return {"error": "用户不存在"}, 404
    
    @app.route('/api/cache', methods=['POST'])
    def set_cache():
        from flask import request
        data = request.json
        key = data.get('key')
        value = data.get('value')
        ttl = data.get('ttl', 3600)
        
        redis_service.set_ex(key, value, ttl=ttl)
        return {"success": True}
    
    if __name__ == '__main__':
        app.run(debug=True)
    
    # ============================================================
    # 使用说明
    # ============================================================
    
    1. 安装依赖: pip install -r requirements.txt
    2. 配置数据库连接（修改config.py）
    3. 初始化数据库: python init_database.py
    4. 启动应用: python app.py
    5. 应用会自动：
       - 优先使用Redis
       - Redis故障时降级到MySQL
       - Redis恢复后自动同步数据
       - 每60秒清理过期数据
    
    # ============================================================
    """)


if __name__ == "__main__":
    print("Redis降级服务集成指南\n")
    print("选择你的集成方式：")
    print("1. 直接使用")
    print("2. 单例模式")
    print("3. 装饰器模式")
    print("4. Flask集成")
    print("5. Django集成")
    print("6. Celery集成")
    print("7. 完整应用示例")
    
    complete_application_example()
