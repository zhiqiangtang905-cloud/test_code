"""
Redis降级服务初始化模块
提供简单的初始化接口，方便集成到现有项目
"""
import logging
from typing import Callable, Optional
from health_check import get_health_check, RedisHealthCheck
from redis_fallback_service import get_redis_fallback_service, RedisFallbackService

logger = logging.getLogger(__name__)


class RedisFallbackServiceManager:
    """
    Redis降级服务管理器
    封装初始化逻辑，提供简单的使用接口
    """
    
    def __init__(self):
        self._health_check: Optional[RedisHealthCheck] = None
        self._fallback_service: Optional[RedisFallbackService] = None
        self._initialized = False
    
    def initialize(
        self, 
        get_redis_service_func: Callable,
        get_db_session_func: Callable,
        auto_start_schedule: bool = True
    ) -> RedisFallbackService:
        """
        初始化Redis降级服务
        
        Args:
            get_redis_service_func: 获取Redis服务的函数（你项目中的get_redis_cache_service）
            get_db_session_func: 获取数据库会话的函数（你项目中的get_db_session）
            auto_start_schedule: 是否自动启动定时任务（默认True）
            
        Returns:
            RedisFallbackService: 降级服务实例
            
        Example:
            from init_service import RedisFallbackServiceManager
            from your_project import get_redis_cache_service, get_db_session
            
            # 初始化服务
            manager = RedisFallbackServiceManager()
            redis_service = manager.initialize(
                get_redis_cache_service,
                get_db_session
            )
            
            # 使用服务
            redis_service.set("key", "value")
        """
        if self._initialized:
            logger.warning("Redis降级服务已初始化，返回现有实例")
            return self._fallback_service
        
        try:
            # 1. 初始化健康检查服务
            self._health_check = get_health_check(
                get_redis_service_func,
                get_db_session_func
            )
            logger.info("健康检查服务初始化成功")
            
            # 2. 启动定时任务
            if auto_start_schedule:
                self._health_check.start_schedule()
                logger.info("定时清理任务已启动（每60秒执行一次）")
            
            # 3. 初始化降级服务
            self._fallback_service = get_redis_fallback_service(
                get_redis_service_func,
                get_db_session_func,
                self._health_check
            )
            logger.info("Redis降级服务初始化成功")
            
            self._initialized = True
            
            logger.info("=" * 60)
            logger.info("Redis降级服务已就绪")
            logger.info("- 支持自动降级到MySQL")
            logger.info("- 支持自动健康拨测和恢复")
            logger.info("- 支持多实例分布式协调")
            logger.info("=" * 60)
            
            return self._fallback_service
            
        except Exception as e:
            logger.error(f"初始化Redis降级服务失败: {e}", exc_info=True)
            raise
    
    def get_service(self) -> RedisFallbackService:
        """
        获取降级服务实例
        
        Returns:
            RedisFallbackService: 降级服务实例
            
        Raises:
            RuntimeError: 如果服务未初始化
        """
        if not self._initialized or self._fallback_service is None:
            raise RuntimeError(
                "Redis降级服务未初始化，请先调用initialize()方法"
            )
        return self._fallback_service
    
    def get_health_check(self) -> RedisHealthCheck:
        """
        获取健康检查服务实例
        
        Returns:
            RedisHealthCheck: 健康检查服务实例
            
        Raises:
            RuntimeError: 如果服务未初始化
        """
        if not self._initialized or self._health_check is None:
            raise RuntimeError(
                "健康检查服务未初始化，请先调用initialize()方法"
            )
        return self._health_check
    
    def stop(self):
        """
        停止降级服务
        主要是停止定时任务
        """
        if self._health_check:
            self._health_check.stop_schedule()
            logger.info("定时任务已停止")
        
        self._initialized = False
        logger.info("Redis降级服务已停止")
    
    def is_redis_alive(self) -> bool:
        """
        检查Redis是否可用
        
        Returns:
            bool: Redis是否可用
        """
        if self._health_check:
            return self._health_check.redis_alive
        return False
    
    def force_check_redis(self):
        """
        强制执行一次Redis健康检查
        用于手动触发检查
        """
        if self._health_check:
            if self._health_check._check_redis_alive():
                logger.info("Redis健康检查通过")
                if not self._health_check.redis_alive:
                    # Redis已恢复
                    self._health_check._sync_mysql_to_redis()
                    self._health_check.redis_alive = True
                    self._health_check.is_probing = False
                    logger.info("Redis已恢复，数据已同步")
            else:
                logger.warning("Redis健康检查失败")
                self._health_check.mark_redis_down()
        else:
            logger.error("健康检查服务未初始化")


# 全局管理器实例（单例）
_manager_instance: Optional[RedisFallbackServiceManager] = None


def get_manager() -> RedisFallbackServiceManager:
    """
    获取全局管理器实例（单例模式）
    
    Returns:
        RedisFallbackServiceManager: 管理器实例
    """
    global _manager_instance
    if _manager_instance is None:
        _manager_instance = RedisFallbackServiceManager()
    return _manager_instance


def quick_init(
    get_redis_service_func: Callable,
    get_db_session_func: Callable,
    auto_start_schedule: bool = True
) -> RedisFallbackService:
    """
    快速初始化Redis降级服务（推荐使用）
    
    这是一个便捷函数，封装了完整的初始化流程
    
    Args:
        get_redis_service_func: 获取Redis服务的函数
        get_db_session_func: 获取数据库会话的函数
        auto_start_schedule: 是否自动启动定时任务
        
    Returns:
        RedisFallbackService: 降级服务实例
        
    Example:
        from init_service import quick_init
        from your_project import get_redis_cache_service, get_db_session
        
        # 一行代码完成初始化
        redis_service = quick_init(get_redis_cache_service, get_db_session)
        
        # 直接使用
        redis_service.set("key", "value")
        value = redis_service.get("key")
    """
    manager = get_manager()
    return manager.initialize(
        get_redis_service_func,
        get_db_session_func,
        auto_start_schedule
    )
