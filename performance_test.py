#!/usr/bin/env python3
"""
Redis降级MySQL性能测试脚本
"""

import time
import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed
from redis_mysql_fallback import RedisGateway
import logging

logging.basicConfig(level=logging.WARNING)

# 配置
redis_config = {
    'host': 'localhost',
    'port': 6379,
    'db': 0,
    'password': None,
    'decode_responses': False,
}

mysql_config = {
    'connection_string': 'mysql+pymysql://root:password@localhost:3306/redis_gateway_info?charset=utf8mb4'
}


class PerformanceTester:
    """性能测试类"""
    
    def __init__(self):
        self.gateway = RedisGateway(redis_config, mysql_config)
        self.results = []
    
    def measure_time(self, func, *args, **kwargs):
        """测量函数执行时间"""
        start = time.time()
        result = func(*args, **kwargs)
        elapsed = (time.time() - start) * 1000  # 转换为毫秒
        return elapsed, result
    
    def test_set_get(self, num_ops=1000):
        """测试set和get性能"""
        print(f"\n=== Set/Get性能测试 ({num_ops}次操作) ===")
        
        # 测试set
        set_times = []
        for i in range(num_ops):
            elapsed, _ = self.measure_time(self.gateway.set, f'test_key_{i}', f'value_{i}')
            set_times.append(elapsed)
        
        print(f"Set操作:")
        print(f"  平均耗时: {statistics.mean(set_times):.2f}ms")
        print(f"  中位数: {statistics.median(set_times):.2f}ms")
        print(f"  最大耗时: {max(set_times):.2f}ms")
        print(f"  最小耗时: {min(set_times):.2f}ms")
        
        # 等待异步写入完成
        time.sleep(2)
        
        # 测试get (从Redis)
        get_times = []
        for i in range(num_ops):
            elapsed, _ = self.measure_time(self.gateway.get, f'test_key_{i}')
            get_times.append(elapsed)
        
        print(f"\nGet操作 (从Redis):")
        print(f"  平均耗时: {statistics.mean(get_times):.2f}ms")
        print(f"  中位数: {statistics.median(get_times):.2f}ms")
        
        # 测试get (从MySQL - 模拟Redis故障)
        self.gateway.health_checker.redis_alive = False
        mysql_get_times = []
        for i in range(min(100, num_ops)):  # MySQL较慢，只测试100次
            elapsed, _ = self.measure_time(self.gateway.get, f'test_key_{i}')
            mysql_get_times.append(elapsed)
        
        print(f"\nGet操作 (从MySQL):")
        print(f"  平均耗时: {statistics.mean(mysql_get_times):.2f}ms")
        print(f"  中位数: {statistics.median(mysql_get_times):.2f}ms")
        
        # 恢复Redis状态
        self.gateway.health_checker.redis_alive = True
        
        # 清理
        for i in range(num_ops):
            self.gateway.delete(f'test_key_{i}')
    
    def test_hash(self, num_ops=1000):
        """测试Hash性能"""
        print(f"\n=== Hash性能测试 ({num_ops}次操作) ===")
        
        # 测试hset
        hset_times = []
        for i in range(num_ops):
            elapsed, _ = self.measure_time(
                self.gateway.hset, 
                f'hash_{i // 10}',  # 每10个操作用同一个hash
                f'field_{i}', 
                f'value_{i}'
            )
            hset_times.append(elapsed)
        
        print(f"HSet操作:")
        print(f"  平均耗时: {statistics.mean(hset_times):.2f}ms")
        print(f"  中位数: {statistics.median(hset_times):.2f}ms")
        
        # 等待异步写入
        time.sleep(2)
        
        # 测试hget
        hget_times = []
        for i in range(num_ops):
            elapsed, _ = self.measure_time(
                self.gateway.hget,
                f'hash_{i // 10}',
                f'field_{i}'
            )
            hget_times.append(elapsed)
        
        print(f"\nHGet操作:")
        print(f"  平均耗时: {statistics.mean(hget_times):.2f}ms")
        print(f"  中位数: {statistics.median(hget_times):.2f}ms")
        
        # 清理
        for i in range(num_ops // 10):
            self.gateway.delete(f'hash_{i}')
    
    def test_list(self, num_ops=1000):
        """测试List性能"""
        print(f"\n=== List性能测试 ({num_ops}次操作) ===")
        
        # 测试rpush
        rpush_times = []
        for i in range(num_ops):
            elapsed, _ = self.measure_time(self.gateway.rpush, 'test_list', f'item_{i}')
            rpush_times.append(elapsed)
        
        print(f"RPush操作:")
        print(f"  平均耗时: {statistics.mean(rpush_times):.2f}ms")
        print(f"  中位数: {statistics.median(rpush_times):.2f}ms")
        
        # 等待异步写入
        time.sleep(2)
        
        # 测试lrange
        elapsed, _ = self.measure_time(self.gateway.lrange, 'test_list', 0, -1)
        print(f"\nLRange操作 (获取{num_ops}个元素):")
        print(f"  耗时: {elapsed:.2f}ms")
        
        # 测试lpop
        lpop_times = []
        for i in range(min(100, num_ops)):
            elapsed, _ = self.measure_time(self.gateway.lpop, 'test_list')
            lpop_times.append(elapsed)
        
        print(f"\nLPop操作:")
        print(f"  平均耗时: {statistics.mean(lpop_times):.2f}ms")
        
        # 清理
        self.gateway.delete('test_list')
    
    def test_concurrent(self, num_threads=10, ops_per_thread=100):
        """测试并发性能"""
        print(f"\n=== 并发性能测试 ({num_threads}线程, 每线程{ops_per_thread}次操作) ===")
        
        def worker(thread_id):
            times = []
            for i in range(ops_per_thread):
                key = f'concurrent_key_{thread_id}_{i}'
                elapsed, _ = self.measure_time(self.gateway.set, key, f'value_{i}')
                times.append(elapsed)
            return times
        
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(worker, i) for i in range(num_threads)]
            all_times = []
            for future in as_completed(futures):
                all_times.extend(future.result())
        
        total_time = time.time() - start_time
        total_ops = num_threads * ops_per_thread
        
        print(f"总操作数: {total_ops}")
        print(f"总耗时: {total_time:.2f}秒")
        print(f"吞吐量: {total_ops / total_time:.2f} ops/秒")
        print(f"平均延迟: {statistics.mean(all_times):.2f}ms")
        print(f"P95延迟: {statistics.quantiles(all_times, n=20)[18]:.2f}ms")
        print(f"P99延迟: {statistics.quantiles(all_times, n=100)[98]:.2f}ms")
        
        # 清理
        for i in range(num_threads):
            for j in range(ops_per_thread):
                self.gateway.delete(f'concurrent_key_{i}_{j}')
    
    def test_distributed_lock(self, num_attempts=100):
        """测试分布式锁性能"""
        print(f"\n=== 分布式锁性能测试 ({num_attempts}次获取) ===")
        
        lock_times = []
        success_count = 0
        
        for i in range(num_attempts):
            start = time.time()
            with self.gateway.distributed_lock('test_lock', timeout=1) as acquired:
                elapsed = (time.time() - start) * 1000
                lock_times.append(elapsed)
                if acquired:
                    success_count += 1
        
        print(f"成功获取: {success_count}/{num_attempts}")
        print(f"平均耗时: {statistics.mean(lock_times):.2f}ms")
        print(f"中位数: {statistics.median(lock_times):.2f}ms")
    
    def test_failover(self):
        """测试故障切换性能"""
        print(f"\n=== 故障切换测试 ===")
        
        # 正常情况
        key = 'failover_test'
        self.gateway.set(key, 'test_value')
        time.sleep(0.5)
        
        # Redis读取
        elapsed, _ = self.measure_time(self.gateway.get, key)
        print(f"Redis读取耗时: {elapsed:.2f}ms")
        
        # 模拟Redis故障
        self.gateway.health_checker.redis_alive = False
        
        # MySQL读取
        elapsed, _ = self.measure_time(self.gateway.get, key)
        print(f"MySQL读取耗时 (降级): {elapsed:.2f}ms")
        print(f"性能下降: {elapsed / 0.5:.1f}倍" if elapsed > 0.5 else "性能正常")
        
        # 恢复
        self.gateway.health_checker.redis_alive = True
        self.gateway.delete(key)
    
    def run_all_tests(self):
        """运行所有测试"""
        print("="*60)
        print("Redis降级MySQL性能测试")
        print("="*60)
        
        # 基础性能测试
        self.test_set_get(num_ops=1000)
        self.test_hash(num_ops=1000)
        self.test_list(num_ops=500)
        
        # 并发测试
        self.test_concurrent(num_threads=10, ops_per_thread=100)
        
        # 分布式锁测试
        self.test_distributed_lock(num_attempts=50)
        
        # 故障切换测试
        self.test_failover()
        
        print("\n" + "="*60)
        print("测试完成")
        print("="*60)
        
        # 清理
        self.gateway.close()


def main():
    """主函数"""
    tester = PerformanceTester()
    
    try:
        tester.run_all_tests()
    except KeyboardInterrupt:
        print("\n\n测试中断")
    except Exception as e:
        print(f"\n测试失败: {e}")
        import traceback
        traceback.print_exc()
    finally:
        tester.gateway.close()


if __name__ == '__main__':
    main()
