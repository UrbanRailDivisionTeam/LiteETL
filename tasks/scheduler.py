import os
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from tasks.base import task
from utils.logger import make_logger

class scheduler:
    def __init__(self) -> None:
        num = os.cpu_count()
        num = num * 2 if not num is None else 5
        self.tpool = ThreadPoolExecutor(max_workers=num)
        self.tasks: list[task] = []
        self.log = make_logger("ETL负载调度器")
        self.log.info("调度器初始化已完成")
    
    def submit(self, func: Callable[[], None]) -> Future:
        return self.tpool.submit(func)
    
    def run(self) -> None:
        '''运行任务'''
        for temp_task in self.tasks:
            self.submit(temp_task.run)
    
    def is_runed(self) -> bool:
        '''检查所有任务是否均以运行完成'''
        for temp_task in self.tasks:
            if not temp_task.is_run:
                return False
        return True
        
    def stop(self):
        self.tpool.shutdown(wait=False, cancel_futures=True)
    
    def task_init(self):
        '''
        用于生成任务和定义任务之间的依赖关系
        返回值是需要最开始运行的任务列表
        '''
        ...
        self.log.info("任务初始化已完成")
            
SCHEDULER = scheduler()