import os
from concurrent.futures import ThreadPoolExecutor
from utils.logger import make_logger

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from tasks.base import task

class scheduler:
    def __init__(self) -> None:
        self.tasks: list['task'] = []
        self.log = make_logger("ETL负载调度器", "etl_scheduler")
        num = self.get_cpu_count()
        self.log.info(f"线程池线程数为{str(num)}")
        self.tpool = ThreadPoolExecutor(max_workers=num)
        self.log.info("调度器初始化已完成")
    
    def is_runed(self) -> bool:
        '''检查所有任务是否均以运行完成'''
        for temp_task in self.tasks:
            if not temp_task.is_run:
                return False
        return True
        
    def stop(self) -> None:
        self.tpool.shutdown(wait=False, cancel_futures=True)
    
    def append(self, input_tasks: list['task']) -> 'scheduler':
        self.tasks += input_tasks
        for temp_task in input_tasks:
            self.tpool.submit(temp_task.run)
        return self
    
    @staticmethod
    def get_cpu_count():
        num = os.cpu_count()
        num = num * 2 if not num is None else 5
        return num
            
SCHEDULER = scheduler()