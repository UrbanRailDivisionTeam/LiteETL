import os
import time
import duckdb
from concurrent.futures import ThreadPoolExecutor
from tasks.base import task, extract_increase, extract
from utils.logger import make_logger


class executer:
    def __init__(self, _duckdb: duckdb.DuckDBPyConnection) -> None:
        self.task_list: list['task'] = []
        self.log = make_logger(_duckdb, "ETL负载执行器", "etl_executer")
        num = self.get_cpu_count()
        self.log.info(f"线程池线程数为{str(num)}")
        self.tpool = ThreadPoolExecutor(max_workers=num)
        self.log.info("调度器初始化已完成")

    def stop(self) -> None:
        self.tpool.shutdown(wait=False, cancel_futures=True)

    def can_start(self, input_task: task) -> bool:
        # 没有依赖可以直接运行
        if len(input_task.depend) == 0:
            return True
        # 已经开始运行不可再次运行
        if input_task.start_run:
            return False
        # 检查依赖是否全运行完成，全运行完成可以运行
        for _tasks in self.task_list:
            if _tasks.name in input_task.depend and not _tasks.end_run:
                return False
        return True

    def can_stop(self) -> bool:
        # 如果所有任务均运行完成即可退出
        for _tasks in self.task_list:
            if not _tasks.end_run:
                return False
        return True

    def run(self, input_tasks: list[task]) -> None:
        self.log.info("调度器开始执行")
        self.task_list = input_tasks
        while True:
            for _task in self.task_list:
                if self.can_start(_task):
                    self.tpool.submit(_task.run)
            if self.can_stop():
                break
            # 一般抽取任务最少也要0.1秒完成，0.5秒检查一次
            time.sleep(0.1)
        self.log.info("调度器已执行完成")

    @staticmethod
    def get_cpu_count():
        num = os.cpu_count()
        num = num * 2 if not num is None else 5
        return num
