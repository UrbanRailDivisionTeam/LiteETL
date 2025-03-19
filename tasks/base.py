import time
import sqlalchemy
import traceback
import logging
from abc import ABC, abstractmethod
from tasks.scheduler import SCHEDULER
from utils.logger import make_logger

class task_connect_with:
    '''用来处理连接异常的上下文管理器'''
    def __init__(self, engine: sqlalchemy.engine.Engine, log: logging.Logger) -> None:
        self.engine = engine
        self.connection = engine.connect()
        self.log = log

    def __enter__(self) -> sqlalchemy.engine.Connection:
        return self.connection

    def __exit__(self, exc_type, exc_value, traceback_info) -> bool:
        # 退出上下文时，处理异常并回滚事务
        if exc_type is not None:  # 如果有异常发生
            self.log.critical("报错类型：" + str(exc_type))
            self.log.critical("报错内容：" + str(exc_value))
            self.log.critical("报错堆栈信息：" + str(traceback.format_exc()))
            # 回退操作
            self.connection.rollback()
        else:
            # 如果没有异常，提交事务
            self.connection.commit()
        self.connection.close()
        return True

class task(ABC):
    '''所有任务的抽象'''
    def __init__(self, name: str) -> None:
        self.name = name
        self.is_run = False
        self.log = make_logger(self.name)
        self.next: list[task] = []
        
    def then(self, input_task: 'task') -> 'task':
        self.next.append(input_task)
        return self
    
    # 继承后实现逻辑的地方
    @abstractmethod
    def task_main(self) -> None:
        ...
    
    def run(self) -> None:
        # 真正运行函数的地方
        start_time = time.time()
        try:
            self.task_main()
        except Exception as e:
            self.log.critical("报错内容：" + str(e))
            self.log.critical("报错堆栈信息：" + str(traceback.format_exc()))
        end_time = time.time()
        self.log.info("函数花费时间为:{} 秒".format(end_time - start_time))
        # 如果存在对应的依赖任务，则添加到调度器中
        for next_task in self.next:
            SCHEDULER.submit(next_task.run)
        self.is_run = True
        

    
