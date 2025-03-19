import time
import traceback
import logging
import polars as pl
from dataclasses import dataclass
from sqlalchemy import engine, text, TextClause
from abc import ABC, abstractmethod

from tasks.scheduler import SCHEDULER
from utils.config import CONFIG
from utils.connect import CONNECTER
from utils.logger import make_logger


class task_connect_with:
    '''用来处理连接异常的上下文管理器'''
    def __init__(self, engine: engine.Engine, log: logging.Logger) -> None:
        self.engine = engine
        self.connection = engine.connect()
        self.log = log

    def __enter__(self) -> engine.Connection:
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
        
        
@dataclass
class sync_data:
    name: str
    source: str
    target: str
    source_sql: TextClause
    taget_table: str

class sync(task):
    def __init__(self, data: sync_data) -> None:
        super().__init__(data.name)
        self.data = data
        self.source = CONNECTER.get(data.source)
        self.target = CONNECTER.get(data.target)
        self.log.info(f"任务{self.data.name}初始化完成")

    def __del__(self) -> None:
        self.source.close()
        self.target.close()

    def task_main(self) -> None:
        df = pl.read_database(self.data.source_sql, self.source, batch_size=10000)
        index = df.write_database(self.data.taget_table, self.target, if_table_exists="replace")
        self.log.debug(f"已成功从{self.data.source}全量抽取并写入到{self.data.target}的{self.data.taget_table},抽取了{str(index)}行")

@dataclass
class increase_data:
    name: str
    source: str
    target: str
    source_sync_sql: TextClause
    source_increase_sql: TextClause
    source_entry_sync_sql: list
    taget_table: str
    taget_increase_sql: TextClause

class increase(task):
    def __init__(self, data: increase_data) -> None:
        super().__init__(data.name)
        self.data = data
        self.source = CONNECTER.get(data.source)
        self.target = CONNECTER.get(data.target)
        self.log.info(f"任务{self.data.name}初始化完成")

    def task_main(self) -> None:
        source_increase_df = pl.read_database(self.data.source_increase_sql, self.source, batch_size=10000)
        taget_increase_df = pl.read_database(self.data.taget_increase_sql, self.target, batch_size=10000)

        new_diff = source_increase_df.join(taget_increase_df, left_on="id")
        del_diff = taget_increase_df.join(source_increase_df, left_on="id")
        if source_increase_df.width != 1:
            change_diff = source_increase_df.join(taget_increase_df, on="id", how="inner").filter(
                pl.any_horizontal(pl.col(source_increase_df.columns[1:]) != pl.col(taget_increase_df.columns[1:]))
            ).select("id")
        else:
            change_diff = pl.DataFrame()
        
        # 如果超过要求大小，退化为全量同步
        change_len = new_diff.height + change_diff.height
        if change_len > CONFIG.MAX_INCREASE_CHANGE:
            self.log.debug(f"变更行数{str(change_len)}行，超过规定{str(CONFIG.MAX_INCREASE_CHANGE)},退化为全量同步")
            df = pl.read_database(self.data.source_sync_sql, self.source, batch_size=10000)
            index = df.write_database(self.data.taget_table, self.target, if_table_exists="replace")
            self.log.debug(f"已成功从{self.data.source}全量抽取并写入到{self.data.target}的{self.data.taget_table},抽取了{str(index)}行")
            return
        
        if del_diff.height + change_diff.height > 0:
            ids_to_delete = del_diff["id"].to_list() + change_diff["id"].to_list()
            delete_statement = text(f"DELETE FROM your_table WHERE id IN ({','.join(map(str, ids_to_delete))})")
            self.target.execute(delete_statement)
            self.target.commit()
            
        if new_diff.height + change_diff.height > 0:
            ids_to_select = new_diff["id"].to_list() + change_diff["id"].to_list()
            in_clause = ", ".join(map(str, ids_to_select))
            select_statement = text(f"SELECT * FROM {self.data.source_sync_sql} AS subquery WHERE subquery.id in ({in_clause})")
            increase_df = pl.read_database(select_statement, self.source)
            increase_df.write_database(self.data.taget_table, self.target, if_table_exists="append") # 这里一定能添加成功，因为重复的行数已经删除了
            
        self.log.debug("已成功完成该表的增量同步")
        

    
