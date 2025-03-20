import time
import datetime
import traceback
import sqlalchemy
import clickhouse_connect.driver.client
import pandas as pd
import polars as pl
from dataclasses import dataclass
from sqlalchemy import text
from abc import ABC, abstractmethod

from tasks.scheduler import SCHEDULER
from utils.connect import CONNECTER, CK_TYPE_MAP
from utils.logger import make_logger


class task(ABC):
    '''所有任务的抽象'''

    def __init__(self, name: str, logger_name: str) -> None:
        self.name = name
        self.is_run = False
        self.log = make_logger(name, logger_name)
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
        SCHEDULER.append(self.next)
        self.is_run = True


def get_metadata(select_sql: str, connect: sqlalchemy.engine.Connection) -> list[list[str]]:
    '''获取来源视图的元数据'''
    temp_sql = text(f"SELECT * FROM ({select_sql}) AS T LIMIT 1")
    res = pd.read_sql(temp_sql, connect)
    _col = res.columns.to_list()
    _type = res.iloc[0].to_list()
    return [["`" + str(_col[i]) + "`", CK_TYPE_MAP[type(_type[i])]] for i in range(len(_col))]


def create_target(taget_table: str, metadata: list[list[str]], connect: clickhouse_connect.driver.client.Client) -> None:
    '''根据元数据创建目标表'''
    connect.command("CREATE DATABASE IF NOT EXISTS ods")
    connect.command(f"USE ods")
    connect.command(f"""
        CREATE TABLE IF NOT EXISTS ods.{taget_table}
        (
            {",".join([" ".join(ch) for ch in metadata])}
        )
        ENGINE = MergeTree()
        PRIMARY KEY (`id`)
    """)


def get_fotmat(value) -> str:
    '''获取变量的格式化形式'''
    if isinstance(value, str):
        return f"""\"{str(value)}\""""
    elif isinstance(value, type(None)):
        return ""
    elif isinstance(value, type(pd.NaT)):
        return "('1970-01-01 00:00:00', 1)"
    elif isinstance(value, datetime.datetime):
        return f"""('{value.strftime("%Y-%m-%d %H:%M:%S")}', 1)"""
    else:
        return str(value)


@dataclass
class sync_data:
    name: str
    logger_name: str
    source: str
    target: str
    source_sql: str
    taget_table: str


class sync(task):
    def __init__(self, data: sync_data) -> None:
        super().__init__(data.name, data.logger_name)
        self.data = data
        self.source = CONNECTER[data.source]
        self.target = CONNECTER[data.target]
        self.metadata = get_metadata(data.source_sql, self.source)
        create_target(data.taget_table, self.metadata, CONNECTER.get_ck())
        self.log.info(f"任务{self.data.name}初始化完成")

    def task_main(self) -> None:
        # clickhouse建议每次插入最少1万行，最多100万行，所以取个中间20万行
        dflist = pd.read_sql(self.data.source_sql, self.source, chunksize=200000)
        for df in dflist:
            df.to_sql(self.data.taget_table, self.target, schema="ods", if_exists="append")
        self.log.debug(f"已成功从{self.data.source}全量抽取并写入到{self.data.taget_table}")

    def __del__(self) -> None:
        self.source.close()
        self.target.close()

# @dataclass
# class increase_data:
#     name: str
#     logger_name: str
#     source: str
#     target: str
#     source_sync_sql: str
#     source_increase_sql: str
#     # 级联更新，人工定义表的外键
#     source_entry_sync_sql: dict[str, dict]
#     taget_table: str
#     taget_increase_sql: str

# class increase(task):
#     def __init__(self, data: increase_data) -> None:
#         super().__init__(data.name, data.logger_name)
#         self.data = data
#         self.source = CONNECTER[data.source]
#         self.target = CONNECTER[data.target]
#         self.log.info(f"任务{self.data.name}初始化完成")

#     def task_main(self) -> None:
#         source_increase_df = pl.read_database(self.data.source_increase_sql, self.source, batch_size=10000)
#         taget_increase_df = pl.read_database(self.data.taget_increase_sql, self.target, batch_size=10000)

#         new_diff = source_increase_df.join(taget_increase_df, left_on="id")
#         del_diff = taget_increase_df.join(source_increase_df, left_on="id")
#         if source_increase_df.width != 1:
#             change_diff = source_increase_df.join(taget_increase_df, on="id", how="inner").filter(
#                 pl.any_horizontal(pl.col(source_increase_df.columns[1:]) != pl.col(taget_increase_df.columns[1:]))
#             ).select("id")
#         else:
#             change_diff = pl.DataFrame()

#         # 如果超过要求大小，退化为全量同步
#         change_len = new_diff.height + change_diff.height
#         if change_len > CONFIG.MAX_INCREASE_CHANGE:
#             self.log.debug(f"变更行数{str(change_len)}行，超过规定{str(CONFIG.MAX_INCREASE_CHANGE)},退化为全量同步")
#             df = pl.read_database(self.data.source_sync_sql, self.source, batch_size=10000)
#             index = df.write_database(self.data.taget_table, self.target, if_table_exists="replace")
#             self.log.debug(f"已成功从{self.data.source}全量抽取并写入到{self.data.target}的{self.data.taget_table},抽取了{str(index)}行")
#             return

#         if del_diff.height + change_diff.height > 0:
#             ids_to_delete = del_diff["id"].to_list() + change_diff["id"].to_list()
#             delete_statement = text(f"DELETE FROM your_table WHERE id IN ({','.join(map(str, ids_to_delete))})")
#             self.target.execute(delete_statement)
#             self.target.commit()

#         if new_diff.height + change_diff.height > 0:
#             ids_to_select = new_diff["id"].to_list() + change_diff["id"].to_list()
#             in_clause = ", ".join(map(str, ids_to_select))
#             select_statement = text(f"SELECT * FROM {self.data.source_sync_sql} AS subquery WHERE subquery.id in ({in_clause})")
#             increase_df = pl.read_database(select_statement, self.source)
#             increase_df.write_database(self.data.taget_table, self.target, if_table_exists="append") # 这里一定能添加成功，因为重复的行数已经删除了

#         self.log.debug("已成功完成该表的增量同步")
