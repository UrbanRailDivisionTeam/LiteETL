import time
import traceback
import sqlalchemy
import pandas as pd
from dataclasses import dataclass
from sqlalchemy import text
from abc import ABC, abstractmethod

from tasks.scheduler import SCHEDULER
from utils.config import CONFIG
from utils.connect import CONNECT, DUCKDB
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

    @abstractmethod
    def task_main(self) -> None:
        # 继承后实现逻辑的地方
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
        self.log.info("依赖任务已添加到线程池，任务已完成")


@dataclass
class load_data:
    name: str
    logger_name: str
    source_sql: str
    target_table: str
    target_db: str
    target: str


class load(task):
    def __init__(self, data: load_data) -> None:
        super().__init__(data.name, data.logger_name)
        self.data = data
        self.source = DUCKDB.cursor()
        self.source.execute("CREATE SCHEMA IF NOT EXISTS dm")
        self.source.execute("USE dm")
        self.target = CONNECT[data.target]
        self.log.info(f"任务{self.data.name}初始化完成")

    def task_main(self) -> None:
        temp_df: pd.DataFrame = self.source.sql(self.data.source_sql).fetchdf()
        temp_df.to_sql(name=self.data.target_table, con=self.target, schema=self.data.target_db, if_exists="replace")
        self.log.debug(f"全量抽取已完成")

    def __del__(self) -> None:
        self.source.close()
        self.target.close()


@dataclass
class extract_data:
    name: str
    logger_name: str
    source_sql: str
    taget_table: str
    source: str


class extract(task):
    def __init__(self, data: extract_data) -> None:
        super().__init__(data.name, data.logger_name)
        self.data = data
        self.source = CONNECT[data.source]
        self.target = DUCKDB.cursor()
        self.target.execute("CREATE SCHEMA IF NOT EXISTS ods")
        self.target.execute("USE ods")
        self.log.info(f"任务{self.data.name}初始化完成")

    def task_main(self) -> None:
        temp_df = pd.read_sql(self.data.source_sql, self.source)
        self.target.execute(f"CREATE OR REPLACE TABLE {self.data.taget_table} AS SELECT * FROM temp_df")
        self.log.debug(f"全量抽取已完成")

    def __del__(self) -> None:
        self.source.close()
        self.target.close()


@dataclass
class sync_data:
    name: str
    logger_name: str
    source_sql: str
    taget_table: str
    source: str
    target: str


class sync(task):
    def __init__(self, data: sync_data) -> None:
        super().__init__(data.name, data.logger_name)
        self.data = data
        self.source = CONNECT[data.source]
        self.target = CONNECT[data.target]
        self.log.info(f"任务{self.data.name}初始化完成")

    def task_main(self) -> None:
        temp_df = pd.read_sql(self.data.source_sql, self.source)
        index = temp_df.to_sql(self.data.taget_table, self.target, if_exists="replace")
        self.log.debug(f"全量同步已完成，变更{str(index)}行")

    def __del__(self) -> None:
        self.source.close()
        self.target.close()


def get_diff(source_increase_df: pd.DataFrame, taget_increase_df: pd.DataFrame) -> tuple[list, list, list]:
    '''获取两个dataframe的不同'''
    new_diff = source_increase_df[~source_increase_df['id'].isin(taget_increase_df['id'])]['id'].tolist()
    del_diff = taget_increase_df[~taget_increase_df['id'].isin(source_increase_df['id'])]['id'].tolist()
    common_ids = pd.merge(source_increase_df, taget_increase_df, on='id', how='inner', suffixes=('_a', '_b'))
    columns_to_check = [col for col in source_increase_df.columns if col != 'id']
    common_ids['is_diff'] = common_ids.apply(lambda row: any(row[f'{col}_a'] != row[f'{col}_b'] for col in columns_to_check), axis=1)
    change_diff = common_ids[common_ids['is_diff']]['id'].tolist()
    return new_diff, del_diff, change_diff

def get_database_metadata(conn: sqlalchemy.engine.Connection, db_type:str, schema: str, table: str) -> pd.DataFrame:
    '''获取目标数据库目标表的元数据，用于监控数据库的结构变更'''
    if db_type in "mysql":
        return pd.read_sql(text(
            f"""
            SELECT 
                COLUMN_NAME AS '字段名',
                COLUMN_TYPE AS '数据类型',
                IS_NULLABLE AS '是否允许为空',
                COLUMN_DEFAULT AS '默认值'
            FROM 
                INFORMATION_SCHEMA.COLUMNS
            WHERE 
                TABLE_SCHEMA = '{schema}'
                AND TABLE_NAME = '{table}'
            """
        ), conn)
    elif db_type == "pgsql":
        return pd.read_sql(text(
            f"""
                SELECT 
                    column_name AS 字段名,
                    data_type AS 数据类型,
                    is_nullable AS 是否允许为空,
                    column_default AS 默认值
                FROM 
                    information_schema.columns
                WHERE 
                    table_schema = '{schema}'
                    AND table_name = '{table}';
            """
        ), conn)
    elif db_type == "sqlserver":
        return pd.read_sql(text(
            f"""
            SELECT 
                COLUMN_NAME AS 字段名,
                DATA_TYPE AS 数据类型,
                IS_NULLABLE AS 是否允许为空,
                COLUMN_DEFAULT AS 默认值
            FROM 
                INFORMATION_SCHEMA.COLUMNS
            WHERE 
                TABLE_SCHEMA = '{schema}'
                AND TABLE_NAME = '{table}';
            """
        ), conn)
    elif db_type == "oracle":
        return pd.read_sql(text(
            f"""
            SELECT 
                COLUMN_NAME AS 字段名,
                DATA_TYPE AS 数据类型,
                NULLABLE AS 是否允许为空,
                DATA_DEFAULT AS 默认值
            FROM 
                ALL_TAB_COLUMNS
            WHERE 
                OWNER = '{schema}'
                AND TABLE_NAME = '{table}'
            """
        ), conn)
    else:
        raise ValueError("不支持的数据库类型")
        

@dataclass
class load_increase_data:
    name: str
    logger_name: str
    source_sync_sql: str
    source_increase_sql: str
    taget_table: str
    taget_increase_sql: str
    target: str


class load_increase(task):
    def __init__(self, data: load_increase_data) -> None:
        super().__init__(data.name, data.logger_name)
        self.data = data
        self.source = DUCKDB.cursor()
        self.source.execute("CREATE SCHEMA IF NOT EXISTS dm")
        self.source.execute("USE dm")
        self.target = CONNECT[data.target]
        self.log.info(f"任务{self.data.name}初始化完成")

    def trans_sync(self) -> None:
        df: pd.DataFrame = self.source.sql(self.data.source_sync_sql).fetchdf()
        index = df.to_sql(self.data.taget_table, self.target, if_exists="replace")
        self.log.debug(f"全量抽取已完成,抽取了{str(index)}行")

    def trans_increase(self, new_diff: list, del_diff: list, change_diff: list) -> None:
        if len(del_diff) + len(change_diff) > 0:
            ids_to_delete = del_diff + change_diff
            delete_statement = text(f"DELETE FROM {self.data.taget_table} WHERE id IN ({','.join([f"\'{ch}\'" for ch in ids_to_delete])})")
            self.target.execute(delete_statement)
            self.target.commit()

        if len(new_diff) + len(change_diff) > 0:
            ids_to_select = new_diff + change_diff
            in_clause = ', '.join([f"\'{ch}\'" for ch in ids_to_select])
            increase_df: pd.DataFrame = self.source.sql(f"SELECT * FROM ({self.data.source_sync_sql}) AS subquery WHERE subquery.id in ({in_clause})").fetchdf()
            increase_df.to_sql(self.data.taget_table, self.target, if_exists="append")

        self.log.debug("已成功完成该主表的增量同步")

    def task_main(self) -> None:
        try:
            taget_increase_df = pd.read_sql(self.data.taget_increase_sql, self.target)
        except Exception as e:
            self.log.warning("获取本地同步缓存失败，增量转换为全量同步")
            self.trans_sync()
        else:
            source_increase_df: pd.DataFrame = self.source.sql(self.data.source_increase_sql).fetchdf()
            new_diff, del_diff, change_diff = get_diff(source_increase_df, taget_increase_df)
            # 如果超过要求大小，退化为全量同步
            change_len = len(new_diff) + len(change_diff)
            if change_len > CONFIG.MAX_INCREASE_CHANGE:
                self.log.debug(f"变更行数{str(change_len)}行，超过规定{str(CONFIG.MAX_INCREASE_CHANGE)},退化为全量同步")
                self.trans_sync()
            else:
                self.trans_increase(new_diff, del_diff, change_diff)


@dataclass
class extract_increase_data:
    name: str
    logger_name: str
    source_sync_sql: str
    source_increase_sql: str
    taget_table: str
    taget_increase_sql: str
    source: str


class extract_increase(task):
    def __init__(self, data: extract_increase_data) -> None:
        super().__init__(data.name, data.logger_name)
        self.data = data
        self.source = CONNECT[data.source]
        self.target = DUCKDB.cursor()
        self.target.execute("CREATE SCHEMA IF NOT EXISTS ods")
        self.target.execute("USE ods")
        self.log.info(f"任务{self.data.name}初始化完成")

    def trans_sync(self) -> None:
        temp_df = pd.read_sql(self.data.source_sync_sql, self.source)
        self.target.execute(f"CREATE OR REPLACE TABLE {self.data.taget_table} AS SELECT * FROM temp_df")
        self.log.debug(f"全量抽取已完成")

    def trans_increase(self, new_diff: list, del_diff: list, change_diff: list) -> None:
        if len(del_diff) + len(change_diff) > 0:
            ids_to_delete = del_diff + change_diff
            self.target.execute(f"DELETE FROM ods.{self.data.taget_table} WHERE id IN ({','.join([f"\'{ch}\'" for ch in ids_to_delete])})")

        if len(new_diff) + len(change_diff) > 0:
            ids_to_select = new_diff + change_diff
            in_clause = ', '.join([f"\'{ch}\'" for ch in ids_to_select])
            select_statement = text(f"SELECT * FROM ({self.data.source_sync_sql}) AS subquery WHERE subquery.id in ({in_clause})")
            increase_df = pd.read_sql(select_statement, self.source)
            self.target.execute(f"INSERT INTO {self.data.taget_table} SELECT * FROM increase_df")

        self.log.debug("已成功完成该主表的增量同步")

    def task_main(self) -> None:
        try:
            taget_increase_df: pd.DataFrame = self.target.sql(self.data.taget_increase_sql).fetchdf()
        except Exception as e:
            self.log.warning("获取本地同步缓存失败，增量转换为全量同步")
            self.trans_sync()
            return
        else:
            source_increase_df = pd.read_sql(self.data.source_increase_sql, self.source)
            new_diff, del_diff, change_diff = get_diff(source_increase_df, taget_increase_df)
            # 如果超过要求大小，退化为全量同步
            change_len = len(new_diff) + len(change_diff)
            if change_len > CONFIG.MAX_INCREASE_CHANGE:
                self.log.debug(f"变更行数{str(change_len)}行，超过规定{str(CONFIG.MAX_INCREASE_CHANGE)},退化为全量同步")
                self.trans_sync()
            else:
                self.trans_increase(new_diff, del_diff, change_diff)


@dataclass
class sync_increase_data:
    name: str
    logger_name: str
    source_sync_sql: str
    source_increase_sql: str
    taget_table: str
    taget_increase_sql: str
    source: str
    target: str


class sync_increase(task):
    def __init__(self, data: sync_increase_data) -> None:
        super().__init__(data.name, data.logger_name)
        self.data = data
        self.source = CONNECT[data.source]
        self.target = CONNECT[data.target]
        self.log.info(f"任务{self.data.name}初始化完成")

    def trans_sync(self) -> None:
        df = pd.read_sql(self.data.source_sync_sql, self.source)
        index = df.to_sql(self.data.taget_table, self.target, if_exists="replace")
        self.log.debug(f"全量抽取已完成,抽取了{str(index)}行")

    def trans_increase(self, new_diff: list, del_diff: list, change_diff: list) -> None:
        if len(del_diff) + len(change_diff) > 0:
            ids_to_delete = del_diff + change_diff
            delete_statement = text(f"DELETE FROM {self.data.taget_table} WHERE id IN ({','.join([f"\'{ch}\'" for ch in ids_to_delete])})")
            self.target.execute(delete_statement)
            self.target.commit()

        if len(new_diff) + len(change_diff) > 0:
            ids_to_select = new_diff + change_diff
            in_clause = ', '.join([f"\'{ch}\'" for ch in ids_to_select])
            select_statement = text(f"SELECT * FROM ({self.data.source_sync_sql}) AS subquery WHERE subquery.id in ({in_clause})")
            pd.read_sql(select_statement, self.source).to_sql(self.data.taget_table, self.target, if_exists="append")  # 这里一定能添加成功，因为重复的行数已经删除了

        self.log.debug("已成功完成该主表的增量同步")

    def task_main(self) -> None:
        try:
            taget_increase_df = pd.read_sql(self.data.taget_increase_sql, self.target)
        except Exception as e:
            self.log.warning("获取本地同步缓存失败，增量转换为全量同步")
            self.trans_sync()
        else:
            source_increase_df = pd.read_sql(self.data.source_increase_sql, self.source)
            new_diff, del_diff, change_diff = get_diff(source_increase_df, taget_increase_df)
            # 如果超过要求大小，退化为全量同步
            change_len = len(new_diff) + len(change_diff)
            if change_len > CONFIG.MAX_INCREASE_CHANGE:
                self.log.debug(f"变更行数{str(change_len)}行，超过规定{str(CONFIG.MAX_INCREASE_CHANGE)},退化为全量同步")
                self.trans_sync()
            else:
                self.trans_increase(new_diff, del_diff, change_diff)
