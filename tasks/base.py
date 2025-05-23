import time
import duckdb
import traceback
import sqlalchemy
import pandas as pd
from dataclasses import dataclass
from sqlalchemy import text
from abc import ABC, abstractmethod
from enum import Enum

from utils.config import CONFIG
from utils.connect import connect_data
from utils.logger import make_logger


class task_status(Enum):
    '''任务的当前状态，也就是任务的生命周期'''
    WAIT = 1
    RUNNING = 2
    END = 3
    DEAD = 4

class task(ABC):
    '''所有任务的抽象'''

    def __init__(self, _duckdb: duckdb.DuckDBPyConnection, name: str, logger_name: str) -> None:
        self.name = name
        self.status: task_status = task_status.WAIT
        self.log = make_logger(_duckdb, name, logger_name)
        self.depend: list[str] = []

    def dp(self, task_name: 'task') -> 'task':
        if task_name.name == self.name:
            raise ValueError("不能将自己设置为前置依赖")
        self.depend.append(task_name.name)
        return self

    @abstractmethod
    def task_run(self) -> None:
        # 继承后实现逻辑的地方
        ...
        
    @abstractmethod
    def task_delete(self) -> None:
        # 继承后实现逻辑的地方
        ...

    def run(self) -> None:
        # 真正运行函数的地方
        if self.status == task_status.WAIT:
            self.status = task_status.RUNNING
            start_time = time.time()
            try:
                self.task_run()
            except Exception as e:
                self.log.critical("报错内容：" + str(e))
                self.log.critical("报错堆栈信息：" + str(traceback.format_exc()))
            end_time = time.time()
            self.status = task_status.END
            self.log.info("函数花费时间为:{} 秒,".format(end_time - start_time) + f"{self.name} 任务已完成")
            
    def __del__(self) -> None:
        if self.status == task_status.END:
            self.status = task_status.DEAD
            self.task_delete()


@dataclass
class load_data:
    name: str
    logger_name: str
    source_sql: str
    target_table: str
    target_db: str
    target: str


class load(task):
    def __init__(self, connect: connect_data, data: load_data) -> None:
        super().__init__(connect.duckdb, data.name, data.logger_name)
        self.data = data
        self.source = connect.duckdb.cursor()
        self.target = connect.connect[data.target]
        self.log.info(f"任务{self.data.name}初始化完成")

    def task_run(self) -> None:
        temp_df: pd.DataFrame = self.source.sql(self.data.source_sql).fetchdf()
        temp_df.to_sql(name=self.data.target_table, con=self.target, schema=self.data.target_db, if_exists="replace")
        self.log.debug(f"全量抽取已完成")

    def task_delete(self) -> None:
        self.source.close()
        self.target.close()


@dataclass
class extract_data:
    name: str
    logger_name: str
    source_sql: str
    target_table: str
    source: str


class extract(task):
    def __init__(self, connect: connect_data, data: extract_data) -> None:
        super().__init__(connect.duckdb, data.name, data.logger_name)
        self.data = data
        self.source = connect.connect[data.source]
        self.target = connect.duckdb.cursor()
        self.log.info(f"任务{self.data.name}初始化完成")

    def task_run(self) -> None:
        temp_df = pd.read_sql(self.data.source_sql, self.source)
        self.target.execute(f"CREATE OR REPLACE TABLE ods.{self.data.target_table} AS SELECT * FROM temp_df")
        self.log.debug(f"全量抽取已完成")

    def task_delete(self) -> None:
        self.source.close()
        self.target.close()


@dataclass
class sync_data:
    name: str
    logger_name: str
    source: str
    source_sql: str
    taget_table: str
    target_db: str
    target: str


class sync(task):
    def __init__(self, connect: connect_data, data: sync_data) -> None:
        super().__init__(connect.duckdb, data.name, data.logger_name)
        self.data = data
        self.source = connect.connect[data.source]
        self.target = connect.connect[data.target]
        self.log.info(f"任务{self.data.name}初始化完成")

    def task_run(self) -> None:
        temp_df = pd.read_sql(self.data.source_sql, self.source)
        index = temp_df.to_sql(name=self.data.taget_table, con=self.target, schema=self.data.target_db, if_exists="replace")
        self.log.debug(f"全量同步已完成，变更{str(index)}行")

    def task_delete(self) -> None:
        self.source.close()
        self.target.close()


def chunk_list(lst: list[str], chunk_size: int = 1000) -> list[list[str]]:
    '''切分列表为指定的长度'''
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]


def get_diff(cursor: duckdb.DuckDBPyConnection, source_increase_df: pd.DataFrame, target_increase_df: pd.DataFrame) -> tuple[list[str], list[str], list[str]]:
    '''获取两个dataframe的不同'''
    if source_increase_df.columns.to_list() != target_increase_df.columns.to_list():
        raise ValueError("增量检查的数据列名不同")

    new_diff = cursor.sql(
        f"""
        SELECT "id" FROM source_increase_df
        EXCEPT
        SELECT "id" FROM target_increase_df
        """
    ).fetchall()
    new_diff = [str(item[0]) for item in new_diff]
    del_diff = cursor.sql(
        f"""
        SELECT "id" FROM target_increase_df
        EXCEPT
        SELECT "id" FROM source_increase_df
        """
    ).fetchall()
    del_diff = [str(item[0]) for item in del_diff]

    # 比较共同 ID 的记录是否有差异
    columns_to_check = [col for col in source_increase_df.columns if col != 'id']
    if len(columns_to_check) != 0:
        source_where = f"MD5(CONCAT({", ".join([f"CAST(s.\"{ch}\" AS VARCHAR)" for ch in columns_to_check])}))"
        target_where = f"MD5(CONCAT({", ".join([f"CAST(t.\"{ch}\" AS VARCHAR)" for ch in columns_to_check])}))"
        _where = f"{source_where} != {target_where}"
        change_diff = cursor.sql(
            f'''
                SELECT CAST(s."id" AS VARCHAR)
                FROM source_increase_df s
                JOIN target_increase_df t ON MD5(CAST(s."id" AS VARCHAR)) = MD5(CAST(t."id" AS VARCHAR))
                WHERE {_where}
            '''
        ).fetchall()
        change_diff = [str(item[0]) for item in change_diff]
    else:
        change_diff = []
    return new_diff, del_diff, change_diff


def get_database_type(conn: sqlalchemy.engine.Connection) -> str:
    '''根据sqlalchemy的连接获取数据库的类型'''
    dtype = conn.dialect.name
    if dtype == "postgresql":
        dtype = "pgsql"
    elif dtype == "mysql":
        pass
    elif dtype == "oracle":
        pass
    elif dtype == "mssql":
        dtype = "sqlserver"
    else:
        raise ValueError("不支持的数据库类型")
    return dtype


def get_database_metadata(conn: sqlalchemy.engine.Connection, schema: str, table: str) -> pd.DataFrame:
    '''获取目标数据库目标表的元数据，用于监控数据库的结构变更'''
    db_type = get_database_type(conn)
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
    target: str
    target_table: str
    target_db: str
    target_increase_sql: str
    is_del: bool = True


class load_increase(task):
    def __init__(self, connect: connect_data, data: load_increase_data) -> None:
        super().__init__(connect.duckdb, data.name, data.logger_name)
        self.data = data
        self.source = connect.duckdb.cursor()
        self.target = connect.connect[data.target]
        self.log.info(f"任务{self.data.name}初始化完成")

    def trans_sync(self) -> None:
        first_label = True
        index = 0
        while True:
            # 注意这里全量同步的分片是根据duckdb内部的向量来的，是2048的倍数
            # 这里因为是从duckdb中抽数据到其它系统，所以不需要考虑类型问题
            df: pd.DataFrame = self.source.sql(self.data.source_sync_sql).fetch_df_chunk()
            if df.empty:
                break
            if first_label:
                temp_index = df.to_sql(name=self.data.target_table, con=self.target, schema=self.data.target_db, if_exists="replace")
                first_label = False
            else:
                temp_index = df.to_sql(name=self.data.target_table, con=self.target, schema=self.data.target_db, if_exists="append")
            index += temp_index if temp_index is not None else 0
        self.log.debug(f"全量抽取已完成,抽取了{str(index)}行")
        meta_data = get_database_metadata(self.target, self.data.target_db, self.data.target_table)
        self.source.execute(f"CREATE OR REPLACE TABLE meta.{self.data.target_db}_{self.data.target_table} AS SELECT * FROM meta_data")
        self.log.debug(f"已重建元数据")

    def task_del(self, delete_list: list[str]) -> None:
        '''删除目标表中需要删除的和需要变更的'''
        del_len = len(delete_list)
        if del_len > 0:
            if del_len < CONFIG.MAX_WHERE_LIST:
                delete_statement = text(f"DELETE FROM {self.data.target_db}.{self.data.target_table} WHERE id IN ({','.join([f"\'{ch}\'" for ch in delete_list])})")
            else:
                delete_table = chunk_list(delete_list, CONFIG.MAX_WHERE_LIST)
                temp_where = "OR ".join([f"id IN ({','.join([f"\'{ch}\'" for ch in ch_list])})" for ch_list in delete_table])
                delete_statement = text(f"DELETE FROM {self.data.target_db}.{self.data.target_table} WHERE {temp_where}")
            self.target.execute(delete_statement)
            self.target.commit()

    def task_append(self, append_list: list[str]) -> None:
        '''查询需要新增和需要变更的数据写入目标表'''
        append_len = len(append_list)
        if append_len > 0:
            in_clause = ', '.join([f"\'{ch}\'" for ch in append_list])
            increase_df: pd.DataFrame = self.source.sql(f"""SELECT * FROM ({self.data.source_sync_sql}) subquery WHERE subquery.id in ({in_clause})""").fetchdf()
            # 因为前文已经检查过目标表的结构了，一般情况下可以成功插入
            increase_df.to_sql(name=self.data.target_table, con=self.target, schema=self.data.target_db, if_exists="append")

    def task_run(self) -> None:
        temp = self.source.sql(f"SELECT 1 FROM information_schema.tables WHERE table_schema = 'meta' AND table_name = '{self.data.target_db}_{self.data.target_table}'").fetchall()
        if len(temp) == 0:
            self.log.warning("本地未找到目标表的元数据缓存，转换为全量同步")
            return self.trans_sync()
        meta_data = get_database_metadata(self.target, self.data.target_db, self.data.target_table)
        local_meta_data: pd.DataFrame = self.source.sql(f"SELECT * FROM meta.{self.data.target_db}_{self.data.target_table}").fetchdf()
        if not meta_data.equals(local_meta_data):
            self.log.warning("目标表元数据和本地表元数据不同，转换为全量同步")
            return self.trans_sync()
        target_increase_df = pd.read_sql(self.data.target_increase_sql, self.target)
        source_increase_df: pd.DataFrame = self.source.sql(self.data.source_increase_sql).fetchdf()
        new_diff, del_diff, change_diff = get_diff(self.source, source_increase_df, target_increase_df)
        # 如果超过要求大小，退化为全量同步
        change_len = len(new_diff) + len(change_diff)
        if change_len > CONFIG.MAX_INCREASE_CHANGE:
            self.log.warning(f"变更行数{str(change_len)}行，超过规定{str(CONFIG.MAX_INCREASE_CHANGE)}，转换为全量同步")
            return self.trans_sync()

        if self.data.is_del:
            self.log.debug(f"本次同步新增{str(len(new_diff))}行，修改{str(len(change_diff))}行，删除{str(len(del_diff))}行")
            self.task_del(change_diff + del_diff)
        else:
            self.log.debug(f"本次同步新增{str(len(new_diff))}行，修改{str(len(change_diff))}行")
            self.task_del(change_diff)
        # 查询需要新增和需要变更的数据写入目标表
        self.task_append(new_diff + change_diff)
        self.log.debug("已成功完成该主表的增量同步")
    
    def task_delete(self) -> None:
        self.source.close()
        self.target.close()

@dataclass
class extract_increase_data:
    name: str
    logger_name: str
    source: str
    source_sync_sql: str
    source_increase_sql: str
    target_table: str
    target_increase_sql: str
    is_del: bool = True


class extract_increase(task):
    def __init__(self, connect: connect_data, data: extract_increase_data) -> None:
        super().__init__(connect.duckdb, data.name, data.logger_name)
        self.data = data
        self.source = connect.connect[data.source]
        self.target = connect.duckdb.cursor()
        self.log.info(f"任务{self.data.name}初始化完成")

    def trans_sync(self) -> None:
        temp_df = pd.read_sql(self.data.source_sync_sql, self.source)
        self.target.execute(f"CREATE OR REPLACE TABLE ods.{self.data.target_table} AS SELECT * FROM temp_df")
        self.log.debug(f"全量抽取已完成")

    def task_del(self, delete_list: list[str]) -> None:
        '''删除目标表中需要删除的和需要变更的'''
        del_len = len(delete_list)
        if del_len > 0:
            if del_len < CONFIG.MAX_WHERE_LIST:
                self.target.execute(f"DELETE FROM ods.{self.data.target_table} WHERE id IN ({','.join([f"\'{ch}\'" for ch in delete_list])})")
            else:
                delete_table = chunk_list(delete_list, CONFIG.MAX_WHERE_LIST)
                temp_where = "OR ".join([f"id IN ({','.join([f"\'{ch}\'" for ch in ch_list])})" for ch_list in delete_table])
                self.target.execute(
                    f"""
                        DELETE FROM ods.{self.data.target_table} 
                        WHERE {temp_where}
                    """
                )

    def task_append(self, append_list: list[str]) -> None:
        '''查询需要新增和需要变更的数据写入目标表'''
        append_len = len(append_list)
        if append_len > 0:
            in_clause = ', '.join([f"\'{ch}\'" for ch in append_list])
            select_statement = text(f"""SELECT * FROM ({self.data.source_sync_sql}) subquery WHERE subquery.id in ({in_clause})""")
            increase_df = pd.read_sql(select_statement, self.source)
            try:
                self.target.execute(f"INSERT INTO ods.{self.data.target_table} SELECT * FROM increase_df")
            except Exception as e:
                self.log.warning(e)
                self.log.warning("插入数据失败，疑似为表结构变更，转换为全量同步")
                return self.trans_sync()

    def task_run(self) -> None:
        temp = self.target.sql(f"SELECT 1 FROM information_schema.tables WHERE table_schema = 'ods' AND table_name = '{self.data.target_table}'").fetchall()
        if len(temp) == 0:
            self.log.warning("本地未找到目标表，转换为全量同步")
            return self.trans_sync()
        taget_increase_df: pd.DataFrame = self.target.sql(self.data.target_increase_sql).fetchdf()
        source_increase_df = pd.read_sql(self.data.source_increase_sql, self.source)
        new_diff, del_diff, change_diff = get_diff(self.target, source_increase_df, taget_increase_df)
        # 如果超过要求大小，退化为全量同步
        change_len = len(new_diff) + len(change_diff)
        if change_len >= CONFIG.MAX_INCREASE_CHANGE:
            self.log.warning(f"变更行数{str(change_len)}行，超过规定{str(CONFIG.MAX_INCREASE_CHANGE)},退化为全量同步")
            return self.trans_sync()
        if self.data.is_del:
            self.log.debug(f"本次同步新增{str(len(new_diff))}行，修改{str(len(change_diff))}行，删除{str(len(del_diff))}行")
            self.task_del(del_diff + change_diff)
        else:
            self.log.debug(f"本次同步新增{str(len(new_diff))}行，修改{str(len(change_diff))}行")
            self.task_del(change_diff)
        self.task_append(new_diff + change_diff)
        self.log.debug("已成功完成该主表的增量同步")
        
    def task_delete(self) -> None:
        self.source.close()
        self.target.close()


@dataclass
class sync_increase_data:
    name: str
    logger_name: str
    source: str
    source_sync_sql: str
    source_increase_sql: str
    target: str
    target_db: str
    target_table: str
    target_increase_sql: str
    is_del: bool = True


class sync_increase(task):
    def __init__(self, connect: connect_data, data: sync_increase_data) -> None:
        super().__init__(connect.duckdb, data.name, data.logger_name)
        self.data = data
        self.local = connect.duckdb.cursor()
        self.source = connect.connect[data.source]
        self.target = connect.connect[data.target]
        self.log.info(f"任务{self.data.name}初始化完成")

    def trans_sync(self) -> None:
        df = pd.read_sql(self.data.source_sync_sql, self.source)
        index = df.to_sql(name=self.data.target_table, con=self.target, schema=self.data.target_db, if_exists="replace")
        self.log.debug(f"全量抽取已完成,抽取了{str(index)}行")
        meta_data = get_database_metadata(self.target, self.data.target_db, self.data.target_table)
        self.local.execute(f"CREATE OR REPLACE TABLE meta.{self.data.target_db}_{self.data.target_table} AS SELECT * FROM meta_data")
        self.log.debug(f"已重建元数据")

    def task_del(self, delete_list: list[str]) -> None:
        '''删除目标表中需要删除的和需要变更的'''
        del_len = len(delete_list)
        if del_len > 0:
            if del_len < CONFIG.MAX_WHERE_LIST:
                delete_statement = text(f"DELETE FROM {self.data.target_db}.{self.data.target_table} WHERE id IN ({','.join([f"\'{ch}\'" for ch in delete_list])})")
            else:
                delete_table = chunk_list(delete_list, CONFIG.MAX_WHERE_LIST)
                temp_where = "OR ".join([f"id IN ({','.join([f"\'{ch}\'" for ch in ch_list])})" for ch_list in delete_table])
                delete_statement = text(f"DELETE FROM {self.data.target_db}.{self.data.target_table} WHERE {temp_where}")
            self.target.execute(delete_statement)
            self.target.commit()

    def task_append(self, append_list: list[str]) -> None:
        '''查询需要新增和需要变更的数据写入目标表'''
        append_len = len(append_list)
        if append_len > 0:
            in_clause = ', '.join([f"\'{ch}\'" for ch in append_list])
            select_statement = text(f"""SELECT * FROM ({self.data.source_sync_sql}) subquery WHERE subquery.id in ({in_clause})""")
            increase_df = pd.read_sql(select_statement, self.source)
            try:
                increase_df.to_sql(name=self.data.target_table, con=self.target, schema=self.data.target_db, if_exists="append")
            except Exception as e:
                self.log.warning(e)
                self.log.warning("插入数据失败，疑似为表结构变更，转换为全量同步")
                return self.trans_sync()

    def task_run(self) -> None:
        temp = self.local.sql(f"SELECT 1 FROM information_schema.tables WHERE table_schema = 'meta' AND table_name = '{self.data.target_db}_{self.data.target_table}'").fetchall()
        if len(temp) == 0:
            self.log.warning("本地未找到目标表的元数据缓存，转换为全量同步")
            return self.trans_sync()
        meta_data = get_database_metadata(self.target, self.data.target_db, self.data.target_table)
        local_meta_data: pd.DataFrame = self.local.sql(f"SELECT * FROM meta.{self.data.target_db}_{self.data.target_table}").fetchdf()
        if not meta_data.equals(local_meta_data):
            self.log.warning("目标表元数据和本地表元数据不同，转换为全量同步")
            return self.trans_sync()
        taget_increase_df = pd.read_sql(self.data.target_increase_sql, self.target)
        source_increase_df = pd.read_sql(self.data.source_increase_sql, self.source)
        new_diff, del_diff, change_diff = get_diff(self.local, source_increase_df, taget_increase_df)
        # 如果超过要求大小，退化为全量同步
        change_len = len(new_diff) + len(change_diff)
        if change_len > CONFIG.MAX_INCREASE_CHANGE:
            self.log.debug(f"变更行数{str(change_len)}行，超过规定{str(CONFIG.MAX_INCREASE_CHANGE)},退化为全量同步")
            return self.trans_sync()
        if self.data.is_del:
            self.log.debug(f"本次同步新增{str(len(new_diff))}行，修改{str(len(change_diff))}行，删除{str(len(del_diff))}行")
            self.task_del(change_diff + del_diff)
        else:
            self.log.debug(f"本次同步新增{str(len(new_diff))}行，修改{str(len(change_diff))}行")
            self.task_del(change_diff)
        self.task_append(new_diff + change_diff)
        self.log.debug("已成功完成该主表的增量同步")

    def task_delete(self) -> None:
        self.local.close()
        self.source.close()
        self.target.close()