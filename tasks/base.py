import time
import duckdb
import traceback
import sqlalchemy
import pandas as pd
from dataclasses import dataclass
from sqlalchemy import text
from abc import ABC, abstractmethod

from utils.config import CONFIG
from utils.connect import connect_data
from utils.logger import make_logger


class task(ABC):
    '''所有任务的抽象'''

    def __init__(self, _duckdb: duckdb.DuckDBPyConnection, name: str, logger_name: str) -> None:
        self.name = name
        self.start_run = False
        self.end_run = False
        self.log = make_logger(_duckdb, name, logger_name)
        self.depend: list[str] = []

    def dp(self, task_name: 'task') -> 'task':
        if task_name.name == self.name:
            raise ValueError("不能将自己设置为前置依赖")
        self.depend.append(task_name.name)
        return self

    @abstractmethod
    def task_main(self) -> None:
        # 继承后实现逻辑的地方
        ...

    def run(self) -> None:
        # 真正运行函数的地方
        if not self.start_run and not self.end_run:
            self.start_run = True
            start_time = time.time()
            try:
                self.task_main()
            except Exception as e:
                self.log.critical("报错内容：" + str(e))
                self.log.critical("报错堆栈信息：" + str(traceback.format_exc()))
            end_time = time.time()
            self.end_run = True
            self.log.info("函数花费时间为:{} 秒,".format(end_time - start_time) + f"{self.name} 任务已完成")


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
    target_table: str
    source: str


class extract(task):
    def __init__(self, connect: connect_data, data: extract_data) -> None:
        super().__init__(connect.duckdb, data.name, data.logger_name)
        self.data = data
        self.source = connect.connect[data.source]
        self.target = connect.duckdb.cursor()
        self.log.info(f"任务{self.data.name}初始化完成")

    def task_main(self) -> None:
        temp_df = pd.read_sql(self.data.source_sql, self.source)
        self.target.execute(f"CREATE OR REPLACE TABLE ods.{self.data.target_table} AS SELECT * FROM temp_df")
        self.log.debug(f"全量抽取已完成")

    def __del__(self) -> None:
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

    def task_main(self) -> None:
        temp_df = pd.read_sql(self.data.source_sql, self.source)
        index = temp_df.to_sql(name=self.data.taget_table, con=self.target, schema=self.data.target_db, if_exists="replace")
        self.log.debug(f"全量同步已完成，变更{str(index)}行")

    def __del__(self) -> None:
        self.source.close()
        self.target.close()


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

    def task_main(self) -> None:
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
        else:
            if self.data.is_del:
                self.log.debug(f"本次同步新增{str(len(new_diff))}行，修改{str(len(change_diff))}行，删除{str(len(del_diff))}行")
            else:
                self.log.debug(f"本次同步新增{str(len(new_diff))}行，修改{str(len(change_diff))}行")
        # 删除目标表中需要删除的和需要变更的
        if self.data.is_del:
            if len(del_diff) + len(change_diff) > 0:
                ids_to_delete = del_diff + change_diff
                delete_statement = text(f"DELETE FROM {self.data.target_db}.{self.data.target_table} WHERE id IN ({','.join([f"\'{ch}\'" for ch in ids_to_delete])})")
                self.target.execute(delete_statement)
                self.target.commit()
        else:
            if len(change_diff) > 0:
                delete_statement = text(f"DELETE FROM {self.data.target_db}.{self.data.target_table} WHERE id IN ({','.join([f"\'{ch}\'" for ch in change_diff])})")
                self.target.execute(delete_statement)
                self.target.commit()
        # 查询需要新增和需要变更的数据写入目标表
        if len(new_diff) + len(change_diff) > 0:
            ids_to_select = new_diff + change_diff
            in_clause = ', '.join([f"\'{ch}\'" for ch in ids_to_select])
            increase_df: pd.DataFrame = self.source.sql(f"""SELECT * FROM ({self.data.source_sync_sql}) subquery WHERE subquery.id in ({in_clause})""").fetchdf()
            # 因为前文已经检查过目标表的结构了，一般情况下可以成功插入
            increase_df.to_sql(name=self.data.target_table, con=self.target, schema=self.data.target_db, if_exists="append")
        self.log.debug("已成功完成该主表的增量同步")


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

    def task_main(self) -> None:
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
        else:
            if self.data.is_del:
                self.log.debug(f"本次同步新增{str(len(new_diff))}行，修改{str(len(change_diff))}行，删除{str(len(del_diff))}行")
            else:
                self.log.debug(f"本次同步新增{str(len(new_diff))}行，修改{str(len(change_diff))}行")
        # 删除目标表中需要删除的和需要变更的
        if self.data.is_del:
            if len(del_diff) + len(change_diff) > 0:
                ids_to_delete = del_diff + change_diff
                self.target.execute(f"DELETE FROM ods.{self.data.target_table} WHERE id IN ({','.join([f"\'{ch}\'" for ch in ids_to_delete])})")
        else:
            if len(change_diff) > 0:
                self.target.execute(f"DELETE FROM ods.{self.data.target_table} WHERE id IN ({','.join([f"\'{ch}\'" for ch in change_diff])})")
        # 查询需要新增和需要变更的数据写入目标表
        if len(new_diff) + len(change_diff) > 0:
            ids_to_select = new_diff + change_diff
            in_clause = ', '.join([f"\'{ch}\'" for ch in ids_to_select])
            # 为了解决oracle数据库兼容问题
            select_statement = text(f"""SELECT * FROM ({self.data.source_sync_sql}) subquery WHERE subquery.id in ({in_clause})""")
            increase_df = pd.read_sql(select_statement, self.source)
            try:
                self.target.execute(f"INSERT INTO ods.{self.data.target_table} SELECT * FROM increase_df")
            except Exception as e:
                self.log.warning(e)
                self.log.warning("插入数据失败，疑似为表结构变更，转换为全量同步")
                return self.trans_sync()
        self.log.debug("已成功完成该主表的增量同步")


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

    def task_main(self) -> None:
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
        else:
            if self.data.is_del:
                self.log.debug(f"本次同步新增{str(len(new_diff))}行，修改{str(len(change_diff))}行，删除{str(len(del_diff))}行")
            else:
                self.log.debug(f"本次同步新增{str(len(new_diff))}行，修改{str(len(change_diff))}行")
        if self.data.is_del:
            # 删除目标表中需要删除的和需要变更的
            if len(del_diff) + len(change_diff) > 0:
                ids_to_delete = del_diff + change_diff
                delete_statement = text(f"DELETE FROM {self.data.target_db}.{self.data.target_table} WHERE id IN ({','.join([f"\'{ch}\'" for ch in ids_to_delete])})")
                self.target.execute(delete_statement)
                self.target.commit()
        else:
            if len(change_diff) > 0:
                delete_statement = text(f"DELETE FROM {self.data.target_db}.{self.data.target_table} WHERE id IN ({','.join([f"\'{ch}\'" for ch in change_diff])})")
                self.target.execute(delete_statement)
                self.target.commit()
        # 查询需要新增和需要变更的数据写入目标表
        if len(new_diff) + len(change_diff) > 0:
            ids_to_select = new_diff + change_diff
            in_clause = ', '.join([f"\'{ch}\'" for ch in ids_to_select])
            select_statement = text(f"""SELECT * FROM ({self.data.source_sync_sql}) subquery WHERE subquery.id in ({in_clause})""")
            increase_df = pd.read_sql(select_statement, self.source)
            try:
                increase_df.to_sql(name=self.data.target_table, con=self.target, schema=self.data.target_db, if_exists="append")
            except Exception as e:
                self.log.warning(e)
                self.log.warning("插入数据失败，疑似为表结构变更，转换为全量同步")
                return self.trans_sync()
        self.log.debug("已成功完成该主表的增量同步")
