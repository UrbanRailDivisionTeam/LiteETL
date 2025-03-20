import os
from utils.config import CONFIG
from tasks.base import task, sync, sync_data


def read_sql(sql_path: str) -> str:
    with open(os.path.abspath(os.path.join(CONFIG.SOURCE_PATH, sql_path)), mode="r", encoding="utf-8") as file:
        sql_str = file.read()
    return sql_str

def trans_table_to_sql(table_name: str, schema_name: str| None = None) -> str:
    if schema_name is None:
        return f""" SELECT * FROM `{table_name}`"""
    else:
        return f""" SELECT * FROM `{schema_name}`.`{table_name}`"""


def task_init() -> list[task]:
    '''任务真真正进行初始化，并添加依赖关系的地方'''
    tasks_group = []
    tasks_group.append(
        sync(
            sync_data(
                name="业联执行关闭",
                logger_name="business_connection_close",
                source="mysql服务",
                target="clickhouse服务",
                source_sql=read_sql(os.path.join("business_connection", "business_connection", "sync", "business_connection.sql")),
                taget_table="business_connection_close"
            )
        ))
    return tasks_group
