import os
from utils.config import CONFIG
from tasks.base import extract, extract_data, task, extract_increase, extract_increase_data


def read_sql(sql_path: str) -> str:
    with open(os.path.abspath(os.path.join(CONFIG.SELECT_PATH, sql_path)), mode="r", encoding="utf-8") as file:
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
        extract_increase(
            extract_increase_data(
                name="车间联络单",
                logger_name="business_connection",
                source="mysql服务",
                source_sync_sql=read_sql(os.path.join("business_connection", "business_connection", "sync", "business_connection.sql")),
                source_increase_sql=read_sql(os.path.join("business_connection", "business_connection", "increase", "business_connection_source.sql")),
                taget_table="business_connection_close",
                taget_increase_sql = read_sql(os.path.join("business_connection", "business_connection", "increase", "business_connection_target.sql")),
            )
        ))
    return tasks_group
