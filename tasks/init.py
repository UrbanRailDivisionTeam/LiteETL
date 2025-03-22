import os
from utils.config import CONFIG
from tasks.base import task, extract_increase, extract_increase_data
from tasks.process.ameliorate import ameliorate

def read_sql(sql_path: str) -> str:
    with open(os.path.abspath(os.path.join(CONFIG.SELECT_PATH, sql_path)), mode="r", encoding="utf-8") as file:
        sql_str = file.read()
    return sql_str


def trans_table_to_sql(table_name: str, schema_name: str | None = None) -> str:
    if schema_name is None:
        return f""" SELECT * FROM `{table_name}`"""
    else:
        return f""" SELECT * FROM `{schema_name}`.`{table_name}`"""


def task_init() -> list[task]:
    '''任务真真正进行初始化，并添加依赖关系的地方'''
    # 任务初始化
    tesk_0 = extract_increase(
            extract_increase_data(
                name="改善数据处理",
                logger_name="ameliorate",
                source="mysql服务",
                source_sync_sql=read_sql(os.path.join("ameliorate", "sync", "business_connection.sql")),
                source_increase_sql=read_sql(os.path.join("ameliorate", "increase", "ameliorate_source.sql")),
                taget_table="business_connection_close",
                taget_increase_sql=read_sql(os.path.join("ameliorate", "increase", "ameliorate_target.sql")),
            )
        )
    tesk_1 = ameliorate()
    
    # 添加任务间关系
    tesk_0.then(tesk_1)
    
    # 整理首先要运行的任务
    tasks_group = []
    tasks_group.append(tesk_0)
    return tasks_group
