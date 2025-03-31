import os
from utils.config import CONFIG, DEBUG
from utils.connect import connecter, connect_data
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
    

def task_init(connect_data: connect_data) -> list[task]:
    '''任务进行初始化，并添加依赖关系的地方'''
    
    task_0 = extract_increase(
        connect_data,
        extract_increase_data(
            name="改善数据抽取",
            logger_name="ameliorate",
            source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
            source_sync_sql=read_sql(os.path.join("ameliorate", "sync", "ameliorate.sql")),
            source_increase_sql=read_sql(os.path.join("ameliorate", "increase", "ameliorate_source.sql")),
            target_table="ameliorate",
            target_increase_sql=read_sql(os.path.join("ameliorate", "increase", "ameliorate_target.sql")),
        )
    )
    task_1 = extract_increase(
        connect_data,
        extract_increase_data(
            name="人员基础数据抽取",
            logger_name="person",
            source="SHR" if not DEBUG else "oracle服务",
            source_sync_sql=read_sql(os.path.join("person", "sync", "person.sql")),
            source_increase_sql=read_sql(os.path.join("person", "increase", "person_source.sql")),
            target_table="person",
            target_increase_sql=read_sql(os.path.join("person", "increase", "person_target.sql")),
        )
    )
    
    
    
    task_2 = ameliorate(connect_data)
    task_2.dp(task_0).dp(task_1)
    
    tasks_group = []
    tasks_group.append(task_0)
    tasks_group.append(task_1)
    tasks_group.append(task_2)
    return tasks_group
