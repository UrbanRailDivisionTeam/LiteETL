import os
from utils.config import CONFIG, DEBUG
from utils.connect import connect_data
from tasks.base import task, extract_increase, extract_increase_data, extract, extract_data
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
    task_e0 = extract_increase(
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
    task_e1 = extract_increase(
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
    task_e2 = extract_increase(
        connect_data,
        extract_increase_data(
            name="相关方安全数据抽取",
            logger_name="interested_party",
            source="相关方数据库" if not DEBUG else "pgsql服务",
            source_sync_sql=read_sql(os.path.join("interested_party", "interested_party", "sync", "interested_party.sql")),
            source_increase_sql=read_sql(os.path.join("interested_party", "interested_party", "increase", "interested_party_source.sql")),
            target_table="interested_party",
            target_increase_sql=read_sql(os.path.join("interested_party", "interested_party", "increase", "interested_party_target.sql")),
        )
    )
    task_e3 = extract_increase(
        connect_data,
        extract_increase_data(
            name="相关方安全随行人员数据抽取",
            logger_name="interested_party_entry",
            source="相关方数据库" if not DEBUG else "pgsql服务",
            source_sync_sql=read_sql(os.path.join("interested_party", "interested_party_entry", "sync", "interested_party_entry.sql")),
            source_increase_sql=read_sql(os.path.join("interested_party", "interested_party_entry", "increase", "interested_party_entry_source.sql")),
            target_table="interested_party_entry",
            target_increase_sql=read_sql(os.path.join("interested_party", "interested_party_entry", "increase", "interested_party_entry_target.sql")),
        )
    )
    # task_e4 = extract_increase(
    #     connect_data,
    #     extract_increase_data(
    #         name="业联-业务联系书数据抽取",
    #         logger_name="business_connection",
    #         source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
    #         source_sync_sql=read_sql(os.path.join("business_connection", "business_connection", "sync", "business_connection.sql")),
    #         source_increase_sql=read_sql(os.path.join("business_connection", "business_connection", "increase", "business_connection_source.sql")),
    #         target_table="business_connection",
    #         target_increase_sql=read_sql(os.path.join("business_connection", "business_connection", "increase", "business_connection_target.sql")),
    #     )
    # )
    # task_e5 = extract(
    #     connect_data,
    #     extract_data(
    #         name="业联-业务联系书主送人数据抽取",
    #         logger_name="business_connection_main_delivery_unit",
    #         source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
    #         source_sql=read_sql(os.path.join("business_connection", "business_connection_main_delivery_unit.sql")),
    #         target_table="business_connection_main_delivery_unit"
    #     )
    # )
    # task_e6 = extract(
    #     connect_data,
    #     extract_data(
    #         name="业联-业务联系书抄送人数据抽取",
    #         logger_name="business_connection_copy_delivery_unit",
    #         source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
    #         source_sql=read_sql(os.path.join("business_connection", "business_connection_copy_delivery_unit.sql")),
    #         target_table="business_connection_copy_delivery_unit"
    #     )
    # )
    # task_e7 = extract_increase(
    #     connect_data,
    #     extract_increase_data(
    #         name="业联-业联执行关闭数据抽取",
    #         logger_name="business_connection_close",
    #         source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
    #         source_sync_sql=read_sql(os.path.join("business_connection", "business_connection_close", "sync", "business_connection_close.sql")),
    #         source_increase_sql=read_sql(os.path.join("business_connection", "business_connection_close", "increase", "business_connection_close_source.sql")),
    #         target_table="business_connection_close",
    #         target_increase_sql=read_sql(os.path.join("business_connection", "business_connection_close", "increase", "business_connection_close_target.sql")),
    #     )
    # )
    # task_e8 = extract_increase(
    #     connect_data,
    #     extract_increase_data(
    #         name="业联-班组基础资料数据抽取",
    #         logger_name="class_group",
    #         source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
    #         source_sync_sql=read_sql(os.path.join("business_connection", "class_group", "sync", "class_group.sql")),
    #         source_increase_sql=read_sql(os.path.join("business_connection", "class_group", "increase", "class_group_source.sql")),
    #         target_table="class_group",
    #         target_increase_sql=read_sql(os.path.join("business_connection", "class_group", "increase", "class_group_target.sql")),
    #     )
    # )
    # task_e9 = extract_increase(
    #     connect_data,
    #     extract_increase_data(
    #         name="业联-班组基础资料分录数据抽取",
    #         logger_name="class_group_entry",
    #         source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
    #         source_sync_sql=read_sql(os.path.join("business_connection", "class_group_entry", "sync", "class_group_entry.sql")),
    #         source_increase_sql=read_sql(os.path.join("business_connection", "class_group_entry", "increase", "class_group_entry_source.sql")),
    #         target_table="class_group_entry",
    #         target_increase_sql=read_sql(os.path.join("business_connection", "class_group_entry", "increase", "class_group_entry_target.sql")),
    #     )
    # )
    
    
    
    task_p0 = ameliorate(connect_data)
    task_p0.dp(task_e0).dp(task_e1)
    
    tasks_group = [
        task_e0,
        task_e1,
        task_e2,
        task_e3,
        # task_e4,
        # task_e5,
        # task_e6,
        # task_e7,
        # task_e8,
        # task_e9,
        
        task_p0
    ]    
    return tasks_group
