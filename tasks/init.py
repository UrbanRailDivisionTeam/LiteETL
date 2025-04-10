import os
from utils import read_sql
from utils.config import DEBUG
from utils.connect import connect_data
from tasks.base import task, extract_increase, extract_increase_data
from tasks.sync import business_connection, interested_party, wire_number, ameliorate, work_time
    

def task_init(connect_data: connect_data) -> list[task]:
    bc_task_group = business_connection.init(connect_data)
    ip_task_group = interested_party.init(connect_data)
    wn_task_group = wire_number.init(connect_data)
    am_task_group = ameliorate.init(connect_data)
    wt_task_group = work_time.init(connect_data)

    task_person = extract_increase(
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

    am_task_group["相关方数据处理"].dp(task_person)

    res = [task_person]
    res += list(bc_task_group.values())
    res += list(ip_task_group.values())
    res += list(wn_task_group.values())
    res += list(am_task_group.values())
    res += list(wt_task_group.values())
    return res
        
