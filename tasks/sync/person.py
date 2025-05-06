import os
from utils import read_sql
from utils.connect import connect_data
from tasks.sync import init_warpper
from tasks.base import task, extract_increase, extract_increase_data


@init_warpper
def init(connect_data: connect_data) -> list[task]:
    return [
        extract_increase(
            connect_data,
            extract_increase_data(
                name="人员基础数据抽取",
                logger_name="person",
                source="SHR",
                source_sync_sql=read_sql(os.path.join("person", "sync", "person.sql")),
                source_increase_sql=read_sql(os.path.join("person", "increase", "person_source.sql")),
                target_table="person",
                target_increase_sql=read_sql(os.path.join("person", "increase", "person_target.sql")),
            )
        )
    ]