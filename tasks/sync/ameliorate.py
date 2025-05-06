import os
from utils import read_sql
from utils.connect import connect_data
from tasks.base import task, extract_increase, extract_increase_data
from tasks.sync import init_warpper
from tasks.process.ameliorate import ameliorate_process


@init_warpper
def init(connect_data: connect_data) -> list[task]:
    return [
        extract_increase(
            connect_data,
            extract_increase_data(
                name="改善数据抽取",
                logger_name="ameliorate",
                source="金蝶云苍穹-正式库",
                source_sync_sql=read_sql(os.path.join("ameliorate", "sync", "ameliorate.sql")),
                source_increase_sql=read_sql(os.path.join("ameliorate", "increase", "ameliorate_source.sql")),
                target_table="ameliorate",
                target_increase_sql=read_sql(os.path.join("ameliorate", "increase", "ameliorate_target.sql")),
            )
        ),
        ameliorate_process(connect_data)
    ]
