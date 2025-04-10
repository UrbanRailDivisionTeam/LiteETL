import os
from utils import read_sql
from utils.config import DEBUG
from utils.connect import connect_data
from tasks.base import task, extract_increase, extract_increase_data
from tasks.process.wire_number import wire_number


def init(connect_data: connect_data) -> dict[str, task]:
    task_group = [
        extract_increase(
            connect_data,
            extract_increase_data(
                name="线号标签申请上下标数据抽取",
                logger_name="wire_number_head",
                source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
                source_sync_sql=read_sql(os.path.join("wire_number", "head", "sync", "wire_number_head.sql")),
                source_increase_sql=read_sql(os.path.join("wire_number", "head", "increase", "wire_number_head_source.sql")),
                target_table="wire_number_head",
                target_increase_sql=read_sql(os.path.join("wire_number", "head", "increase", "wire_number_head_target.sql")),
            )
        ),
        extract_increase(
            connect_data,
            extract_increase_data(
                name="线号标签申请位置号数据抽取",
                logger_name="wire_number_entry",
                source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
                source_sync_sql=read_sql(os.path.join("wire_number", "entry", "sync", "wire_number_entry.sql")),
                source_increase_sql=read_sql(os.path.join("wire_number", "entry", "increase", "wire_number_entry_source.sql")),
                target_table="wire_number_entry",
                target_increase_sql=read_sql(os.path.join("wire_number", "entry", "increase", "wire_number_entry_target.sql")),
            )
        ),
        wire_number(connect_data)
    ]

    keys = [ch.name for ch in task_group]
    res: dict[str, task] = dict(zip(keys, task_group))
    
    res["线号标签申请数据处理"].dp(res["线号标签申请上下标数据抽取"]).dp(res["线号标签申请位置号数据抽取"])
    return res
