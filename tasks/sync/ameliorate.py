import os
from utils import read_sql
from utils.config import DEBUG
from utils.connect import connect_data
from tasks.base import task, extract_increase, extract_increase_data
from tasks.process.ameliorate import ameliorate


def init(connect_data: connect_data) -> dict[str, task]:
    task_group = [
        extract_increase(
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
        ),
        ameliorate(connect_data)
    ]

    keys = [ch.name for ch in task_group]
    res: dict[str, task] = dict(zip(keys, task_group))

    res["全员型改善数据分析"].dp(res["改善数据抽取"])
    return res
