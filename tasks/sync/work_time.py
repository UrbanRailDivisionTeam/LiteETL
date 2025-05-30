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
                name="线上工时数据抽取",
                logger_name="online_worktime",
                source="EAS" ,
                source_sync_sql=read_sql(os.path.join("worktime", "online_worktime", "sync", "online_worktime.sql")),
                source_increase_sql=read_sql(os.path.join("worktime", "online_worktime", "increase", "online_worktime_source.sql")),
                target_table="online_worktime",
                target_increase_sql=read_sql(os.path.join("worktime", "online_worktime", "increase", "online_worktime_target.sql")),
                is_del=False
            )
        ),
        extract_increase(
            connect_data,
            extract_increase_data(
                name="临时工时数据抽取",
                logger_name="temp_worktime",
                source="生产辅助系统-城轨" ,
                source_sync_sql=read_sql(os.path.join("worktime", "temp_worktime", "sync", "temp_worktime.sql")),
                source_increase_sql=read_sql(os.path.join("worktime", "temp_worktime", "increase", "temp_worktime_source.sql")),
                target_table="temp_worktime",
                target_increase_sql=read_sql(os.path.join("worktime", "temp_worktime", "increase", "temp_worktime_target.sql")),
            )
        ),
    ]
