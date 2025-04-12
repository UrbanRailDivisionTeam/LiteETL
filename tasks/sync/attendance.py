import os
from utils import read_sql
from utils.config import DEBUG
from utils.connect import connect_data
from tasks.sync import init_warpper
from tasks.base import task, extract_increase, extract_increase_data, extract, extract_data

@init_warpper
def init(connect_data: connect_data) -> list[task]:
    return [
        extract_increase(
            connect_data,
            extract_increase_data(
                name="考勤数据抽取",
                logger_name="attendance_kq_time",
                source="考勤系统" if not DEBUG else "oracle服务",
                source_sync_sql=read_sql(os.path.join("attendance", "kq_time", "sync", "kq_time.sql")),
                source_increase_sql=read_sql(os.path.join("attendance", "kq_time", "increase", "kq_time_source.sql")),
                target_table="attendance_kq_time",
                target_increase_sql=read_sql(os.path.join("attendance", "kq_time", "increase", "kq_time_target.sql")),
                is_del=False
            )
        ),
        extract_increase(
            connect_data,
            extract_increase_data(
                name="考勤节假日数据抽取",
                logger_name="attendance_kq_scheduling_holiday",
                source="考勤系统" if not DEBUG else "oracle服务",
                source_sync_sql=read_sql(os.path.join("attendance", "kq_scheduling_holiday", "sync", "kq_scheduling_holiday.sql")),
                source_increase_sql=read_sql(os.path.join("attendance", "kq_scheduling_holiday", "increase", "kq_scheduling_holiday_source.sql")),
                target_table="attendance_kq_scheduling_holiday",
                target_increase_sql=read_sql(os.path.join("attendance", "kq_scheduling_holiday", "increase", "kq_scheduling_holiday_target.sql")),
            )
        ),
        extract(
            connect_data,
            extract_data(
                name="考勤类型数据抽取",
                logger_name="kq_type",
                source="考勤系统" if not DEBUG else "oracle服务",
                source_sql=read_sql(os.path.join("attendance", "kq_type.sql")),
                target_table="attendance_kq_type"
            )
        )
    ]