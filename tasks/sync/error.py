import os
from utils import read_sql
from utils.connect import connect_data
from tasks.sync import init_warpper
from tasks.base import task, extract_increase, extract_increase_data
from tasks.process.error import alignment_error_process


@init_warpper
def init(connect_data: connect_data) -> list[task]:
    return [
        extract_increase(
            connect_data,
            extract_increase_data(
                name="校线异常发起单数据抽取",
                logger_name="alignment_error_handle",
                source="金蝶云苍穹-正式库",
                source_sync_sql=read_sql(os.path.join("error", "alignment_error_handle", "sync", "alignment_error_handle.sql")),
                source_increase_sql=read_sql(os.path.join("error", "alignment_error_handle", "increase", "alignment_error_handle_source.sql")),
                target_table="alignment_error_handle",
                target_increase_sql=read_sql(os.path.join("error", "alignment_error_handle", "increase", "alignment_error_handle_target.sql")),
            )
        ),
        extract_increase(
            connect_data,
            extract_increase_data(
                name="校线异常处理单数据抽取",
                logger_name="alignment_error_initiate",
                source="金蝶云苍穹-正式库",
                source_sync_sql=read_sql(os.path.join("error", "alignment_error_initiate", "sync", "alignment_error_initiate.sql")),
                source_increase_sql=read_sql(os.path.join("error", "alignment_error_initiate", "increase", "alignment_error_initiate_source.sql")),
                target_table="alignment_error_initiate",
                target_increase_sql=read_sql(os.path.join("error", "alignment_error_initiate", "increase", "alignment_error_initiate_target.sql")),
            )
        ),
        alignment_error_process(connect_data)
    ]