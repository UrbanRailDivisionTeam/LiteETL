import os
from utils import read_sql
from utils.config import DEBUG
from utils.connect import connect_data
from tasks.base import task, extract_increase, extract_increase_data
from tasks.sync import init_warpper


@init_warpper
def init(connect_data: connect_data) -> list[task]:
    return [
        extract_increase(
            connect_data,
            extract_increase_data(
                name="流程操作日志数据抽取",
                logger_name="af_operation_log",
                source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
                source_sync_sql=read_sql(os.path.join("approval_flow", "operation_log", "sync", "operation_log.sql")),
                source_increase_sql=read_sql(os.path.join("approval_flow", "operation_log", "increase", "operation_log_source.sql")),
                target_table="af_operation_log",
                target_increase_sql=read_sql(os.path.join("approval_flow", "operation_log", "increase", "operation_log_target.sql")),
            )
        ),
        extract_increase(
            connect_data,
            extract_increase_data(
                name="流程实例数据抽取",
                logger_name="af_current_flow",
                source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
                source_sync_sql=read_sql(os.path.join("approval_flow", "current_flow", "sync", "current_flow.sql")),
                source_increase_sql=read_sql(os.path.join("approval_flow", "current_flow", "increase", "current_flow_source.sql")),
                target_table="af_current_flow",
                target_increase_sql=read_sql(os.path.join("approval_flow", "current_flow", "increase", "current_flow_target.sql")),
            )
        ),
        extract_increase(
            connect_data,
            extract_increase_data(
                name="历史流程实例数据抽取",
                logger_name="af_historical_flow",
                source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
                source_sync_sql=read_sql(os.path.join("approval_flow", "historical_flow", "sync", "historical_flow.sql")),
                source_increase_sql=read_sql(os.path.join("approval_flow", "historical_flow", "increase", "historical_flow_source.sql")),
                target_table="af_historical_flow",
                target_increase_sql=read_sql(os.path.join("approval_flow", "historical_flow", "increase", "historical_flow_target.sql")),
            )
        ),
        extract_increase(
            connect_data,
            extract_increase_data(
                name="单据数据模型抽取",
                logger_name="af_entity_info",
                source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
                source_sync_sql=read_sql(os.path.join("approval_flow", "entity_info", "sync", "entity_info.sql")),
                source_increase_sql=read_sql(os.path.join("approval_flow", "entity_info", "increase", "entity_info_source.sql")),
                target_table="af_entity_info",
                target_increase_sql=read_sql(os.path.join("approval_flow", "entity_info", "increase", "entity_info_target.sql")),
            )
        ),
    ]
