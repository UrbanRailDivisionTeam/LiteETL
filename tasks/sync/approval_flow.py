import os
from utils import read_sql
from utils.config import DEBUG
from utils.connect import connect_data
from tasks.base import task, extract_increase, extract_increase_data
from tasks.sync import init_warpper
from tasks.process.approval_flow import entity_design_process


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
                name="单据数据模型抽取",
                logger_name="af_entity_design",
                source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
                source_sync_sql=read_sql(os.path.join("approval_flow", "entity_design", "sync", "entity_design.sql")),
                source_increase_sql=read_sql(os.path.join("approval_flow", "entity_design", "increase", "entity_design_source.sql")),
                target_table="af_entity_design",
                target_increase_sql=read_sql(os.path.join("approval_flow", "entity_design", "increase", "entity_design_target.sql")),
            )
        ),
        entity_design_process(connect_data)
    ]
