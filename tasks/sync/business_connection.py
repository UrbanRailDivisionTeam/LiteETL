import os
from utils import read_sql
from utils.connect import connect_data
from tasks.sync import init_warpper
from tasks.base import task, extract_increase, extract_increase_data, extract, extract_data


@init_warpper
def init(connect_data: connect_data) -> list[task]:
    return [
        extract_increase(
            connect_data,
            extract_increase_data(
                name="业联-业务联系书数据抽取",
                logger_name="business_connection",
                source="金蝶云苍穹-正式库" ,
                source_sync_sql=read_sql(os.path.join("business_connection", "business_connection", "sync", "business_connection.sql")),
                source_increase_sql=read_sql(os.path.join("business_connection", "business_connection", "increase", "business_connection_source.sql")),
                target_table="business_connection",
                target_increase_sql=read_sql(os.path.join("business_connection", "business_connection", "increase", "business_connection_target.sql")),
            )
        ),
        extract(
            connect_data,
            extract_data(
                name="业联-业务联系书主送人数据抽取",
                logger_name="business_connection_main_delivery_unit",
                source="金蝶云苍穹-正式库" ,
                source_sql=read_sql(os.path.join("business_connection", "business_connection_main_delivery_unit.sql")),
                target_table="business_connection_main_delivery_unit"
            )
        ),
        extract(
            connect_data,
            extract_data(
                name="业联-业务联系书抄送人数据抽取",
                logger_name="business_connection_copy_delivery_unit",
                source="金蝶云苍穹-正式库" ,
                source_sql=read_sql(os.path.join("business_connection", "business_connection_copy_delivery_unit.sql")),
                target_table="business_connection_copy_delivery_unit"
            )
        ),
        extract_increase(
            connect_data,
            extract_increase_data(
                name="业联-业联执行关闭数据抽取",
                logger_name="business_connection_close",
                source="金蝶云苍穹-正式库" ,
                source_sync_sql=read_sql(os.path.join("business_connection", "business_connection_close", "sync", "business_connection_close.sql")),
                source_increase_sql=read_sql(os.path.join("business_connection", "business_connection_close", "increase", "business_connection_close_source.sql")),
                target_table="business_connection_close",
                target_increase_sql=read_sql(os.path.join("business_connection", "business_connection_close", "increase", "business_connection_close_target.sql")),
            )
        ),
        extract_increase(
            connect_data,
            extract_increase_data(
                name="业联-班组基础资料数据抽取",
                logger_name="class_group",
                source="金蝶云苍穹-正式库" ,
                source_sync_sql=read_sql(os.path.join("business_connection", "class_group", "sync", "class_group.sql")),
                source_increase_sql=read_sql(os.path.join("business_connection", "class_group", "increase", "class_group_source.sql")),
                target_table="class_group",
                target_increase_sql=read_sql(os.path.join("business_connection", "class_group", "increase", "class_group_target.sql")),
            )
        ),
        extract_increase(
            connect_data,
            extract_increase_data(
                name="业联-班组基础资料分录数据抽取",
                logger_name="class_group_entry",
                source="金蝶云苍穹-正式库" ,
                source_sync_sql=read_sql(os.path.join("business_connection", "class_group_entry", "sync", "class_group_entry.sql")),
                source_increase_sql=read_sql(os.path.join("business_connection", "class_group_entry", "increase", "class_group_entry_source.sql")),
                target_table="class_group_entry",
                target_increase_sql=read_sql(os.path.join("business_connection", "class_group_entry", "increase", "class_group_entry_target.sql")),
            )
        ),
        extract_increase(
            connect_data,
            extract_increase_data(
                name="业联-设计变更单数据抽取",
                logger_name="design_change",
                source="金蝶云苍穹-正式库" ,
                source_sync_sql=read_sql(os.path.join("business_connection", "design_change", "sync", "design_change.sql")),
                source_increase_sql=read_sql(os.path.join("business_connection", "design_change", "increase", "design_change_source.sql")),
                target_table="design_change",
                target_increase_sql=read_sql(os.path.join("business_connection", "design_change", "increase", "design_change_target.sql")),
            )
        ),
        extract_increase(
            connect_data,
            extract_increase_data(
                name="业联-设计变更单分录数据抽取",
                logger_name="design_change_entry",
                source="金蝶云苍穹-正式库" ,
                source_sync_sql=read_sql(os.path.join("business_connection", "design_change_entry", "sync", "design_change_entry.sql")),
                source_increase_sql=read_sql(os.path.join("business_connection", "design_change_entry", "increase", "design_change_entry_source.sql")),
                target_table="design_change_entry",
                target_increase_sql=read_sql(os.path.join("business_connection", "design_change_entry", "increase", "design_change_entry_target.sql")),
            )
        ),
        extract_increase(
            connect_data,
            extract_increase_data(
                name="业联-设计变更执行数据抽取",
                logger_name="design_change_execution",
                source="金蝶云苍穹-正式库" ,
                source_sync_sql=read_sql(os.path.join("business_connection", "design_change_execution", "sync", "design_change_execution.sql")),
                source_increase_sql=read_sql(os.path.join("business_connection", "design_change_execution", "increase", "design_change_execution_source.sql")),
                target_table="design_change_execution",
                target_increase_sql=read_sql(os.path.join("business_connection", "design_change_execution", "increase", "design_change_execution_target.sql")),
            )
        ),
        extract_increase(
            connect_data,
            extract_increase_data(
                name="业联-设计变更执行审核数据抽取",
                logger_name="design_change_execution_audit",
                source="金蝶云苍穹-正式库" ,
                source_sync_sql=read_sql(os.path.join("business_connection", "design_change_execution_audit", "sync", "design_change_execution_audit.sql")),
                source_increase_sql=read_sql(os.path.join("business_connection", "design_change_execution_audit", "increase", "design_change_execution_audit_source.sql")),
                target_table="design_change_execution_audit",
                target_increase_sql=read_sql(os.path.join("business_connection", "design_change_execution_audit", "increase", "design_change_execution_audit_target.sql")),
            )
        ),
        extract_increase(
            connect_data,
            extract_increase_data(
                name="业联-设计变更执行变更内容数据抽取",
                logger_name="design_change_execution_change_content",
                source="金蝶云苍穹-正式库" ,
                source_sync_sql=read_sql(os.path.join("business_connection", "design_change_execution_change_content", "sync", "design_change_execution_change_content.sql")),
                source_increase_sql=read_sql(os.path.join("business_connection", "design_change_execution_change_content", "increase", "design_change_execution_change_content_source.sql")),
                target_table="design_change_execution_change_content",
                target_increase_sql=read_sql(os.path.join("business_connection", "design_change_execution_change_content", "increase", "design_change_execution_change_content_target.sql")),
            )
        ),
        extract_increase(
            connect_data,
            extract_increase_data(
                name="业联-设计变更执行文档变更数据抽取",
                logger_name="design_change_execution_document_change",
                source="金蝶云苍穹-正式库" ,
                source_sync_sql=read_sql(os.path.join("business_connection", "design_change_execution_document_change", "sync", "design_change_execution_document_change.sql")),
                source_increase_sql=read_sql(os.path.join("business_connection", "design_change_execution_document_change", "increase", "design_change_execution_document_change_source.sql")),
                target_table="design_change_execution_document_change",
                target_increase_sql=read_sql(os.path.join("business_connection", "design_change_execution_document_change", "increase", "design_change_execution_document_change_target.sql")),
            )
        ),
        extract_increase(
            connect_data,
            extract_increase_data(
                name="业联-设计变更执行经办人数据抽取",
                logger_name="design_change_execution_handle",
                source="金蝶云苍穹-正式库" ,
                source_sync_sql=read_sql(os.path.join("business_connection", "design_change_execution_handle", "sync", "design_change_execution_handle.sql")),
                source_increase_sql=read_sql(os.path.join("business_connection", "design_change_execution_handle", "increase", "design_change_execution_handle_source.sql")),
                target_table="design_change_execution_handle",
                target_increase_sql=read_sql(os.path.join("business_connection", "design_change_execution_handle", "increase", "design_change_execution_handle_target.sql")),
            )
        ),
        extract_increase(
            connect_data,
            extract_increase_data(
                name="业联-设计变更执行物料变更数据抽取",
                logger_name="design_change_execution_material_change",
                source="金蝶云苍穹-正式库" ,
                source_sync_sql=read_sql(os.path.join("business_connection", "design_change_execution_material_change", "sync", "design_change_execution_material_change.sql")),
                source_increase_sql=read_sql(os.path.join("business_connection", "design_change_execution_material_change", "increase", "design_change_execution_material_change_source.sql")),
                target_table="design_change_execution_material_change",
                target_increase_sql=read_sql(os.path.join("business_connection", "design_change_execution_material_change", "increase", "design_change_execution_material_change_target.sql")),
            )
        ),
        extract_increase(
            connect_data,
            extract_increase_data(
                name="业联-设计变更执行备料工艺数据抽取",
                logger_name="design_change_execution_material_preparation_technology",
                source="金蝶云苍穹-正式库" ,
                source_sync_sql=read_sql(os.path.join("business_connection", "design_change_execution_material_preparation_technology", "sync", "design_change_execution_material_preparation_technology.sql")),
                source_increase_sql=read_sql(os.path.join("business_connection", "design_change_execution_material_preparation_technology", "increase", "design_change_execution_material_preparation_technology_source.sql")),
                target_table="design_change_execution_material_preparation_technology",
                target_increase_sql=read_sql(os.path.join("business_connection", "design_change_execution_material_preparation_technology", "increase", "design_change_execution_material_preparation_technology_target.sql")),
            )
        ),
        extract_increase(
            connect_data,
            extract_increase_data(
                name="业联-设计变更执行返工工艺数据抽取",
                logger_name="design_change_execution_reworked_material",
                source="金蝶云苍穹-正式库" ,
                source_sync_sql=read_sql(os.path.join("business_connection", "design_change_execution_reworked_material", "sync", "design_change_execution_reworked_material.sql")),
                source_increase_sql=read_sql(os.path.join("business_connection", "design_change_execution_reworked_material", "increase", "design_change_execution_reworked_material_source.sql")),
                target_table="design_change_execution_reworked_material",
                target_increase_sql=read_sql(os.path.join("business_connection", "design_change_execution_reworked_material", "increase", "design_change_execution_reworked_material_target.sql")),
            )
        ),
        extract(
            connect_data,
            extract_data(
                name="业联-设计变更执行抄送人数据抽取",
                logger_name="design_change_execution_copy_delivery_unit",
                source="金蝶云苍穹-正式库" ,
                source_sql=read_sql(os.path.join("business_connection", "design_change_execution_copy_delivery_unit.sql")),
                target_table="design_change_execution_copy_delivery_unit"
            )
        ),
        extract(
            connect_data,
            extract_data(
                name="业联-设计变更执行文档变更分录数据抽取",
                logger_name="design_change_execution_document_change_unit",
                source="金蝶云苍穹-正式库" ,
                source_sql=read_sql(os.path.join("business_connection", "design_change_execution_document_change_unit.sql")),
                target_table="design_change_execution_document_change_unit"
            )
        ),
        extract(
            connect_data,
            extract_data(
                name="业联-设计变更执行主送人数据抽取",
                logger_name="design_change_execution_main_delivery_unit",
                source="金蝶云苍穹-正式库" ,
                source_sql=read_sql(os.path.join("business_connection", "design_change_execution_main_delivery_unit.sql")),
                target_table="design_change_execution_main_delivery_unit"
            )
        ),
        extract(
            connect_data,
            extract_data(
                name="业联-设计变更执行备料工艺分录数据抽取",
                logger_name="design_change_execution_material_preparation_technology_unit",
                source="金蝶云苍穹-正式库" ,
                source_sql=read_sql(os.path.join("business_connection", "design_change_execution_material_preparation_technology_unit.sql")),
                target_table="design_change_execution_material_preparation_technology_unit"
            )
        ),
        extract(
            connect_data,
            extract_data(
                name="业联-设计变更执行返工工艺分录数据抽取",
                logger_name="design_change_execution_reworked_material_unit",
                source="金蝶云苍穹-正式库" ,
                source_sql=read_sql(os.path.join("business_connection", "design_change_execution_reworked_material_unit.sql")),
                target_table="design_change_execution_reworked_material_unit"
            )
        ),
        extract_increase(
            connect_data,
            extract_increase_data(
                name="业联-工艺流程数据抽取",
                logger_name="technological_process",
                source="金蝶云苍穹-正式库" ,
                source_sync_sql=read_sql(os.path.join("business_connection", "technological_process", "sync", "technological_process.sql")),
                source_increase_sql=read_sql(os.path.join("business_connection", "technological_process", "increase", "technological_process_source.sql")),
                target_table="technological_process",
                target_increase_sql=read_sql(os.path.join("business_connection", "technological_process", "increase", "technological_process_target.sql")),
            )
        ),
        extract_increase(
            connect_data,
            extract_increase_data(
                name="业联-工艺流程工艺变更数据抽取",
                logger_name="technological_process_change",
                source="金蝶云苍穹-正式库" ,
                source_sync_sql=read_sql(os.path.join("business_connection", "technological_process_change", "sync", "technological_process_change.sql")),
                source_increase_sql=read_sql(os.path.join("business_connection", "technological_process_change", "increase", "technological_process_change_source.sql")),
                target_table="technological_process_change",
                target_increase_sql=read_sql(os.path.join("business_connection", "technological_process_change", "increase", "technological_process_change_target.sql")),
            )
        ),
        extract_increase(
            connect_data,
            extract_increase_data(
                name="业联-工艺流程任务流程数据抽取",
                logger_name="technological_process_flow",
                source="金蝶云苍穹-正式库" ,
                source_sync_sql=read_sql(os.path.join("business_connection", "technological_process_flow", "sync", "technological_process_flow.sql")),
                source_increase_sql=read_sql(os.path.join("business_connection", "technological_process_flow", "increase", "technological_process_flow_source.sql")),
                target_table="technological_process_flow",
                target_increase_sql=read_sql(os.path.join("business_connection", "technological_process_flow", "increase", "technological_process_flow_target.sql")),
            )
        ),
        extract_increase(
            connect_data,
            extract_increase_data(
                name="业联-车间执行单数据抽取",
                logger_name="shop_execution",
                source="金蝶云苍穹-正式库" ,
                source_sync_sql=read_sql(os.path.join("business_connection", "shop_execution", "sync", "shop_execution.sql")),
                source_increase_sql=read_sql(os.path.join("business_connection", "shop_execution", "increase", "shop_execution_source.sql")),
                target_table="shop_execution",
                target_increase_sql=read_sql(os.path.join("business_connection", "shop_execution", "increase", "shop_execution_target.sql")),
            )
        ),
        extract_increase(
            connect_data,
            extract_increase_data(
                name="业联-车间执行单审核人数据抽取",
                logger_name="shop_execution_audit",
                source="金蝶云苍穹-正式库" ,
                source_sync_sql=read_sql(os.path.join("business_connection", "shop_execution_audit", "sync", "shop_execution_audit.sql")),
                source_increase_sql=read_sql(os.path.join("business_connection", "shop_execution_audit", "increase", "shop_execution_audit_source.sql")),
                target_table="shop_execution_audit",
                target_increase_sql=read_sql(os.path.join("business_connection", "shop_execution_audit", "increase", "shop_execution_audit_target.sql")),
            )
        ),
        extract_increase(
            connect_data,
            extract_increase_data(
                name="业联-车间执行单经办人数据抽取",
                logger_name="shop_execution_handle",
                source="金蝶云苍穹-正式库" ,
                source_sync_sql=read_sql(os.path.join("business_connection", "shop_execution_handle", "sync", "shop_execution_handle.sql")),
                source_increase_sql=read_sql(os.path.join("business_connection", "shop_execution_handle", "increase", "shop_execution_handle_source.sql")),
                target_table="shop_execution_handle",
                target_increase_sql=read_sql(os.path.join("business_connection", "shop_execution_handle", "increase", "shop_execution_handle_target.sql")),
            )
        ),
        extract_increase(
            connect_data,
            extract_increase_data(
                name="业联-车间执行单返工工艺数据抽取",
                logger_name="shop_execution_reworked_material",
                source="金蝶云苍穹-正式库" ,
                source_sync_sql=read_sql(os.path.join("business_connection", "shop_execution_reworked_material", "sync", "shop_execution_reworked_material.sql")),
                source_increase_sql=read_sql(os.path.join("business_connection", "shop_execution_reworked_material", "increase", "shop_execution_reworked_material_source.sql")),
                target_table="shop_execution_reworked_material",
                target_increase_sql=read_sql(os.path.join("business_connection", "shop_execution_reworked_material", "increase", "shop_execution_reworked_material_target.sql")),
            )
        ),
        extract_increase(
            connect_data,
            extract_increase_data(
                name="业联-车间执行单任务项点数据抽取",
                logger_name="shop_execution_task_item_point",
                source="金蝶云苍穹-正式库" ,
                source_sync_sql=read_sql(os.path.join("business_connection", "shop_execution_task_item_point", "sync", "shop_execution_task_item_point.sql")),
                source_increase_sql=read_sql(os.path.join("business_connection", "shop_execution_task_item_point", "increase", "shop_execution_task_item_point_source.sql")),
                target_table="shop_execution_task_item_point",
                target_increase_sql=read_sql(os.path.join("business_connection", "shop_execution_task_item_point", "increase", "shop_execution_task_item_point_target.sql")),
            )
        ),
        extract_increase(
            connect_data,
            extract_increase_data(
                name="业联-车间执行单备料工艺数据抽取",
                logger_name="shop_exeecution_material_preparation_technology",
                source="金蝶云苍穹-正式库" ,
                source_sync_sql=read_sql(os.path.join("business_connection", "shop_exeecution_material_preparation_technology", "sync", "shop_exeecution_material_preparation_technology.sql")),
                source_increase_sql=read_sql(os.path.join("business_connection", "shop_exeecution_material_preparation_technology", "increase", "shop_exeecution_material_preparation_technology_source.sql")),
                target_table="shop_exeecution_material_preparation_technology",
                target_increase_sql=read_sql(os.path.join("business_connection", "shop_exeecution_material_preparation_technology", "increase", "shop_exeecution_material_preparation_technology_target.sql")),
            )
        ),
        extract(
            connect_data,
            extract_data(
                name="业联-车间执行单抄送人分录数据抽取",
                logger_name="shop_execution_copy_delivery_unit",
                source="金蝶云苍穹-正式库" ,
                source_sql=read_sql(os.path.join("business_connection", "shop_execution_copy_delivery_unit.sql")),
                target_table="shop_execution_copy_delivery_unit"
            )
        ),
        extract(
            connect_data,
            extract_data(
                name="业联-车间执行单主送人分录数据抽取",
                logger_name="shop_execution_main_delivery_unit",
                source="金蝶云苍穹-正式库" ,
                source_sql=read_sql(os.path.join("business_connection", "shop_execution_main_delivery_unit.sql")),
                target_table="shop_execution_main_delivery_unit"
            )
        ),
        extract(
            connect_data,
            extract_data(
                name="业联-车间执行单返工工艺分录数据抽取",
                logger_name="shop_execution_reworked_material_unit",
                source="金蝶云苍穹-正式库" ,
                source_sql=read_sql(os.path.join("business_connection", "shop_execution_reworked_material_unit.sql")),
                target_table="shop_execution_reworked_material_unit"
            )
        ),
        extract(
            connect_data,
            extract_data(
                name="业联-车间执行单任务项点分录数据抽取",
                logger_name="shop_execution_task_item_point_unit",
                source="金蝶云苍穹-正式库" ,
                source_sql=read_sql(os.path.join("business_connection", "shop_execution_task_item_point_unit.sql")),
                target_table="shop_execution_task_item_point_unit"
            )
        ),
        extract(
            connect_data,
            extract_data(
                name="业联-车间执行单返工工艺班组分录数据抽取",
                logger_name="shop_exeecution_material_preparation_technology_unit_class",
                source="金蝶云苍穹-正式库" ,
                source_sql=read_sql(os.path.join("business_connection", "shop_exeecution_material_preparation_technology_unit_class.sql")),
                target_table="shop_exeecution_material_preparation_technology_unit_class"
            )
        ),
        extract(
            connect_data,
            extract_data(
                name="业联-车间执行单返工工艺处理分录数据抽取",
                logger_name="shop_exeecution_material_preparation_technology_unit_process",
                source="金蝶云苍穹-正式库" ,
                source_sql=read_sql(os.path.join("business_connection", "shop_exeecution_material_preparation_technology_unit_process.sql")),
                target_table="shop_exeecution_material_preparation_technology_unit_process"
            )
        )
    ]
