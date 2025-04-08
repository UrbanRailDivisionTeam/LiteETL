import os
from utils.config import CONFIG, DEBUG
from utils.connect import connect_data
from tasks.base import task, extract_increase, extract_increase_data, extract, extract_data, sync_increase, sync_increase_data
from tasks.process.ameliorate import ameliorate
from tasks.process.wire_number import wire_number


def read_sql(sql_path: str) -> str:
    with open(os.path.abspath(os.path.join(CONFIG.SELECT_PATH, sql_path)), mode="r", encoding="utf-8") as file:
        sql_str = file.read()
    return sql_str


def trans_table_to_sql(table_name: str, schema_name: str | None = None) -> str:
    if schema_name is None:
        return f""" SELECT * FROM `{table_name}`"""
    else:
        return f""" SELECT * FROM `{schema_name}`.`{table_name}`"""
    

def task_init(connect_data: connect_data) -> list[task]:
    '''任务进行初始化，并添加依赖关系的地方'''
    task_e0 = extract_increase(
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
    )
    task_e1 = extract_increase(
        connect_data,
        extract_increase_data(
            name="人员基础数据抽取",
            logger_name="person",
            source="SHR" if not DEBUG else "oracle服务",
            source_sync_sql=read_sql(os.path.join("person", "sync", "person.sql")),
            source_increase_sql=read_sql(os.path.join("person", "increase", "person_source.sql")),
            target_table="person",
            target_increase_sql=read_sql(os.path.join("person", "increase", "person_target.sql")),
        )
    )
    task_e2 = extract_increase(
        connect_data,
        extract_increase_data(
            name="相关方安全数据抽取",
            logger_name="interested_party",
            source="相关方数据库" if not DEBUG else "pgsql服务",
            source_sync_sql=read_sql(os.path.join("interested_party", "interested_party", "sync", "interested_party.sql")),
            source_increase_sql=read_sql(os.path.join("interested_party", "interested_party", "increase", "interested_party_source.sql")),
            target_table="interested_party",
            target_increase_sql=read_sql(os.path.join("interested_party", "interested_party", "increase", "interested_party_target.sql")),
        )
    )
    task_e3 = extract_increase(
        connect_data,
        extract_increase_data(
            name="相关方安全随行人员数据抽取",
            logger_name="interested_party_entry",
            source="相关方数据库" if not DEBUG else "pgsql服务",
            source_sync_sql=read_sql(os.path.join("interested_party", "interested_party_entry", "sync", "interested_party_entry.sql")),
            source_increase_sql=read_sql(os.path.join("interested_party", "interested_party_entry", "increase", "interested_party_entry_source.sql")),
            target_table="interested_party_entry",
            target_increase_sql=read_sql(os.path.join("interested_party", "interested_party_entry", "increase", "interested_party_entry_target.sql")),
        )
    )
    task_e4 = extract_increase(
        connect_data,
        extract_increase_data(
            name="业联-业务联系书数据抽取",
            logger_name="business_connection",
            source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
            source_sync_sql=read_sql(os.path.join("business_connection", "business_connection", "sync", "business_connection.sql")),
            source_increase_sql=read_sql(os.path.join("business_connection", "business_connection", "increase", "business_connection_source.sql")),
            target_table="business_connection",
            target_increase_sql=read_sql(os.path.join("business_connection", "business_connection", "increase", "business_connection_target.sql")),
        )
    )
    task_e5 = extract(
        connect_data,
        extract_data(
            name="业联-业务联系书主送人数据抽取",
            logger_name="business_connection_main_delivery_unit",
            source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
            source_sql=read_sql(os.path.join("business_connection", "business_connection_main_delivery_unit.sql")),
            target_table="business_connection_main_delivery_unit"
        )
    )
    task_e6 = extract(
        connect_data,
        extract_data(
            name="业联-业务联系书抄送人数据抽取",
            logger_name="business_connection_copy_delivery_unit",
            source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
            source_sql=read_sql(os.path.join("business_connection", "business_connection_copy_delivery_unit.sql")),
            target_table="business_connection_copy_delivery_unit"
        )
    )
    task_e7 = extract_increase(
        connect_data,
        extract_increase_data(
            name="业联-业联执行关闭数据抽取",
            logger_name="business_connection_close",
            source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
            source_sync_sql=read_sql(os.path.join("business_connection", "business_connection_close", "sync", "business_connection_close.sql")),
            source_increase_sql=read_sql(os.path.join("business_connection", "business_connection_close", "increase", "business_connection_close_source.sql")),
            target_table="business_connection_close",
            target_increase_sql=read_sql(os.path.join("business_connection", "business_connection_close", "increase", "business_connection_close_target.sql")),
        )
    )
    task_e8 = extract_increase(
        connect_data,
        extract_increase_data(
            name="业联-班组基础资料数据抽取",
            logger_name="class_group",
            source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
            source_sync_sql=read_sql(os.path.join("business_connection", "class_group", "sync", "class_group.sql")),
            source_increase_sql=read_sql(os.path.join("business_connection", "class_group", "increase", "class_group_source.sql")),
            target_table="class_group",
            target_increase_sql=read_sql(os.path.join("business_connection", "class_group", "increase", "class_group_target.sql")),
        )
    )
    task_e9 = extract_increase(
        connect_data,
        extract_increase_data(
            name="业联-班组基础资料分录数据抽取",
            logger_name="class_group_entry",
            source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
            source_sync_sql=read_sql(os.path.join("business_connection", "class_group_entry", "sync", "class_group_entry.sql")),
            source_increase_sql=read_sql(os.path.join("business_connection", "class_group_entry", "increase", "class_group_entry_source.sql")),
            target_table="class_group_entry",
            target_increase_sql=read_sql(os.path.join("business_connection", "class_group_entry", "increase", "class_group_entry_target.sql")),
        )
    )
    task_e10 = extract_increase(
        connect_data,
        extract_increase_data(
            name="业联-设计变更单数据抽取",
            logger_name="design_change",
            source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
            source_sync_sql=read_sql(os.path.join("business_connection", "design_change", "sync", "design_change.sql")),
            source_increase_sql=read_sql(os.path.join("business_connection", "design_change", "increase", "design_change_source.sql")),
            target_table="design_change",
            target_increase_sql=read_sql(os.path.join("business_connection", "design_change", "increase", "design_change_target.sql")),
        )
    )
    task_e11 = extract_increase(
        connect_data,
        extract_increase_data(
            name="业联-设计变更单分录数据抽取",
            logger_name="design_change_entry",
            source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
            source_sync_sql=read_sql(os.path.join("business_connection", "design_change_entry", "sync", "design_change_entry.sql")),
            source_increase_sql=read_sql(os.path.join("business_connection", "design_change_entry", "increase", "design_change_entry_source.sql")),
            target_table="design_change_entry",
            target_increase_sql=read_sql(os.path.join("business_connection", "design_change_entry", "increase", "design_change_entry_target.sql")),
        )
    )
    task_e12 = extract_increase(
        connect_data,
        extract_increase_data(
            name="业联-设计变更执行数据抽取",
            logger_name="design_change_execution",
            source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
            source_sync_sql=read_sql(os.path.join("business_connection", "design_change_execution", "sync", "design_change_execution.sql")),
            source_increase_sql=read_sql(os.path.join("business_connection", "design_change_execution", "increase", "design_change_execution_source.sql")),
            target_table="design_change_execution",
            target_increase_sql=read_sql(os.path.join("business_connection", "design_change_execution", "increase", "design_change_execution_target.sql")),
        )
    )
    task_e13 = extract_increase(
        connect_data,
        extract_increase_data(
            name="业联-设计变更执行审核数据抽取",
            logger_name="design_change_execution_audit",
            source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
            source_sync_sql=read_sql(os.path.join("business_connection", "design_change_execution_audit", "sync", "design_change_execution_audit.sql")),
            source_increase_sql=read_sql(os.path.join("business_connection", "design_change_execution_audit", "increase", "design_change_execution_audit_source.sql")),
            target_table="design_change_execution_audit",
            target_increase_sql=read_sql(os.path.join("business_connection", "design_change_execution_audit", "increase", "design_change_execution_audit_target.sql")),
        )
    )
    task_e15 = extract_increase(
        connect_data,
        extract_increase_data(
            name="业联-设计变更执行变更内容数据抽取",
            logger_name="design_change_execution_change_content",
            source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
            source_sync_sql=read_sql(os.path.join("business_connection", "design_change_execution_change_content", "sync", "design_change_execution_change_content.sql")),
            source_increase_sql=read_sql(os.path.join("business_connection", "design_change_execution_change_content", "increase", "design_change_execution_change_content_source.sql")),
            target_table="design_change_execution_change_content",
            target_increase_sql=read_sql(os.path.join("business_connection", "design_change_execution_change_content", "increase", "design_change_execution_change_content_target.sql")),
        )
    )
    task_e16 = extract_increase(
        connect_data,
        extract_increase_data(
            name="业联-设计变更执行文档变更数据抽取",
            logger_name="design_change_execution_document_change",
            source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
            source_sync_sql=read_sql(os.path.join("business_connection", "design_change_execution_document_change", "sync", "design_change_execution_document_change.sql")),
            source_increase_sql=read_sql(os.path.join("business_connection", "design_change_execution_document_change", "increase", "design_change_execution_document_change_source.sql")),
            target_table="design_change_execution_document_change",
            target_increase_sql=read_sql(os.path.join("business_connection", "design_change_execution_document_change", "increase", "design_change_execution_document_change_target.sql")),
        )
    )
    task_e17 = extract_increase(
        connect_data,
        extract_increase_data(
            name="业联-设计变更执行经办人数据抽取",
            logger_name="design_change_execution_handle",
            source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
            source_sync_sql=read_sql(os.path.join("business_connection", "design_change_execution_handle", "sync", "design_change_execution_handle.sql")),
            source_increase_sql=read_sql(os.path.join("business_connection", "design_change_execution_handle", "increase", "design_change_execution_handle_source.sql")),
            target_table="design_change_execution_handle",
            target_increase_sql=read_sql(os.path.join("business_connection", "design_change_execution_handle", "increase", "design_change_execution_handle_target.sql")),
        )
    )
    task_e18 = extract_increase(
        connect_data,
        extract_increase_data(
            name="业联-设计变更执行物料变更数据抽取",
            logger_name="design_change_execution_material_change",
            source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
            source_sync_sql=read_sql(os.path.join("business_connection", "design_change_execution_material_change", "sync", "design_change_execution_material_change.sql")),
            source_increase_sql=read_sql(os.path.join("business_connection", "design_change_execution_material_change", "increase", "design_change_execution_material_change_source.sql")),
            target_table="design_change_execution_material_change",
            target_increase_sql=read_sql(os.path.join("business_connection", "design_change_execution_material_change", "increase", "design_change_execution_material_change_target.sql")),
        )
    )
    task_e19 = extract_increase(
        connect_data,
        extract_increase_data(
            name="业联-设计变更执行备料工艺数据抽取",
            logger_name="design_change_execution_material_preparation_technology",
            source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
            source_sync_sql=read_sql(os.path.join("business_connection", "design_change_execution_material_preparation_technology", "sync", "design_change_execution_material_preparation_technology.sql")),
            source_increase_sql=read_sql(os.path.join("business_connection", "design_change_execution_material_preparation_technology", "increase", "design_change_execution_material_preparation_technology_source.sql")),
            target_table="design_change_execution_material_preparation_technology",
            target_increase_sql=read_sql(os.path.join("business_connection", "design_change_execution_material_preparation_technology", "increase", "design_change_execution_material_preparation_technology_target.sql")),
        )
    )
    task_e20 = extract_increase(
        connect_data,
        extract_increase_data(
            name="业联-设计变更执行返工工艺数据抽取",
            logger_name="design_change_execution_reworked_material",
            source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
            source_sync_sql=read_sql(os.path.join("business_connection", "design_change_execution_reworked_material", "sync", "design_change_execution_reworked_material.sql")),
            source_increase_sql=read_sql(os.path.join("business_connection", "design_change_execution_reworked_material", "increase", "design_change_execution_reworked_material_source.sql")),
            target_table="design_change_execution_reworked_material",
            target_increase_sql=read_sql(os.path.join("business_connection", "design_change_execution_reworked_material", "increase", "design_change_execution_reworked_material_target.sql")),
        )
    )
    task_e21 = extract(
        connect_data,
        extract_data(
            name="业联-设计变更执行抄送人数据抽取",
            logger_name="design_change_execution_copy_delivery_unit",
            source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
            source_sql=read_sql(os.path.join("business_connection", "design_change_execution_copy_delivery_unit.sql")),
            target_table="design_change_execution_copy_delivery_unit"
        )
    )
    task_e22 = extract(
        connect_data,
        extract_data(
            name="业联-设计变更执行文档变更分录数据抽取",
            logger_name="design_change_execution_document_change_unit",
            source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
            source_sql=read_sql(os.path.join("business_connection", "design_change_execution_document_change_unit.sql")),
            target_table="design_change_execution_document_change_unit"
        )
    )
    task_e23 = extract(
        connect_data,
        extract_data(
            name="业联-设计变更执行主送人数据抽取",
            logger_name="design_change_execution_main_delivery_unit",
            source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
            source_sql=read_sql(os.path.join("business_connection", "design_change_execution_main_delivery_unit.sql")),
            target_table="design_change_execution_main_delivery_unit"
        )
    )
    task_e24 = extract(
        connect_data,
        extract_data(
            name="业联-设计变更执行备料工艺分录数据抽取",
            logger_name="design_change_execution_material_preparation_technology_unit",
            source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
            source_sql=read_sql(os.path.join("business_connection", "design_change_execution_material_preparation_technology_unit.sql")),
            target_table="design_change_execution_material_preparation_technology_unit"
        )
    )
    task_e14 = extract(
        connect_data,
        extract_data(
            name="业联-设计变更执行返工工艺分录数据抽取",
            logger_name="design_change_execution_reworked_material_unit",
            source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
            source_sql=read_sql(os.path.join("business_connection", "design_change_execution_reworked_material_unit.sql")),
            target_table="design_change_execution_reworked_material_unit"
        )
    )
    task_e25 = extract_increase(
        connect_data,
        extract_increase_data(
            name="业联-工艺流程数据抽取",
            logger_name="technological_process",
            source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
            source_sync_sql=read_sql(os.path.join("business_connection", "technological_process", "sync", "technological_process.sql")),
            source_increase_sql=read_sql(os.path.join("business_connection", "technological_process", "increase", "technological_process_source.sql")),
            target_table="technological_process",
            target_increase_sql=read_sql(os.path.join("business_connection", "technological_process", "increase", "technological_process_target.sql")),
        )
    )
    task_e26 = extract_increase(
        connect_data,
        extract_increase_data(
            name="业联-工艺流程工艺变更数据抽取",
            logger_name="technological_process_change",
            source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
            source_sync_sql=read_sql(os.path.join("business_connection", "technological_process_change", "sync", "technological_process_change.sql")),
            source_increase_sql=read_sql(os.path.join("business_connection", "technological_process_change", "increase", "technological_process_change_source.sql")),
            target_table="technological_process_change",
            target_increase_sql=read_sql(os.path.join("business_connection", "technological_process_change", "increase", "technological_process_change_target.sql")),
        )
    )
    task_e27 = extract_increase(
        connect_data,
        extract_increase_data(
            name="业联-工艺流程任务流程数据抽取",
            logger_name="technological_process_flow",
            source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
            source_sync_sql=read_sql(os.path.join("business_connection", "technological_process_flow", "sync", "technological_process_flow.sql")),
            source_increase_sql=read_sql(os.path.join("business_connection", "technological_process_flow", "increase", "technological_process_flow_source.sql")),
            target_table="technological_process_flow",
            target_increase_sql=read_sql(os.path.join("business_connection", "technological_process_flow", "increase", "technological_process_flow_target.sql")),
        )
    )
    task_e28 = extract_increase(
        connect_data,
        extract_increase_data(
            name="业联-车间执行单数据抽取",
            logger_name="shop_execution",
            source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
            source_sync_sql=read_sql(os.path.join("business_connection", "shop_execution", "sync", "shop_execution.sql")),
            source_increase_sql=read_sql(os.path.join("business_connection", "shop_execution", "increase", "shop_execution_source.sql")),
            target_table="shop_execution",
            target_increase_sql=read_sql(os.path.join("business_connection", "shop_execution", "increase", "shop_execution_target.sql")),
        )
    )
    task_e29 = extract_increase(
        connect_data,
        extract_increase_data(
            name="业联-车间执行单审核人数据抽取",
            logger_name="shop_execution_audit",
            source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
            source_sync_sql=read_sql(os.path.join("business_connection", "shop_execution_audit", "sync", "shop_execution_audit.sql")),
            source_increase_sql=read_sql(os.path.join("business_connection", "shop_execution_audit", "increase", "shop_execution_audit_source.sql")),
            target_table="shop_execution_audit",
            target_increase_sql=read_sql(os.path.join("business_connection", "shop_execution_audit", "increase", "shop_execution_audit_target.sql")),
        )
    )
    task_e30 = extract_increase(
        connect_data,
        extract_increase_data(
            name="业联-车间执行单经办人数据抽取",
            logger_name="shop_execution_handle",
            source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
            source_sync_sql=read_sql(os.path.join("business_connection", "shop_execution_handle", "sync", "shop_execution_handle.sql")),
            source_increase_sql=read_sql(os.path.join("business_connection", "shop_execution_handle", "increase", "shop_execution_handle_source.sql")),
            target_table="shop_execution_handle",
            target_increase_sql=read_sql(os.path.join("business_connection", "shop_execution_handle", "increase", "shop_execution_handle_target.sql")),
        )
    )
    task_e31 = extract_increase(
        connect_data,
        extract_increase_data(
            name="业联-车间执行单返工工艺数据抽取",
            logger_name="shop_execution_reworked_material",
            source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
            source_sync_sql=read_sql(os.path.join("business_connection", "shop_execution_reworked_material", "sync", "shop_execution_reworked_material.sql")),
            source_increase_sql=read_sql(os.path.join("business_connection", "shop_execution_reworked_material", "increase", "shop_execution_reworked_material_source.sql")),
            target_table="shop_execution_reworked_material",
            target_increase_sql=read_sql(os.path.join("business_connection", "shop_execution_reworked_material", "increase", "shop_execution_reworked_material_target.sql")),
        )
    )
    task_e32 = extract_increase(
        connect_data,
        extract_increase_data(
            name="业联-车间执行单任务项点数据抽取",
            logger_name="shop_execution_task_item_point",
            source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
            source_sync_sql=read_sql(os.path.join("business_connection", "shop_execution_task_item_point", "sync", "shop_execution_task_item_point.sql")),
            source_increase_sql=read_sql(os.path.join("business_connection", "shop_execution_task_item_point", "increase", "shop_execution_task_item_point_source.sql")),
            target_table="shop_execution_task_item_point",
            target_increase_sql=read_sql(os.path.join("business_connection", "shop_execution_task_item_point", "increase", "shop_execution_task_item_point_target.sql")),
        )
    )
    task_e33 = extract_increase(
        connect_data,
        extract_increase_data(
            name="业联-车间执行单备料工艺数据抽取",
            logger_name="shop_exeecution_material_preparation_technology",
            source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
            source_sync_sql=read_sql(os.path.join("business_connection", "shop_exeecution_material_preparation_technology", "sync", "shop_exeecution_material_preparation_technology.sql")),
            source_increase_sql=read_sql(os.path.join("business_connection", "shop_exeecution_material_preparation_technology", "increase", "shop_exeecution_material_preparation_technology_source.sql")),
            target_table="shop_exeecution_material_preparation_technology",
            target_increase_sql=read_sql(os.path.join("business_connection", "shop_exeecution_material_preparation_technology", "increase", "shop_exeecution_material_preparation_technology_target.sql")),
        )
    )
    task_e34 = extract(
        connect_data,
        extract_data(
            name="业联-车间执行单抄送人分录数据抽取",
            logger_name="shop_execution_copy_delivery_unit",
            source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
            source_sql=read_sql(os.path.join("business_connection", "shop_execution_copy_delivery_unit.sql")),
            target_table="shop_execution_copy_delivery_unit"
        )
    )
    task_e35 = extract(
        connect_data,
        extract_data(
            name="业联-车间执行单主送人分录数据抽取",
            logger_name="shop_execution_main_delivery_unit",
            source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
            source_sql=read_sql(os.path.join("business_connection", "shop_execution_main_delivery_unit.sql")),
            target_table="shop_execution_main_delivery_unit"
        )
    )
    task_e36 = extract(
        connect_data,
        extract_data(
            name="业联-车间执行单返工工艺分录数据抽取",
            logger_name="shop_execution_reworked_material_unit",
            source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
            source_sql=read_sql(os.path.join("business_connection", "shop_execution_reworked_material_unit.sql")),
            target_table="shop_execution_reworked_material_unit"
        )
    )
    task_e37 = extract(
        connect_data,
        extract_data(
            name="业联-车间执行单任务项点分录数据抽取",
            logger_name="shop_execution_task_item_point_unit",
            source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
            source_sql=read_sql(os.path.join("business_connection", "shop_execution_task_item_point_unit.sql")),
            target_table="shop_execution_task_item_point_unit"
        )
    )
    task_e38 = extract(
        connect_data,
        extract_data(
            name="业联-车间执行单返工工艺班组分录数据抽取",
            logger_name="shop_exeecution_material_preparation_technology_unit_class",
            source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
            source_sql=read_sql(os.path.join("business_connection", "shop_exeecution_material_preparation_technology_unit_class.sql")),
            target_table="shop_exeecution_material_preparation_technology_unit_class"
        )
    )
    task_e39 = extract(
        connect_data,
        extract_data(
            name="业联-车间执行单返工工艺处理分录数据抽取",
            logger_name="shop_exeecution_material_preparation_technology_unit_process",
            source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
            source_sql=read_sql(os.path.join("business_connection", "shop_exeecution_material_preparation_technology_unit_process.sql")),
            target_table="shop_exeecution_material_preparation_technology_unit_process"
        )
    )
    task_e40 = extract_increase(
        connect_data,
        extract_increase_data(
            name="线上工时数据抽取",
            logger_name="online_worktime",
            source="EAS" if not DEBUG else "oracle服务",
            source_sync_sql=read_sql(os.path.join("worktime", "online_worktime", "sync", "online_worktime.sql")),
            source_increase_sql=read_sql(os.path.join("worktime", "online_worktime", "increase", "online_worktime_source.sql")),
            target_table="online_worktime",
            target_increase_sql=read_sql(os.path.join("worktime", "online_worktime", "increase", "online_worktime_target.sql")),
            is_del=False
        )
    )
    task_e41 = extract_increase(
        connect_data,
        extract_increase_data(
            name="临时工时数据抽取",
            logger_name="temp_worktime",
            source="生产辅助系统-城轨" if not DEBUG else "sqlserver服务",
            source_sync_sql=read_sql(os.path.join("worktime", "temp_worktime", "sync", "temp_worktime.sql")),
            source_increase_sql=read_sql(os.path.join("worktime", "temp_worktime", "increase", "temp_worktime_source.sql")),
            target_table="temp_worktime",
            target_increase_sql=read_sql(os.path.join("worktime", "temp_worktime", "increase", "temp_worktime_target.sql")),
        )
    )
    task_e42 = extract_increase(
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
    )
    task_e43 = extract_increase(
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
    )
    task_e44 = sync_increase(
        connect_data,
        sync_increase_data(
            name="相关方审批数据同步",
            logger_name="interested_party_review",
            source="金蝶云苍穹-正式库" if not DEBUG else "mysql服务",
            source_sync_sql=read_sql(os.path.join("interested_party", "interested_party_review", "sync", "interested_party_review.sql")),
            source_increase_sql=read_sql(os.path.join("interested_party", "interested_party_review", "increase", "interested_party_review_source.sql")),
            target="数据运用平台-测试库" if not DEBUG else "mysql服务",
            target_db="zj_data",
            target_table="interested_party_review",
            target_increase_sql=read_sql(os.path.join("interested_party", "interested_party_review", "increase", "interested_party_review_target.sql")),
        )
    )
    
    
    task_p0 = ameliorate(connect_data)
    task_p0.dp(task_e0).dp(task_e1)
    
    task_p1 = wire_number(connect_data)
    task_p1.dp(task_e42).dp(task_e43)
    
    return [
        task_e0,
        task_e1,
        task_e2,
        task_e3,
        task_e4,
        task_e5,
        task_e6,
        task_e7,
        task_e8,
        task_e9,
        task_e10,
        task_e11,
        task_e12,
        task_e13,
        task_e14,
        task_e15,
        task_e16,
        task_e17,
        task_e18,
        task_e19,
        task_e20,
        task_e21,
        task_e22,
        task_e23,
        task_e24,
        task_e25,
        task_e26,
        task_e27,
        task_e28,
        task_e29,
        task_e30,
        task_e31,
        task_e32,
        task_e33,
        task_e34,
        task_e35,
        task_e36,
        task_e37,
        task_e38,
        task_e39,
        task_e40,
        task_e41,
        task_e42,
        task_e43,
        task_e44,
        
        task_p0,
        task_p1
    ]
