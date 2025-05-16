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
                name="人力资源需求分析-实际人员变动数据抽取",
                logger_name="actual_staffing_changes",
                source="金蝶云苍穹-测试库",
                source_sync_sql=read_sql(os.path.join("personnel_requirements", "actual_staffing_changes", "sync", "actual_staffing_changes.sql")),
                source_increase_sql=read_sql(os.path.join("personnel_requirements", "actual_staffing_changes", "increase", "actual_staffing_changes_source.sql")),
                target_table="actual_staffing_changes",
                target_increase_sql=read_sql(os.path.join("personnel_requirements", "actual_staffing_changes", "increase", "actual_staffing_changes_target.sql")),
            )
        ),
        extract_increase(
            connect_data,
            extract_increase_data(
                name="人力资源需求分析-实际人员变动分录数据抽取",
                logger_name="actual_staffing_changes_entry",
                source="金蝶云苍穹-测试库",
                source_sync_sql=read_sql(os.path.join("personnel_requirements", "actual_staffing_changes_entry", "sync", "actual_staffing_changes_entry.sql")),
                source_increase_sql=read_sql(os.path.join("personnel_requirements", "actual_staffing_changes_entry", "increase", "actual_staffing_changes_entry_source.sql")),
                target_table="actual_staffing_changes_entry",
                target_increase_sql=read_sql(os.path.join("personnel_requirements", "actual_staffing_changes_entry", "increase", "actual_staffing_changes_entry_target.sql")),
            )
        ),
        extract_increase(
            connect_data,
            extract_increase_data(
                name="人力资源需求分析-预估人员变动数据抽取",
                logger_name="projected_staffing_changes",
                source="金蝶云苍穹-测试库",
                source_sync_sql=read_sql(os.path.join("personnel_requirements", "projected_staffing_changes", "sync", "projected_staffing_changes.sql")),
                source_increase_sql=read_sql(os.path.join("personnel_requirements", "projected_staffing_changes", "increase", "projected_staffing_changes_source.sql")),
                target_table="projected_staffing_changes",
                target_increase_sql=read_sql(os.path.join("personnel_requirements", "projected_staffing_changes", "increase", "projected_staffing_changes_target.sql")),
            )
        ),
        extract_increase(
            connect_data,
            extract_increase_data(
                name="人力资源需求分析-预估人员变动分录数据抽取",
                logger_name="projected_staffing_changes_entry",
                source="金蝶云苍穹-测试库",
                source_sync_sql=read_sql(os.path.join("personnel_requirements", "projected_staffing_changes_entry", "sync", "projected_staffing_changes_entry.sql")),
                source_increase_sql=read_sql(os.path.join("personnel_requirements", "projected_staffing_changes_entry", "increase", "projected_staffing_changes_entry_source.sql")),
                target_table="projected_staffing_changes_entry",
                target_increase_sql=read_sql(os.path.join("personnel_requirements", "projected_staffing_changes_entry", "increase", "projected_staffing_changes_entry_target.sql")),
            )
        ),
        extract_increase(
            connect_data,
            extract_increase_data(
                name="人力资源需求分析-人员基础信息数据抽取",
                logger_name="personnel_basic_information",
                source="金蝶云苍穹-测试库",
                source_sync_sql=read_sql(os.path.join("personnel_requirements", "personnel_basic_information", "sync", "personnel_basic_information.sql")),
                source_increase_sql=read_sql(os.path.join("personnel_requirements", "personnel_basic_information", "increase", "personnel_basic_information_source.sql")),
                target_table="personnel_basic_information",
                target_increase_sql=read_sql(os.path.join("personnel_requirements", "personnel_basic_information", "increase", "personnel_basic_information_target.sql")),
            )
        ),
        extract_increase(
            connect_data,
            extract_increase_data(
                name="人力资源需求分析-人员所属班组数据抽取",
                logger_name="shift_to_which_the_person_belongs",
                source="金蝶云苍穹-测试库",
                source_sync_sql=read_sql(os.path.join("personnel_requirements", "shift_to_which_the_person_belongs", "sync", "shift_to_which_the_person_belongs.sql")),
                source_increase_sql=read_sql(os.path.join("personnel_requirements", "shift_to_which_the_person_belongs", "increase", "shift_to_which_the_person_belongs_source.sql")),
                target_table="shift_to_which_the_person_belongs",
                target_increase_sql=read_sql(os.path.join("personnel_requirements", "shift_to_which_the_person_belongs", "increase", "shift_to_which_the_person_belongs_target.sql")),
            )
        ),
    ]
