from utils.connect import connect_data
from tasks.base import task, extract_increase, extract
from tasks.sync import (
    ameliorate,
    attendance, 
    business_connection, 
    error,
    interested_party, 
    person,
    wire_number,  
    work_time,
)


def check(task_list: list[dict[str, task]]) -> tuple[bool, str]:
    # 检查对应的任务是否符合要求
    temp_name = set()
    temp_target_table = set()
    for tasks_ in task_list:
        for task_name in tasks_:
            # 检查名称，防止日志出错
            if task_name not in temp_name:
                temp_name.add(task_name)
            else:
                return False, f"有重复的任务名称{task_name}，请检查任务定义信息"
            # 对所有抽取类型的任务检查目标表是否重复
            if isinstance(task, extract_increase) or isinstance(task, extract):
                if task.data.target_table not in temp_target_table:
                    temp_target_table.add(task.data.target_table)
                else:
                    return False, f"对于抽取任务,有重复的目标表名称{task.data.target_table}，请检查任务定义信息"
    return True, ""
    

def task_init(connect_data: connect_data) -> list[task]:
    # 用于任务类的初始化
    ameliorate_group = ameliorate.init(connect_data)
    attendance_group = attendance.init(connect_data)
    business_connection_group = business_connection.init(connect_data)
    error_group = error.init(connect_data)
    interested_party_group = interested_party.init(connect_data)
    person_group = person.init(connect_data)
    wire_number_group = wire_number.init(connect_data)
    work_time_group = work_time.init(connect_data)

    # 检查任务是否符合规范
    temp_res = [
        ameliorate_group, 
        attendance_group,
        business_connection_group,
        error_group,
        interested_party_group,
        person_group,
        wire_number_group,
        work_time_group,
    ]
    label, label_str = check(temp_res)
    if not label:
        raise ValueError(label_str)

    # 将多个来源的初始化任务合并
    gp = {
        **ameliorate_group, 
        **attendance_group,
        **business_connection_group,
        **error_group,
        **interested_party_group,
        **person_group,
        **wire_number_group,
        **work_time_group,
    }
    
    # 用于任务类相关关系定义
    gp["全员型改善数据分析"].dp(gp["改善数据抽取"]).dp(gp["人员基础数据抽取"])
    gp["线号标签申请数据处理"].dp(gp["线号标签申请上下标数据抽取"]).dp(gp["线号标签申请位置号数据抽取"])
    # gp["校线异常数据处理"].dp(gp["校线异常处理单数据抽取"]).dp(gp["校线异常发起单数据抽取"])

    return list(gp.values())
        
