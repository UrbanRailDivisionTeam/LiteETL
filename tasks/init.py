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
    approval_flow,
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
    temp_res = [
        ameliorate.init(connect_data),
        attendance.init(connect_data),
        business_connection.init(connect_data),
        error.init(connect_data),
        # interested_party.init(connect_data),
        person.init(connect_data),
        wire_number.init(connect_data),
        work_time.init(connect_data),
        approval_flow.init(connect_data),
    ]
    # 检查任务是否符合规范
    label, label_str = check(temp_res)
    if not label:
        raise ValueError(label_str)

    # 将多个来源的初始化任务合并
    gp = {k: v for d in temp_res for k, v in d.items()}
    
    # 用于任务类相关关系定义
    gp["全员型改善数据分析"].dp(gp["改善数据抽取"]).dp(gp["人员基础数据抽取"])
    gp["线号标签申请数据处理"].dp(gp["线号标签申请上下标数据抽取"]).dp(gp["线号标签申请位置号数据抽取"])
    # gp["相关方数据处理"].dp(gp["相关方安全数据抽取"]).dp(gp["相关方安全随行人员数据抽取"])
    gp["校线异常数据处理"]\
        .dp(gp["校线异常发起单数据抽取"])\
        .dp(gp["校线异常处理单数据抽取"])\
        .dp(gp["流程操作日志数据抽取"])\
        .dp(gp["流程实例数据抽取"])\
        .dp(gp["历史流程实例数据抽取"])\
        .dp(gp["单据数据模型抽取"])\
        .dp(gp["考勤节假日数据抽取"])\
        .dp(gp["人员基础数据抽取"])
    return list(gp.values())
