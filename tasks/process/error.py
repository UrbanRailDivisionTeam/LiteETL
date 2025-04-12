import datetime
import duckdb
from tasks.process import process
from utils.connect import connect_data


def judge_day(input_time: datetime.datetime, cursor: duckdb.DuckDBPyConnection, holiday_table_name: str) -> bool:
    '''判断当天是否是工作日'''
    res = cursor.sql(
        f'''
            SELECT bill."是否休息"
            FROM ods.{holiday_table_name} bill
            WHERE bill."节假日日期" = '{input_time.strftime('%Y-%m-%d %H:%M')}'
        '''
    ).fetchall()
    # 如果有额外的设定，就按照要求返回结果
    if len(res):
        return not bool(res[0])
    # 如果没有额外的节假日，就按照周一到周五上班返回
    return input_time.weekday() < 5


def judje_time(
    input_time: datetime.datetime,
    cursor: duckdb.DuckDBPyConnection,
    holiday_table_name: str,
    work_time_index: datetime.timedelta,
    request_time: datetime.timedelta,
    commuting_time: list[datetime.timedelta]
) -> tuple[datetime.datetime, datetime.timedelta]:
    '''
        计算响应时间
        如果当天内能响应完全，那么返回值中的时长回合输入的要求时长相同，返回的时间就是响应时间
        如果当天内无法完全响应，那么返回值中的时长就是当前已经响应的工作时长，返回的时间就是当天的结束

        input_time：输入的发起时间
        cursor：数据库连接
        holiday_table_name：节假日存储的表名
        work_time_index：当前已经响应的时间
        request_time：要求的响应时间
        commuting_time：工作日上下班时间
    '''
    # 输入的已经完成的不管，直接返回
    if work_time_index == request_time:
        return input_time, work_time_index
    if work_time_index > request_time:
        raise ValueError(f"计算错误，请检查程序和原始数据, {str(input_time)}, {str(work_time_index)}")
    # 如果当天不是工作日，递归到第二天凌晨起始
    if not judge_day(input_time, cursor, holiday_table_name):
        input_time += datetime.timedelta(days=1)
        input_time = input_time.replace(hour=0, minute=0, second=0, microsecond=0)
        return judje_time(input_time, cursor, holiday_table_name, work_time_index, request_time, commuting_time)
    today = datetime.datetime(year=input_time.year, month=input_time.month, day=input_time.day)
    am_start = today + commuting_time[0]
    am_end = today + commuting_time[1]
    pm_start = today + commuting_time[2]
    pm_end = today + commuting_time[3]
    # 在上午上班前发起的异常，递归到上午上班那一刻
    if input_time < am_start:
        input_time = input_time.replace(hour=am_start.hour, minute=am_start.minute, microsecond=0)
        return judje_time(input_time, cursor, holiday_table_name, work_time_index, request_time, commuting_time)
    # 在中午休息发起的异常，递归到下午上班那一刻
    if input_time >= am_end and input_time < pm_start:
        input_time = input_time.replace(hour=pm_start.hour, minute=pm_start.minute, microsecond=0)
        return judje_time(input_time, cursor, holiday_table_name, work_time_index, request_time, commuting_time)
    # 在晚上下班后发起的异常，递归到第二天凌晨起始
    if input_time > pm_end:
        input_time += datetime.timedelta(days=1)
        input_time = input_time.replace(hour=0, minute=0, second=0, microsecond=0)
        return judje_time(input_time, cursor, holiday_table_name, work_time_index, request_time, commuting_time)
    # 在上午上班时间发起的异常
    if input_time >= am_start and input_time < am_end:
        plan_respond = request_time - work_time_index
        real_time = am_end - am_start
        # 如果计划需要响应的时间大于上午上班时间
        if plan_respond > real_time:
            work_time_index += real_time
            input_time += real_time
        else:
            work_time_index += plan_respond
            input_time += plan_respond
        return judje_time(input_time, cursor, holiday_table_name, work_time_index, request_time, commuting_time)
    # 在下午午上班时间发起的异常
    if input_time >= pm_start and input_time < pm_end:
        plan_respond = request_time - work_time_index
        real_time = pm_end - pm_start
        # 如果计划需要响应的时间大于上午上班时间
        if plan_respond > real_time:
            work_time_index += real_time
            input_time += real_time
        else:
            work_time_index += plan_respond
            input_time += plan_respond
        return judje_time(input_time, cursor, holiday_table_name, work_time_index, request_time, commuting_time)
    raise ValueError(f"计算错误，程序不应当运行到这个位置，请检查程序和原始数据, {str(input_time)}, {str(work_time_index)}")


class alignment_error_process(process):
    def __init__(self, connect: connect_data) -> None:
        super().__init__(connect.duckdb, connect.mongo, "校线异常数据处理", "error_process")

    def task_main(self) -> None:
        temp0 = self.connect.sql(
            f'''
            SELECT
                COUNT(bill."id")
            FROM ods.alignment_error_handle bill
            WHERE bill.
            '''
        ).fetchall()