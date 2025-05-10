import datetime
import duckdb
import pandas as pd

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


def judge_on_time(expect_time: datetime.datetime, real_time: datetime.datetime) -> str | None:
    '''判断时间是否及时'''
    if expect_time is pd.NaT or expect_time is None:
        return None  # 输入数据不正确返回none
    now_date = datetime.datetime.now()
    if real_time is pd.NaT:
        if now_date <= expect_time:
            return '未处理'
    else:
        if real_time <= expect_time:
            return '及时'
    return '未及时'


def judje_time(
    input_time: datetime.datetime,
    cursor: duckdb.DuckDBPyConnection,
    holiday_table_name: str,
    work_time_index: datetime.timedelta,
    request_time: datetime.timedelta,
    commuting_time: list[datetime.timedelta]
) -> tuple[datetime.datetime, datetime.timedelta]:
    '''
        计算预计响应时间
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
    if input_time >= pm_end:
        input_time += datetime.timedelta(days=1)
        input_time = input_time.replace(hour=0, minute=0, second=0, microsecond=0)
        return judje_time(input_time, cursor, holiday_table_name, work_time_index, request_time, commuting_time)
    # 在上午上班时间发起的异常
    if input_time >= am_start and input_time < am_end:
        plan_respond = request_time - work_time_index
        real_time = am_end - max(am_start, input_time)
        temp_dur = min(real_time, plan_respond)
        work_time_index += temp_dur
        input_time += temp_dur
        return judje_time(input_time, cursor, holiday_table_name, work_time_index, request_time, commuting_time)
    # 在下午午上班时间发起的异常
    if input_time >= pm_start and input_time < pm_end:
        plan_respond = request_time - work_time_index
        real_time = pm_end - max(pm_start, input_time)
        temp_dur = min(real_time, plan_respond)
        work_time_index += temp_dur
        input_time += temp_dur
        return judje_time(input_time, cursor, holiday_table_name, work_time_index, request_time, commuting_time)
    raise ValueError(f"计算错误，程序不应当运行到这个位置，请检查程序和原始数据, {str(input_time)}, {str(work_time_index)}")


def judje_time_inverse(
    start_time: datetime.datetime,
    end_time: datetime.datetime,
    cursor: duckdb.DuckDBPyConnection,
    holiday_table_name: str,
    work_time_index: datetime.timedelta,
    commuting_time: list[datetime.timedelta]
) -> tuple[datetime.datetime, datetime.timedelta]:
    '''
        计算响应时长
        这里有个隐含条件，起始时间一定要比结束时间小，否则视为不正确的输入
        如果当天内能响应完全，那么返回值中的时长回合输入的要求时长相同，返回的时间就是响应时间
        如果当天内无法完全响应，那么返回值中的时长就是当前已经响应的工作时长，返回的时间就是当天的结束

        start_time：输入的发起时间
        end_time：输入的结束时间
        cursor：数据库连接
        holiday_table_name：节假日存储的表名
        work_time_index：当前已经响应的时间
        request_time：要求的响应时间
        commuting_time：工作日上下班时间
    '''
    # 输入的已经完成的不管，直接返回
    if start_time == end_time:
        return start_time, work_time_index
    if start_time >= end_time:
        raise ValueError(f"计算错误，请检查程序和原始数据, {str(start_time)}, {str(end_time)}")
    # 如果不在工作日
    if not judge_day(start_time, cursor, holiday_table_name):
        # 如果在同一天，则直接返回
        if start_time.date() == end_time.date():
            return start_time, work_time_index
        # 如果不在则跳过该天
        else:
            start_time += datetime.timedelta(days=1)
            start_time = start_time.replace(hour=0, minute=0, second=0, microsecond=0)
            return judje_time_inverse(start_time, end_time, cursor, holiday_table_name, work_time_index, commuting_time)
    else:
        today = datetime.datetime(year=start_time.year, month=start_time.month, day=start_time.day)
        am_start = today + commuting_time[0]
        am_end = today + commuting_time[1]
        pm_start = today + commuting_time[2]
        pm_end = today + commuting_time[3]
        # 如果起始时间在上午上班前
        if start_time < am_start:
            # 如果终止时间也在上午上班前，直接返回
            if end_time < am_start:
                return start_time, work_time_index
            # 如果终止时间在上午上班时间
            if end_time >= am_start and end_time < am_end:
                work_time_index += (end_time - am_start)
                return end_time, work_time_index
            # 如果终止时间在午休
            if end_time >= am_end and end_time < pm_start:
                work_time_index += (am_end - am_start)
                return end_time, work_time_index
            # 如果终止时间在下午上班时间
            if end_time >= pm_start and end_time < pm_end:
                work_time_index += ((am_end - am_start) + (end_time - pm_start))
                return end_time, work_time_index
            # 如果终止时间在下午上班时间之后
            if end_time >= pm_end:
                work_time_index += ((am_end - am_start) + (pm_end - pm_start))
                # 如果在同一天可以直接返回
                if start_time.date() == end_time.date():
                    return end_time, work_time_index
        # 如果起始时间在上午上班
        elif start_time >= am_start and start_time < am_end:
            # 如果终止时间在上午上班时间
            if end_time >= am_start and end_time < am_end:
                work_time_index += (end_time - start_time)
                return start_time, work_time_index
            # 如果终止时间在午休
            if end_time >= am_end and end_time < pm_start:
                work_time_index += (am_end - start_time)
                return end_time, work_time_index
            # 如果终止时间在下午上班时间
            if end_time >= pm_start and end_time < pm_end:
                work_time_index += ((am_end - start_time) + (end_time - pm_start))
                return end_time, work_time_index
            # 如果终止时间在下午上班时间之后
            if end_time >= pm_end:
                work_time_index += ((am_end - start_time) + (pm_end - pm_start))
                # 如果在同一天可以直接返回
                if start_time.date() == end_time.date():
                    return end_time, work_time_index
        # 如果起始时间在午休
        elif start_time >= am_end and start_time < pm_start:
            # 如果终止时间在午休
            if end_time >= am_end and end_time < pm_start:
                return start_time, work_time_index
            # 如果终止时间在下午上班时间
            if end_time >= pm_start and end_time < pm_end:
                work_time_index += (end_time - pm_start)
                return end_time, work_time_index
            # 如果终止时间在下午上班时间之后
            if end_time >= pm_end:
                work_time_index += (pm_end - pm_start)
                # 如果在同一天可以直接返回
                if start_time.date() == end_time.date():
                    return end_time, work_time_index
        # 如果起始时间在下午上班
        elif start_time >= pm_start and start_time < pm_end:
            # 如果终止时间在下午上班时间
            if end_time >= pm_start and end_time < pm_end:
                work_time_index += (end_time - start_time)
                return end_time, work_time_index
            # 如果终止时间在下午上班时间之后
            if end_time >= pm_end:
                work_time_index += (pm_end - start_time)
                # 如果在同一天可以直接返回
                if start_time.date() == end_time.date():
                    return end_time, work_time_index
        # 如果起始时间在下午上班之后
        elif start_time >= pm_end and end_time >= pm_end:
            # 如果在同一天可以直接返回
            if start_time.date() == end_time.date():
                return end_time, work_time_index
        else:
            raise ValueError(f"计算错误，程序不应当运行到这个位置，请检查程序和原始数据, {str(start_time)}, {str(work_time_index)}")
        # 跨天的递归到下一天
        start_time += datetime.timedelta(days=1)
        start_time = start_time.replace(hour=0, minute=0, second=0, microsecond=0)
        return judje_time_inverse(start_time, end_time, cursor, holiday_table_name, work_time_index, commuting_time)


def specialization_judje_time(connect: duckdb.DuckDBPyConnection, commuting_time: list[datetime.timedelta], input_time: datetime.datetime, request_time: datetime.timedelta) -> datetime.datetime | None:
    if input_time is pd.NaT:
        return None
    real_time, _ = judje_time(
        input_time=input_time,
        cursor=connect,
        holiday_table_name='attendance_kq_scheduling_holiday',
        work_time_index=datetime.timedelta(),
        request_time=request_time,
        commuting_time=commuting_time
    )
    return real_time

def specialization_judje_time_inverse(connect: duckdb.DuckDBPyConnection, commuting_time: list[datetime.timedelta], start_time: datetime.datetime, end_time: datetime.datetime) -> datetime.timedelta | None:
    if start_time is pd.NaT or end_time is pd.NaT:
        return None
    _, work_time_index = judje_time_inverse(
        start_time=start_time,
        end_time=end_time,
        cursor=connect,
        holiday_table_name='attendance_kq_scheduling_holiday',
        work_time_index=datetime.timedelta(),
        commuting_time=commuting_time
    )
    return work_time_index