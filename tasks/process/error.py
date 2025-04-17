import datetime
import duckdb
import pandas as pd
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


class alignment_error_process(process):
    def __init__(self, connect: connect_data) -> None:
        super().__init__(connect.duckdb, connect.mongo, "校线异常数据处理", "error_process")

    def task_main(self) -> None:
        def process(input_time: datetime.datetime) -> datetime.datetime:
            real_time, _ = judje_time(
                input_time=input_time,
                cursor=self.connect,
                holiday_table_name='attendance_kq_scheduling_holiday',
                work_time_index=datetime.timedelta(),
                request_time=datetime.timedelta(hours=2),
                commuting_time=[
                    datetime.timedelta(hours=8, minutes=30),
                    datetime.timedelta(hours=12, minutes=00),
                    datetime.timedelta(hours=13, minutes=30),
                    datetime.timedelta(hours=17, minutes=30)
                ]
            )
            return real_time
        temp0 = self.connect.sql(
            f'''
            SELECT
                COUNT(bill."id")
            FROM ods.alignment_error_handle bill
            WHERE 
                
                AND bill."提报时间" >= CURRENT_DATE
                AND bill."提报时间"  < CURRENT_DATE + INTERVAL '1' DAY
            '''
        ).fetchall()
        temp1 = self.connect.sql(
            f'''
            SELECT
                COUNT(bill."id")
            FROM ods.alignment_error_handle bill
            WHERE 
                bill."提报时间" >= DATE_TRUNC('month', CURRENT_DATE)
                AND bill."提报时间" < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1' MONTH
            '''
        ).fetchall()
        temp2 = self.connect.sql(
            f'''
            SELECT
                COUNT(bill."id")
            FROM ods.alignment_error_handle bill
            WHERE bill."单据状态" = '待响应' OR bill."单据状态" = '转交中'
            '''
        ).fetchall()
        temp3 = self.connect.sql(
            f'''
            SELECT
                COUNT(bill."id")
            FROM ods.alignment_error_handle bill
            WHERE bill."单据状态" = '已响应'
            '''
        ).fetchall()
        total_process_handle: pd.DataFrame = self.connect.sql(f'''SELECT * FROM ods.alignment_error_handle''').fetchdf()
        total_process_initiate: pd.DataFrame = self.connect.sql(f'''SELECT * FROM ods.alignment_error_initiate''').fetchdf()
        total_process_handle["预计响应时间"] = total_process_handle.apply(lambda row: process(row["提报时间"]), axis=1)
        total_process_handle["是否及时响应"] = total_process_handle.apply(lambda row: row["响应时间"] >= row["预计响应时间"], axis=1)
        ...

        total_process_handle_res = total_process_handle[["项目名称",  "列车号", "校线节车号", "构型项名称", "现象分类", "现象描述", "响应人姓名"]]
        
        temp4 = self.connect.sql(
            f'''
            SELECT
                bill."所属班组",
                CAES 
                    WHEN COUNT(bill."id") = 0 
                    THEN 0
                    ELSE SUM(bill."是否及时响应") / COUNT(bill."id") 
                END AS "及时响应率"
            FROM total_process_handle_res bill
            GROUP BY bill."所属班组"
            '''
        ).fetchdf()
        temp5 = self.connect.sql(
            f'''
            SELECT
                bill."项目号",
                CAES 
                    WHEN COUNT(bill2."校线节车号") = 0 
                    THEN 0
                    ELSE COUNT(bill."id")  / COUNT(bill2."校线节车号")
                END AS "异常平均数统计"
            FROM total_process_handle_res bill
            LEFT JOIN (
                SELECT 
                    DISTINCT
                    bill."项目号",
                    bill."列车号",
                    bill."校线节车号"
                FROM total_process_handle_res bill
            ) bill2 on bill2."项目号" = bill."项目号"
            GROUP BY bill."项目号"
            '''
        ).fetchdf()