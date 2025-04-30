import datetime
import duckdb
import pandas as pd
from typing import Any
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


class alignment_error_process(process):
    def __init__(self, connect: connect_data) -> None:
        super().__init__(connect.duckdb, connect.mongo, "校线异常数据处理", "error_process")
        # 流程及格标准
        self.on_time_norms = 0.8
        # 校线异常发起单
        self.request_form_name = "crrc_unqualify"
        self.request_flow_name = "Proc_crrc_unqualify_audit_7"
        # 校线异常处理单
        self.deal_form_name = "crrc_unqualifydeal"
        self.deal_flow_name = "Proc_crrc_unqualifydeal_audit_7"
        # 各个关键环节的要求响应时长与流程总时长
        self.request_time = {
            "响应": datetime.timedelta(hours=2),
            "一次诊断": datetime.timedelta(hours=2),
            "二次诊断": datetime.timedelta(hours=2),
            "返工": datetime.timedelta(hours=2),
            "验收": datetime.timedelta(hours=2),
        }
        commuting_time = [
            datetime.timedelta(hours=8, minutes=30),
            datetime.timedelta(hours=12, minutes=00),
            datetime.timedelta(hours=13, minutes=30),
            datetime.timedelta(hours=17, minutes=30)
        ]

        def __judje_time(input_time: datetime.datetime, request_time: datetime.timedelta) -> datetime.datetime | None:
            if input_time is pd.NaT:
                return None
            real_time, _ = judje_time(
                input_time=input_time,
                cursor=self.connect,
                holiday_table_name='attendance_kq_scheduling_holiday',
                work_time_index=datetime.timedelta(),
                request_time=request_time,
                commuting_time=commuting_time
            )
            return real_time

        def __judje_time_inverse(start_time: datetime.datetime, end_time: datetime.datetime) -> datetime.timedelta | None:
            if start_time is pd.NaT or end_time is pd.NaT:
                return None
            _, work_time_index = judje_time_inverse(
                start_time=start_time,
                end_time=end_time,
                cursor=self.connect,
                holiday_table_name='attendance_kq_scheduling_holiday',
                work_time_index=datetime.timedelta(),
                commuting_time=commuting_time
            )
            return work_time_index
        self.judje_time_func = __judje_time
        self.judje_time_inverse_func = __judje_time_inverse

    def task_main(self) -> None:
        # 申请单及时相关计算
        flow_operation_handle: pd.DataFrame = self.connect.sql(
            f'''
                WITH flow_object AS (
                        SELECT 
                            bill."id",
                            bill."单据编码",
                            bill."流程实例ID",
                            bill."流程标识",
                        FROM ods.af_historical_flow bill
                        LEFT JOIN ods.alignment_error_handle aleh ON aleh."单据编号" = bill."单据编码"
                        WHERE 
                            bill."实体编码" = '{self.request_form_name}' 
                            AND bill."流程标识" = '{self.request_flow_name}'
                            AND aleh."单据状态" <> '暂存'
                        UNION
                        SELECT 
                            bill."id",
                            bill."单据编码",
                            bill."流程实例ID",
                            t_bill."流程标识"
                        FROM ods.af_current_flow bill
                        LEFT JOIN ods.alignment_error_handle aleh ON aleh."单据编号" = bill."单据编码"
                        LEFT JOIN (
                            SELECT
                                bill."id",
                                bill."流程标识",
                            FROM ods.af_current_flow bill
                            WHERE 
                                bill."流程标识" IS NOT NULL 
                                AND bill."流程标识" <> '' 
                                AND bill."流程标识" <> ' '
                        )  t_bill ON t_bill."id" = bill."根流程实例ID"
                        WHERE 
                            bill."单据类型" = '{self.request_form_name}' 
                            AND t_bill."流程标识" = '{self.request_flow_name}'
                            AND aleh."单据状态" <> '暂存'
                    ),
                    flow_operation AS (
                        SELECT 
                            bill."单据编码",
                            GREATEST(MAX(op_0."修改日期"), MAX(op_1."修改日期"), MAX(op_2."修改日期"), MAX(op_3."修改日期")) AS "响应计算起始时间"
                        FROM flow_object bill
                        LEFT JOIN ods.af_operation_log op_0 ON 
                            bill."流程实例ID" = op_0."流程实例ID" 
                            AND op_0."活动实例名称" = '校线异常发起单提交'
                            AND op_0."结果编码" = 'submit'
                        LEFT JOIN ods.af_operation_log op_1 ON 
                            bill."流程实例ID" = op_0."流程实例ID" 
                            AND op_0."活动实例名称" = '整改人'
                            AND op_0."结果编码" = 'RejectToEdit'
                        LEFT JOIN ods.af_operation_log op_2 ON 
                            bill."流程实例ID" = op_0."流程实例ID" 
                            AND op_0."活动实例名称" = '质量专员'
                            AND op_0."结果编码" = 'RejectToEdit'
                        LEFT JOIN ods.af_operation_log op_3 ON 
                            bill."流程实例ID" = op_0."流程实例ID" 
                            AND op_0."活动实例名称" = '转交处理人'
                            AND op_0."结果编码" = 'RejectToEdit'
                        GROUP BY bill."单据编码"
                        ORDER BY bill."单据编码"
                    )
                SELECT 
                    bill."单据编码",
                    bill."响应计算起始时间",
                    afhf."响应时间" AS "实际响应时间"
                FROM flow_operation bill
                LEFT JOIN ods.alignment_error_handle afhf ON afhf."单据编号" = bill."单据编码"
            '''
        ).fetchdf()

        def temp_apply_handle(row: pd.Series) -> pd.Series:
            row["预计及时响应时间"] = self.judje_time_func(row["响应计算起始时间"], self.request_time["响应"])
            row["响应用时"] = self.judje_time_inverse_func(row["响应计算起始时间"], row["实际响应时间"])
            row["是否及时响应"] = judge_on_time(row["预计及时响应时间"], row["实际响应时间"])
            return row
        flow_operation_handle = flow_operation_handle.apply(temp_apply_handle, axis=1)
        # 处理单及时相关计算
        flow_operation_initiate: pd.DataFrame = self.connect.sql(
            f"""
                WITH flow_object AS (
                        SELECT 
                            bill."id",
                            bill."单据编码",
                            bill."流程实例ID",
                            bill."流程标识",
                        FROM ods.af_historical_flow bill
                        LEFT JOIN ods.alignment_error_handle aleh ON aleh."单据编号" = bill."单据编码"
                        WHERE 
                            bill."实体编码" = '{self.deal_form_name}' 
                            AND bill."流程标识" = '{self.deal_flow_name}'
                        UNION
                        SELECT 
                            bill."id",
                            bill."单据编码",
                            bill."流程实例ID",
                            t_bill."流程标识"
                        FROM ods.af_current_flow bill
                        LEFT JOIN ods.alignment_error_handle aleh ON aleh."单据编号" = bill."单据编码"
                        LEFT JOIN (
                            SELECT
                                bill."id",
                                bill."流程标识",
                            FROM ods.af_current_flow bill
                            WHERE 
                                bill."流程标识" IS NOT NULL 
                                AND bill."流程标识" <> '' 
                                AND bill."流程标识" <> ' '
                        )  t_bill ON t_bill."id" = bill."根流程实例ID"
                        WHERE 
                            bill."单据类型" = '{self.deal_form_name}' 
                            AND t_bill."流程标识" = '{self.deal_flow_name}'
                    ),
                flow_operation AS (
                    SELECT 
                        bill."单据编码",
                        MAX(op_0."修改日期") AS "实际一次诊断时间",
                        MAX(op_1."修改日期") AS "实际二次诊断时间",
                        MAX(op_1."修改日期") AS "实际返工时间",
                        MAX(op_1."修改日期") AS "实际验收时间",
                    FROM flow_object bill
                LEFT JOIN ods.af_operation_log op_0 ON 
                    bill."流程实例ID" = op_0."流程实例ID" 
                    AND op_0."活动实例名称" = '校线异常处理提交'
                    AND op_0."结果编码" = 'submit'
                LEFT JOIN ods.af_operation_log op_1 ON 
                    bill."流程实例ID" = op_1."流程实例ID" 
                    AND op_1."活动实例名称" = '指定诊断人'
                    AND op_1."结果编码" = 'Consent'
                LEFT JOIN ods.af_operation_log op_2 ON 
                    bill."流程实例ID" = op_2."流程实例ID" 
                    AND op_2."活动实例名称" = '返工执行人'
                    AND op_2."结果编码" = 'Consent'
                LEFT JOIN ods.af_operation_log op_3 ON 
                    bill."流程实例ID" = op_3."流程实例ID" 
                    AND op_3."活动实例名称" = '提报人'
                    AND op_3."结果编码" = 'Consent'
                    GROUP BY bill."单据编码"
                    ORDER BY bill."单据编码"
                )
                SELECT
                    alei."单据编号" AS "单据编码",
                    alei."响应时间" AS "一次诊断计算起始时间",
                    bill."实际一次诊断时间",
                    bill."实际二次诊断时间",
                    bill."实际返工时间",
                    bill."实际验收时间",
                FROM ods.alignment_error_initiate alei 
                LEFT JOIN flow_operation bill ON alei."单据编号" = bill."单据编码"
                WHERE NOT bill."单据编码" IS NULL
            """
        ).fetchdf()

        def temp_apply_initiate(row: pd.Series) -> pd.Series:
            row["预计及时一次诊断时间"] = self.judje_time_func(row["一次诊断计算起始时间"], self.request_time["一次诊断"])
            row["一次诊断用时"] = self.judje_time_inverse_func(row["一次诊断计算起始时间"], row["实际一次诊断时间"])
            row["是否及时一次诊断"] = judge_on_time(row["预计及时一次诊断时间"], row["实际一次诊断时间"])

            row["预计及时二次诊断时间"] = self.judje_time_func(row["实际一次诊断时间"], self.request_time["二次诊断"])
            row["二次诊断用时"] = self.judje_time_inverse_func(row["实际一次诊断时间"], row["实际二次诊断时间"])
            row["是否及时二次诊断"] = judge_on_time(row["预计及时二次诊断时间"], row["实际二次诊断时间"])

            row["预计及时返工时间"] = self.judje_time_func(row["实际二次诊断时间"], self.request_time["返工"])
            row["返工用时"] = self.judje_time_inverse_func(row["实际二次诊断时间"], row["实际返工时间"])
            row["是否及时返工"] = judge_on_time(row["预计及时返工时间"], row["实际返工时间"])

            row["预计及时验收时间"] = self.judje_time_func(row["实际返工时间"], self.request_time["验收"])
            row["验收用时"] = self.judje_time_inverse_func(row["实际返工时间"], row["实际验收时间"])
            row["是否及时验收"] = judge_on_time(row["预计及时验收时间"], row["实际验收时间"])
            return row
        flow_operation_initiate = flow_operation_initiate.apply(temp_apply_initiate, axis=1)
        # 单据在不同流程中所属的部门
        flow_operation_department: pd.DataFrame = self.connect.sql(
            f"""
                WITH flow_person AS (
                        SELECT  
                            aleh."单据编号",
                            COALESCE(aleh."响应人工号", aleh."转交人工号", aleh."整改人工号") AS "响应所属人工号",
                            alei."提报人工号" AS "一次诊断所属人工号",
                            alei."指定诊断人工号" AS "二次诊断所属人工号",
                            alei."提报人工号" AS "返工所属人工号",
                            aleh."提报人工号" AS "验收所属人工号"
                        FROM ods.alignment_error_handle aleh
                        FULL JOIN ods.alignment_error_initiate alei ON aleh."单据编号" = alei."单据编号"
                    )
                    SELECT
                        f."单据编号",
                        p0."末级组织名称" AS "响应所属组室",
                        p1."末级组织名称" AS "一次诊断所属组室",
                        p2."末级组织名称" AS "二次诊断所属组室",
                        p3."末级组织名称" AS "返工所属组室",
                        p4."末级组织名称" AS "验收所属组室"
                    FROM flow_person f
                    LEFT JOIN ods.person p0 ON p0."员工编码" = f."响应所属人工号"
                    LEFT JOIN ods.person p1 ON p1."员工编码" = f."一次诊断所属人工号"
                    LEFT JOIN ods.person p2 ON p2."员工编码" = f."二次诊断所属人工号"
                    LEFT JOIN ods.person p3 ON p3."员工编码" = f."返工所属人工号"
                    LEFT JOIN ods.person p4 ON p4."员工编码" = f."验收所属人工号"
            """
        ).fetchdf()
        # 单据的责任单位分布
        flow_operation_responsibilities: pd.DataFrame = self.connect.sql(
            f"""
                SELECT
                    alei."单据编号",
                    alei."责任单位"
                FROM ods.alignment_error_initiate alei
                WHERE NOT alei."责任单位" IS NULL
            """
        ).fetchdf()
        # 单据的异常构型分布
        flow_operation_configuration: pd.DataFrame = self.connect.sql(
            f"""
                SELECT
                    alei."单据编号",
                    COALESCE(alei."构型名称", alei."下推构型项名称") AS "构型分类"
                FROM ods.alignment_error_initiate alei
                WHERE NOT COALESCE(alei."构型名称", alei."下推构型项名称") IS NULL
            """
        ).fetchdf()
        # 单据的所属项目分布
        flow_operation_project: pd.DataFrame = self.connect.sql(
            f"""
                SELECT
                    aleh."单据编号",
                    COALESCE(aleh."项目简称", aleh."项目名称") AS "项目名称"
                FROM ods.alignment_error_handle aleh
            """
        ).fetchdf()
        # 单据的所属项目分布
        flow_operation_reason: pd.DataFrame = self.connect.sql(
            f"""
                SELECT
                    alei."单据编号",
                    alei."失效原因"
                FROM ods.alignment_error_initiate alei
                WHERE NOT alei."失效原因" IS NULL
            """
        ).fetchdf()

        # 校线异常相关明细
        self.connect.sql(
            f"""
                CREATE OR REPLACE TABLE dwd.ontime_final_result AS 
                    SELECT 
                        f0."单据编码",
                        f3."责任单位",
                        f4."构型分类",
                        f5."项目名称",
                        f6."失效原因",

                        f0."响应计算起始时间",
                        f0."预计及时响应时间",
                        f0."实际响应时间",
                        f0."响应用时",
                        f0."是否及时响应",
                        f2."响应所属组室",

                        f1."一次诊断计算起始时间",
                        f1."预计及时一次诊断时间",
                        f1."实际一次诊断时间",
                        f1."一次诊断用时",
                        f1."是否及时一次诊断",
                        f2."一次诊断所属组室",

                        f1."实际一次诊断时间" AS "二次诊断计算起始时间",
                        f1."预计及时二次诊断时间",
                        f1."实际二次诊断时间",
                        f1."二次诊断用时",
                        f1."是否及时二次诊断",
                        f2."二次诊断所属组室",

                        f1."实际二次诊断时间" AS "返工计算起始时间",
                        f1."预计及时返工时间",
                        f1."实际返工时间",
                        f1."返工用时",
                        f1."是否及时返工",
                        f2."返工所属组室",

                        f1."实际返工时间" AS "验收计算起始时间",
                        f1."预计及时验收时间",
                        f1."实际验收时间",
                        f1."验收用时",
                        f1."是否及时验收",
                        f2."验收所属组室"
                    FROM flow_operation_handle f0 
                    FULL JOIN flow_operation_initiate f1 ON f0."单据编码" = f1."单据编码"
                    LEFT JOIN flow_operation_department f2 ON f0."单据编码" = f2."单据编号"
                    LEFT JOIN flow_operation_responsibilities f3 ON f0."单据编码" = f3."单据编号"
                    LEFT JOIN flow_operation_configuration f4 ON f0."单据编码" = f4."单据编号"
                    LEFT JOIN flow_operation_project f5 ON f0."单据编码" = f5."单据编号"
                    LEFT JOIN flow_operation_reason f6 ON f0."单据编码" = f6."单据编号"
            """
        )
        # 前端上最上面那一排指标卡片的数据
        temp_map = {
            "未响应异常数": 0,
            "一次诊断进行中流程数": 1,
            "二次诊断进行中流程数": 2,
            "返工进行中流程数": 3,
            "验收进行中流程数": 4,
        }

        def make_json_head(title_name: str, real_time_name: str, start_time_name: str, use_time_name: str, request_time_name: str) -> dict[str, Any]:
            in_process = self.connect.sql(
                f"""
                    SELECT 
                        COUNT(bill."单据编码")
                    FROM dwd.ontime_final_result bill
                    WHERE bill."{real_time_name}" IS NULL AND NOT bill."{start_time_name}" IS NULL
                """
            ).fetchall()[0][0]
            average_value = self.connect.sql(
                f"""
                    SELECT 
                        SUM(epoch(bill."{use_time_name}") / 60) / COUNT(bill."单据编码")
                    FROM dwd.ontime_final_result bill
                    WHERE NOT bill."{use_time_name}" IS NULL
                """
            ).fetchall()[0][0]
            request_value = int(self.request_time[request_time_name].total_seconds() / 60)
            trend = 'ontime' if request_value > average_value else 'overtime'
            return {
                "index": temp_map[title_name],
                "title_name": title_name,
                "trend": trend,
                "request_value": in_process,
                "request_time": request_value,
                "average_time": average_value,
            }
        calibration_line_total_data = [
            make_json_head("未响应异常数", "实际响应时间", "响应计算起始时间", "响应用时", "响应"),
            make_json_head("一次诊断进行中流程数", "实际一次诊断时间", "一次诊断计算起始时间", "一次诊断用时", "一次诊断"),
            make_json_head("二次诊断进行中流程数", "实际二次诊断时间", "二次诊断计算起始时间", "二次诊断用时", "二次诊断"),
            make_json_head("返工进行中流程数", "实际返工时间", "返工计算起始时间", "返工用时", "返工"),
            make_json_head("验收进行中流程数", "实际验收时间", "验收计算起始时间", "验收用时", "验收"),
        ]
        # 中间几个用于显示占比的卡片
        def make_json_center(title_name: str, colunms_name: str) -> dict[str, Any]:
            temp_data: pd.DataFrame = self.connect.sql(
                f"""
                    SELECT 
                        bill."{colunms_name}" AS "label",
                        COUNT(bill."单据编码") AS "value",
                    FROM dwd.ontime_final_result bill
                    WHERE NOT bill."{colunms_name}" IS NULL 
                        AND bill."响应计算起始时间" >= (DATE_TRUNC('month', CURRENT_DATE)::TIMESTAMP_NS)  
                    GROUP BY bill."{colunms_name}"
                """
            ).fetchdf()
            temp_res_data = []
            for index, row in temp_data.iterrows():
                temp_res_data.append(
                    {
                        "label": row["label"],
                        "value": row["value"],
                    }
                )
            return {
                "title_name": title_name,
                "data": temp_res_data,
            }
        pie_chart_error_data = [
            make_json_center("本月异常构型组成", "构型分类"),
            make_json_center("本月异常项目占比", "项目名称"),
            make_json_center("本月异常责任单位占比", "责任单位"),
            make_json_center("本月异常类型组成", "失效原因"),
        ]
        # 下面几个用于显示及时率的卡片
        def make_json_back(title_name: str, colunms_start: str, colunms_or_not: str,colunms_group: str) -> dict[str, Any]:
            temp_data: pd.DataFrame = self.connect.sql(
                f"""
                    SELECT 
                        bill."{colunms_group}" AS "group_name",
                        SUM(
                            CASE bill."{colunms_or_not}" 
                                WHEN '未及时'
                                THEN 0
                                WHEN '及时'
                                THEN 1
                                ELSE 0
                            END
                        ) AS "ontime_value",
                        SUM(
                            CASE bill."{colunms_or_not}" 
                                WHEN '未及时'
                                THEN 1
                                WHEN '及时'
                                THEN 1
                                ELSE 0
                            END
                        ) AS "total_value"
                    FROM dwd.ontime_final_result bill
                    WHERE NOT bill."{colunms_start}" IS NULL 
                        AND bill."{colunms_start}" >= (DATE_TRUNC('month', CURRENT_DATE)::TIMESTAMP_NS)
                    GROUP BY bill."{colunms_group}"
                """
            ).fetchdf()
            temp_res_data = []
            temp_trend = 0
            for index, row in temp_data.iterrows():
                temp_trend += row["ontime_value"] / row["total_value"] if row["total_value"] != 0 else 0
                temp_res_data.append(
                    {
                        "group_name": row["group_name"],
                        "ontime_value": row["ontime_value"],
                        "total_value": row["total_value"],
                    }
                )
            temp_trend = temp_trend / len(temp_res_data)
            trend = 'inlimit' if temp_trend >= self.on_time_norms else 'overlimit'
            return {
                "title_name": title_name,
                "trend": trend,
                "group": temp_res_data,
            }
        calibration_line_group_data = [
            make_json_back("异常响应及时率", "响应计算起始时间", "是否及时响应", "响应所属组室"),
            make_json_back("一次诊断及时率", "一次诊断计算起始时间", "是否及时一次诊断", "一次诊断所属组室"),
            make_json_back("二次诊断及时率", "二次诊断计算起始时间", "是否及时二次诊断", "二次诊断所属组室"),
            make_json_back("返工及时率", "返工计算起始时间", "是否及时返工", "返工所属组室"),
            make_json_back("验收及时率", "验收计算起始时间", "是否及时验收", "验收所属组室"),
        ]
        # 写入数据库
        collection = self.mongo["liteweb"]["calibration_line_total_data"]
        with self.mongo.start_session() as session:
            collection = self.mongo["liteweb"]["calibration_line_total_data"]
            collection.drop(session=session)
            collection.insert_many(calibration_line_total_data, session=session)
            collection = self.mongo["liteweb"]["pie_chart_error_data"]
            collection.drop(session=session)
            collection.insert_many(pie_chart_error_data, session=session)
            collection = self.mongo["liteweb"]["calibration_line_group_data"]
            collection.drop(session=session)
            collection.insert_many(calibration_line_group_data, session=session)