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

def judge_on_time(expect_time: datetime.datetime, real_time: datetime.datetime) -> bool:
    '''判断时间是否及时'''
    if not real_time is pd.NaT:
        return real_time <= expect_time
    else:
        return datetime.datetime.now() <= expect_time
    
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
        self.request_form_name = "crrc_unqualify"
        self.request_flow_name = "Proc_crrc_unqualify_audit_7"
        self.deal_form_name = "crrc_unqualifydeal"
        self.deal_flow_name = "Proc_crrc_unqualifydeal_audit_7"

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
        # 当天异常总数
        today_error = self.connect.sql(
            f'''
            SELECT
                COUNT(bill."id")
            FROM ods.alignment_error_handle bill
            WHERE 
                bill."提报时间" >= CURRENT_DATE
                AND bill."提报时间"  < CURRENT_DATE + INTERVAL '1' DAY
            '''
        ).fetchall()[0][0]
        # 当月异常总数
        month_error = self.connect.sql(
            f'''
            SELECT
                COUNT(bill."id")
            FROM ods.alignment_error_handle bill
            WHERE 
                bill."提报时间" >= DATE_TRUNC('month', CURRENT_DATE)
                AND bill."提报时间" < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1' MONTH
            '''
        ).fetchall()[0][0]
        # 项目的节车异常平均统计
        project_average_error = self.connect.sql(
            f'''
            SELECT
                bill."项目号",
                CAES 
                    WHEN COUNT(bill2."校线节车号") = 0 
                    THEN 0
                    ELSE COUNT(bill."id")  / COUNT(bill2."校线节车号")
                END AS "异常平均数统计"
            FROM ods.alignment_error_handle bill
            LEFT JOIN (
                SELECT 
                    DISTINCT
                    bill."项目号",
                    bill."列车号",
                    bill."校线节车号"
                FROM ods.alignment_error_handle bill
            ) bill2 on bill2."项目号" = bill."项目号"
            GROUP BY bill."项目号"
            '''
        ).fetchdf()
        # 申请单及时相关计算
        flow_operation_0: pd.DataFrame = self.connect.sql(
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
        def temp_apply_0(row: pd.Series) -> pd.Series:
            row["预计及时响应时间"] = process(row["响应计算起始时间"])
            row["是否及时响应"] = judge_on_time(row["预计及时响应时间"], row["实际响应时间"])
            return row
        flow_operation_0 = flow_operation_0.apply(temp_apply_0, axis=1)
        # 处理单及时相关计算
        flow_operation_1: pd.DataFrame = self.connect.sql(
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
                        MAX(op_0."修改日期") AS "实际发起诊断时间",
                        MAX(op_1."修改日期") AS "实际诊断时间",
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
                    alei."响应时间" AS "发起诊断计算起始时间",
                    bill."实际发起诊断时间",
                    bill."实际诊断时间",
                    bill."实际返工时间",
                    bill."实际验收时间",
                FROM ods.alignment_error_initiate alei 
                LEFT JOIN flow_operation bill ON alei."单据编号" = bill."单据编码"
                WHERE NOT bill."单据编码" IS NULL
            """
        ).fetchdf()
        def temp_apply_1(row: pd.Series) -> pd.Series:
            if not row["发起诊断计算起始时间"] is pd.NaT:
                row["预计及时发起诊断时间"] = process(row["发起诊断计算起始时间"])
                row["是否及时发起诊断"] = judge_on_time(row["预计及时发起诊断时间"], row["实际发起诊断时间"])
            else:
                row["预计及时发起诊断时间"] = pd.NaT
                row["是否及时发起诊断"] = pd.NaT
            if not row["实际发起诊断时间"] is pd.NaT:
                row["预计及时诊断时间"] = process(row["实际发起诊断时间"])
                row["是否及时诊断"] = judge_on_time(row["预计及时诊断时间"], row["实际诊断时间"])
            else:
                row["预计及时诊断时间"] = pd.NaT
                row["是否及时诊断"] = pd.NaT
            if not row["实际诊断时间"] is pd.NaT:
                row["预计及时返工时间"] = process(row["实际诊断时间"])
                row["是否及时返工"] = judge_on_time(row["预计及时返工时间"], row["实际返工时间"])
            else:
                row["预计及时返工时间"] = pd.NaT
                row["是否及时返工"] = pd.NaT
            if not row["实际返工时间"] is pd.NaT:
                row["预计及时验收时间"] = process(row["实际返工时间"])
                row["是否及时验收"] = judge_on_time(row["预计及时验收时间"], row["实际返工时间"])
            else:
                row["预计及时验收时间"] = pd.NaT
                row["是否及时验收"] = pd.NaT
            return row
        flow_operation_1 = flow_operation_1.apply(temp_apply_1, axis=1)
        # 及时率相关明细
        ontime_final_result = self.connect.sql(
            f"""
                SELECT 
                    f0."单据编码" AS "单据编码",
                    f0."响应计算起始时间",
                    f0."预计及时响应时间",
                    f0."实际响应时间",
                    f0."是否及时响应",

                    f1."发起诊断计算起始时间",
                    f1."预计及时发起诊断时间",
                    f1."实际发起诊断时间",
                    f1."是否及时发起诊断",

                    f1."实际发起诊断时间" AS "诊断计算起始时间",
                    f1."预计及时诊断时间",
                    f1."实际诊断时间",
                    f1."是否及时诊断",

                    f1."实际诊断时间" AS "返工计算起始时间",
                    f1."预计及时返工时间",
                    f1."实际返工时间",
                    f1."是否及时返工",

                    f1."实际返工时间" AS "验收计算起始时间",
                    f1."预计及时验收时间",
                    f1."实际验收时间",
                    f1."是否及时验收"
                FROM flow_operation_0 f0 
                FULL JOIN flow_operation_1 f1 ON f0."单据编码" = f1."单据编码"
            """
        ).fetchdf()




        

