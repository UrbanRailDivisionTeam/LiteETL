import duckdb
import datetime
import pandas as pd


class detailed_calculations:
    def __init__(self, connect: duckdb.DuckDBPyConnection, request_time: dict[str, datetime.timedelta]) -> None:
        self.connect = connect
        self.request_time = request_time
        self.specialization_name = {
            "发起单单据名称": "crrc_unqualify",
            "发起单流程名称": "Proc_crrc_unqualify_audit_7",
            "处理单单据名称": "crrc_unqualifydeal",
            "处理单流程名称": "Proc_crrc_unqualifydeal_audit_7",
        }
        # 上下班时间的配置
        self.commuting_time = [
            datetime.timedelta(hours=8, minutes=30),
            datetime.timedelta(hours=12, minutes=00),
            datetime.timedelta(hours=13, minutes=30),
            datetime.timedelta(hours=17, minutes=30)
        ]
        # 存储节假日信息的表名
        self.holiday_table_name = "attendance_kq_scheduling_holiday"

    def process(self) -> None:
        '''
        将苍穹系统中抽出的数据进行计算,转换为我们需要的中间形式
        在这里计算的及时率
        '''
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
                            bill."实体编码" = '{self.specialization_name['发起单单据名称']}' 
                            AND bill."流程标识" = '{self.specialization_name['发起单流程名称']}'
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
                            bill."单据类型" = '{self.specialization_name['发起单单据名称']}' 
                            AND t_bill."流程标识" = '{self.specialization_name['发起单流程名称']}'
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
                    afhf."单据编号" AS "单据编码",
                    bill."响应计算起始时间",
                    afhf."响应时间" AS "实际响应时间"
                FROM ods.alignment_error_handle afhf 
                LEFT JOIN flow_operation bill ON afhf."单据编号" = bill."单据编码"
            '''
        ).fetchdf()

        def temp_apply_handle(row: pd.Series) -> pd.Series:
            row["预计及时响应时间"] = self.specialization_judje_time(row["响应计算起始时间"], self.request_time["响应"])
            row["响应用时"] = self.specialization_judje_time_inverse(row["响应计算起始时间"], row["实际响应时间"])
            row["是否及时响应"] = self.judge_on_time(row["预计及时响应时间"], row["实际响应时间"])
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
                            bill."实体编码" = '{self.specialization_name['处理单单据名称']}' 
                            AND bill."流程标识" = '{self.specialization_name['处理单流程名称']}'
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
                            bill."单据类型" = '{self.specialization_name['处理单单据名称']}' 
                            AND t_bill."流程标识" = '{self.specialization_name['处理单流程名称']}'
                    ),
                    flow_operation AS (
                        SELECT 
                            bill."单据编码",
                            MAX(op_0."修改日期") AS "实际一次诊断时间",
                            MAX(op_1."修改日期") AS "实际二次诊断时间",
                            MAX(op_2."修改日期") AS "实际返工时间",
                            MAX(op_3."修改日期") AS "实际验收时间",
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
                    bill."实际验收时间"
                FROM ods.alignment_error_initiate alei 
                LEFT JOIN flow_operation bill ON alei."单据编号" = bill."单据编码"
            """
        ).fetchdf()

        def temp_apply_initiate(row: pd.Series) -> pd.Series:
            # 移除input_time=参数名，直接传递位置参数
            row["预计及时一次诊断时间"] = self.specialization_judje_time(row["一次诊断计算起始时间"], self.request_time["一次诊断"])
            row["一次诊断用时"] = self.specialization_judje_time_inverse(row["一次诊断计算起始时间"], row["实际一次诊断时间"])
            row["是否及时一次诊断"] = self.judge_on_time(row["预计及时一次诊断时间"], row["实际一次诊断时间"])

            row["预计及时二次诊断时间"] = self.specialization_judje_time(row["实际一次诊断时间"], self.request_time["二次诊断"])
            row["二次诊断用时"] = self.specialization_judje_time_inverse(row["实际一次诊断时间"], row["实际二次诊断时间"])
            row["是否及时二次诊断"] = self.judge_on_time(row["预计及时二次诊断时间"], row["实际二次诊断时间"])

            row["预计及时返工时间"] = self.specialization_judje_time(row["实际二次诊断时间"], self.request_time["返工"])
            row["返工用时"] = self.specialization_judje_time_inverse(row["实际二次诊断时间"], row["实际返工时间"])
            row["是否及时返工"] = self.judge_on_time(row["预计及时返工时间"], row["实际返工时间"])

            row["预计及时验收时间"] = self.specialization_judje_time(row["实际返工时间"], self.request_time["验收"])
            row["验收用时"] = self.specialization_judje_time_inverse(row["实际返工时间"], row["实际验收时间"])
            row["是否及时验收"] = self.judge_on_time(row["预计及时验收时间"], row["实际验收时间"])
            return row
        flow_operation_initiate = flow_operation_initiate.apply(temp_apply_initiate, axis=1)
        # 单据在不同流程中所属的部门
        flow_operation_department: pd.DataFrame = self.connect.sql(
            f"""
                WITH flow_person AS (
                        SELECT  
                            aleh."单据编号",
                            COALESCE(aleh."响应人工号", aleh."转交人工号", aleh."整改人工号") AS "响应所属人工号",
                            alei."返工执行人工号" AS "一次诊断所属人工号",  
                            alei."指定诊断人工号" AS "二次诊断所属人工号",   
                            alei."返工执行人工号" AS "返工所属人工号",   
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
                WITH temp_reason AS (
                    SELECT
                        alei."单据编号",
                        alei."失效原因"
                    FROM ods.alignment_error_initiate alei
                    WHERE NOT alei."失效原因" IS NULL
                )
                SELECT 
                    bill."单据编号",
                    CASE
                        WHEN INSTR(bill."失效原因", '.') > 0 THEN STRING_SPLIT(bill."失效原因", '.')[1]
                        WHEN INSTR(bill."失效原因", '_') > 0 THEN STRING_SPLIT(bill."失效原因", '_')[1]
                        ELSE bill."失效原因"
                    END AS "失效原因_一级",
                    CASE
                        WHEN INSTR(bill."失效原因", '.') > 0 THEN STRING_SPLIT(bill."失效原因", '.')[2]
                        WHEN INSTR(bill."失效原因", '_') > 0 THEN STRING_SPLIT(bill."失效原因", '_')[2]
                        ELSE ''
                    END AS "失效原因_二级"
                FROM temp_reason bill
            """
        ).fetchdf()

        # 校线异常相关明细
        self.connect.sql(
            f"""
                CREATE OR REPLACE TABLE dwd.ontime_final_result AS 
                    SELECT 
                        f0."单据编码",
                        f7."单据状态" as "发起单单据状态",
                        f8."单据状态" as "处理单单据状态",
                        f3."责任单位",
                        f4."构型分类",
                        f5."项目名称",
                        f6."失效原因_一级",
                        f6."失效原因_二级",

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
                    LEFT JOIN ods.alignment_error_handle f7 ON f0."单据编码" = f7."单据编号"
                    LEFT JOIN ods.alignment_error_initiate f8 ON f0."单据编码" = f8."单据编号"
            """
        )

    def judge_day(self, input_time: datetime.datetime) -> bool:
        '''判断当天是否是工作日'''
        res = self.connect.sql(
            f'''
                SELECT bill."是否休息"
                FROM ods.{self.holiday_table_name} bill
                WHERE bill."节假日日期" = '{input_time.strftime('%Y-%m-%d %H:%M')}'
            '''
        ).fetchall()
        # 如果有额外的设定，就按照要求返回结果
        if len(res):
            return not bool(res[0])
        # 如果没有额外的节假日，就按照周一到周五上班返回
        return input_time.weekday() < 5

    @staticmethod
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
        self,
        input_time: datetime.datetime,
        work_time_index: datetime.timedelta,
        request_time: datetime.timedelta,
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
        if not self.judge_day(input_time):
            input_time += datetime.timedelta(days=1)
            input_time = input_time.replace(hour=0, minute=0, second=0, microsecond=0)
            return self.judje_time(input_time, work_time_index, request_time)
        today = datetime.datetime(year=input_time.year, month=input_time.month, day=input_time.day)
        am_start = today + self.commuting_time[0]
        am_end = today + self.commuting_time[1]
        pm_start = today + self.commuting_time[2]
        pm_end = today + self.commuting_time[3]
        # 在上午上班前发起的异常，递归到上午上班那一刻
        if input_time < am_start:
            input_time = input_time.replace(hour=am_start.hour, minute=am_start.minute, microsecond=0)
            return self.judje_time(input_time, work_time_index, request_time)
        # 在中午休息发起的异常，递归到下午上班那一刻
        if input_time >= am_end and input_time < pm_start:
            input_time = input_time.replace(hour=pm_start.hour, minute=pm_start.minute, microsecond=0)
            return self.judje_time(input_time, work_time_index, request_time)
        # 在晚上下班后发起的异常，递归到第二天凌晨起始
        if input_time >= pm_end:
            input_time += datetime.timedelta(days=1)
            input_time = input_time.replace(hour=0, minute=0, second=0, microsecond=0)
            return self.judje_time(input_time, work_time_index, request_time)
        # 在上午上班时间发起的异常
        if input_time >= am_start and input_time < am_end:
            plan_respond = request_time - work_time_index
            real_time = am_end - max(am_start, input_time)
            temp_dur = min(real_time, plan_respond)
            work_time_index += temp_dur
            input_time += temp_dur
            return self.judje_time(input_time, work_time_index, request_time)
        # 在下午午上班时间发起的异常
        if input_time >= pm_start and input_time < pm_end:
            plan_respond = request_time - work_time_index
            real_time = pm_end - max(pm_start, input_time)
            temp_dur = min(real_time, plan_respond)
            work_time_index += temp_dur
            input_time += temp_dur
            return self.judje_time(input_time, work_time_index, request_time)
        raise ValueError(f"计算错误，程序不应当运行到这个位置，请检查程序和原始数据, {str(input_time)}, {str(work_time_index)}")

    def judje_time_inverse(
        self,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        work_time_index: datetime.timedelta,
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
        if not self.judge_day(start_time):
            # 如果在同一天，则直接返回
            if start_time.date() == end_time.date():
                return start_time, work_time_index
            # 如果不在则跳过该天
            else:
                start_time += datetime.timedelta(days=1)
                start_time = start_time.replace(hour=0, minute=0, second=0, microsecond=0)
                return self.judje_time_inverse(start_time, end_time, work_time_index)
        else:
            today = datetime.datetime(year=start_time.year, month=start_time.month, day=start_time.day)
            am_start = today + self.commuting_time[0]
            am_end = today + self.commuting_time[1]
            pm_start = today + self.commuting_time[2]
            pm_end = today + self.commuting_time[3]
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
            return self.judje_time_inverse(start_time, end_time, work_time_index)

    def specialization_judje_time(self, input_time: datetime.datetime, request_time: datetime.timedelta) -> datetime.datetime | None:
        if input_time is pd.NaT:
            return None
        real_time, _ = self.judje_time(
            input_time=input_time,
            work_time_index=datetime.timedelta(),
            request_time=request_time,
        )
        return real_time

    def specialization_judje_time_inverse(self, start_time: datetime.datetime, end_time: datetime.datetime) -> datetime.timedelta | None:
        if start_time is pd.NaT or end_time is pd.NaT:
            return None
        _, work_time_index = self.judje_time_inverse(
            start_time=start_time,
            end_time=end_time,
            work_time_index=datetime.timedelta(),
        )
        return work_time_index
