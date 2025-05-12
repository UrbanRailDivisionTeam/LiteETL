import duckdb
import datetime
import pandas as pd
from tasks.process.error.time_process import specialization_judje_time, specialization_judje_time_inverse, judge_on_time


def process(
    connect: duckdb.DuckDBPyConnection,
    request_time: dict[str, datetime.timedelta],
    specialization_name: dict[str, str],
    commuting_time: list[datetime.timedelta]
) -> None:
    '''
    将苍穹系统中抽出的数据进行计算,转换为我们需要的中间形式
    在这里计算的及时率

    输入：
    connect： duckdb数据库的链接
    request_time：各个关键环节的要求响应时长与流程总时长
    specialization_name：一些经常会更改的关键名称
    commuting_time：工作日的上下班时间
    '''
    # 申请单及时相关计算
    flow_operation_handle: pd.DataFrame = connect.sql(
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
                        bill."实体编码" = '{specialization_name['发起单单据名称']}' 
                        AND bill."流程标识" = '{specialization_name['发起单流程名称']}'
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
                        bill."单据类型" = '{specialization_name['发起单单据名称']}' 
                        AND t_bill."流程标识" = '{specialization_name['发起单流程名称']}'
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
        row["预计及时响应时间"] = specialization_judje_time(connect, commuting_time, row["响应计算起始时间"], request_time["响应"])
        row["响应用时"] = specialization_judje_time_inverse(connect, commuting_time, row["响应计算起始时间"], row["实际响应时间"])
        row["是否及时响应"] = judge_on_time(row["预计及时响应时间"], row["实际响应时间"])
        return row
    flow_operation_handle = flow_operation_handle.apply(temp_apply_handle, axis=1)
    # 处理单及时相关计算
    flow_operation_initiate: pd.DataFrame = connect.sql(
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
                        bill."实体编码" = '{specialization_name['处理单单据名称']}' 
                        AND bill."流程标识" = '{specialization_name['处理单流程名称']}'
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
                        bill."单据类型" = '{specialization_name['处理单单据名称']}' 
                        AND t_bill."流程标识" = '{specialization_name['处理单流程名称']}'
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
        # 移除input_time=参数名，直接传递位置参数
        row["预计及时一次诊断时间"] = specialization_judje_time(connect, commuting_time, row["一次诊断计算起始时间"], request_time["一次诊断"])
        row["一次诊断用时"] = specialization_judje_time_inverse(connect, commuting_time, row["一次诊断计算起始时间"], row["实际一次诊断时间"])
        row["是否及时一次诊断"] = judge_on_time(row["预计及时一次诊断时间"], row["实际一次诊断时间"])

        row["预计及时二次诊断时间"] = specialization_judje_time(connect, commuting_time, row["实际一次诊断时间"], request_time["二次诊断"])
        row["二次诊断用时"] = specialization_judje_time_inverse(connect, commuting_time, row["实际一次诊断时间"], row["实际二次诊断时间"])
        row["是否及时二次诊断"] = judge_on_time(row["预计及时二次诊断时间"], row["实际二次诊断时间"])

        row["预计及时返工时间"] = specialization_judje_time(connect, commuting_time, row["实际二次诊断时间"], request_time["返工"])
        row["返工用时"] = specialization_judje_time_inverse(connect, commuting_time, row["实际二次诊断时间"], row["实际返工时间"])
        row["是否及时返工"] = judge_on_time(row["预计及时返工时间"], row["实际返工时间"])

        row["预计及时验收时间"] = specialization_judje_time(connect, commuting_time, row["实际返工时间"], request_time["验收"])
        row["验收用时"] = specialization_judje_time_inverse(connect, commuting_time, row["实际返工时间"], row["实际验收时间"])
        row["是否及时验收"] = judge_on_time(row["预计及时验收时间"], row["实际验收时间"])
        return row
    flow_operation_initiate = flow_operation_initiate.apply(temp_apply_initiate, axis=1)
    # 单据在不同流程中所属的部门
    flow_operation_department: pd.DataFrame = connect.sql(
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
    flow_operation_responsibilities: pd.DataFrame = connect.sql(
        f"""
            SELECT
                alei."单据编号",
                alei."责任单位"
            FROM ods.alignment_error_initiate alei
            WHERE NOT alei."责任单位" IS NULL
        """
    ).fetchdf()
    # 单据的异常构型分布
    flow_operation_configuration: pd.DataFrame = connect.sql(
        f"""
            SELECT
                alei."单据编号",
                COALESCE(alei."构型名称", alei."下推构型项名称") AS "构型分类"
            FROM ods.alignment_error_initiate alei
            WHERE NOT COALESCE(alei."构型名称", alei."下推构型项名称") IS NULL
        """
    ).fetchdf()
    # 单据的所属项目分布
    flow_operation_project: pd.DataFrame = connect.sql(
        f"""
            SELECT
                aleh."单据编号",
                COALESCE(aleh."项目简称", aleh."项目名称") AS "项目名称"
            FROM ods.alignment_error_handle aleh
        """
    ).fetchdf()
    # 单据的所属项目分布
    flow_operation_reason: pd.DataFrame = connect.sql(
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
    connect.sql(
        f"""
            CREATE OR REPLACE TABLE dwd.ontime_final_result AS 
                SELECT 
                    f0."单据编码",
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
        """
    )
