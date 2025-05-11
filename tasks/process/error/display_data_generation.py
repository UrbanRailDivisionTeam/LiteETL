import duckdb
import datetime
import pandas as pd
from typing import Any


def make_json_head(
    connect: duckdb.DuckDBPyConnection,
    request_time: dict[str, datetime.timedelta],
    head_map: dict[str, int],
    title_name: str,
    real_time_name: str,
    start_time_name: str,
    use_time_name: str,
    request_time_name: str
) -> dict[str, Any]:
    '''
    构建前端上最上面那一排指标卡片的数据
    '''
    in_process = connect.sql(
        f"""
            SELECT 
                COUNT(bill."单据编码")
            FROM dwd.ontime_final_result bill
            WHERE bill."{real_time_name}" IS NULL AND NOT bill."{start_time_name}" IS NULL
        """
    ).fetchall()[0][0]
    average_value = connect.sql(
        f"""
            SELECT 
                SUM(epoch(bill."{use_time_name}") / 60) / COUNT(bill."单据编码")
            FROM dwd.ontime_final_result bill
            WHERE NOT bill."{use_time_name}" IS NULL
        """
    ).fetchall()[0][0]
    request_value = int(request_time[request_time_name].total_seconds() / 60)
    trend = 'ontime' if request_value > average_value else 'overtime'
    return {
        "index": head_map[title_name],
        "title_name": title_name,
        "trend": trend,
        "request_value": in_process,
        "request_time": request_value,
        "average_time": average_value,
    }


def make_json_center(
    connect: duckdb.DuckDBPyConnection,
    center_map: dict[str, int],
    title_name: str,
    colunms_name: str
) -> dict[str, Any]:
    '''
    中间几个用于显示占比的卡片
    '''
    temp_data: pd.DataFrame = connect.sql(
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
        "index": center_map[title_name],
        "title_name": title_name,
        "data": temp_res_data,
    }


def make_json_back(
    connect: duckdb.DuckDBPyConnection,
    on_time_norms: float,
    title_name: str,
    colunms_start: str,
    colunms_or_not: str,
    colunms_group: str
) -> dict[str, Any]:
    '''
    下面几个用于显示及时率的卡片
    '''
    temp_data: pd.DataFrame = connect.sql(
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
    trend = 'inlimit' if temp_trend >= on_time_norms else 'overlimit'
    return {
        "title_name": title_name,
        "trend": trend,
        "group": temp_res_data,
    }


def make_json_reason(
    connect: duckdb.DuckDBPyConnection,
    type_name: str,
) -> dict[str, Any]:
    '''
    构建中间显示失效原因的数据
    '''
    temp_data: pd.DataFrame = connect.sql(
        f"""
            SELECT
                bill."失效原因_二级" AS "label",
                COUNT(bill."单据编码") AS "value"
            FROM dwd.ontime_final_result bill
            WHERE NOT bill."失效原因_一级" IS NULL
                AND bill."失效原因_一级" = '{type_name}'
                AND bill."响应计算起始时间" >= (DATE_TRUNC('month', CURRENT_DATE)::TIMESTAMP_NS)
            GROUP BY bill."失效原因_二级"
        """
    ).fetchdf()
    return {
        # 这里的顺序不重要，仅按照类型匹配要求随机赋值
        'index': len(type_name),
        'title_name': type_name + '导致的异常',
        'data': [
            {
                'label': ch['label'],
                'value': ch['value']
            }
            for index, ch in temp_data.iterrows()
        ]
    }


def process(
    connect: duckdb.DuckDBPyConnection,
    request_time: dict[str, datetime.timedelta],
    head_map: dict[str, int],
    center_map: dict[str, int],
    on_time_norms: float
) -> dict[str, list[dict[str, Any]]]:
    calibration_line_total_data = [
        make_json_head(connect, request_time, head_map, "未响应异常数", "实际响应时间", "响应计算起始时间", "响应用时", "响应"),
        make_json_head(connect, request_time, head_map, "一次诊断进行中流程数", "实际一次诊断时间", "一次诊断计算起始时间", "一次诊断用时", "一次诊断"),
        make_json_head(connect, request_time, head_map, "二次诊断进行中流程数", "实际二次诊断时间", "二次诊断计算起始时间", "二次诊断用时", "二次诊断"),
        make_json_head(connect, request_time, head_map, "返工进行中流程数", "实际返工时间", "返工计算起始时间", "返工用时", "返工"),
        make_json_head(connect, request_time, head_map, "验收进行中流程数", "实际验收时间", "验收计算起始时间", "验收用时", "验收"),
    ]
    # 中间几个用于显示占比的卡片
    pie_chart_no_error_data = [
        make_json_center(connect, center_map, "本月异常构型组成", "构型分类"),
        make_json_center(connect, center_map, "本月异常项目占比", "项目名称"),
        make_json_center(connect, center_map, "本月异常责任单位占比", "责任单位"),
    ]
    # 失效原因单列一个模块
    temp_data: pd.DataFrame = connect.sql(
        f"""
            SELECT
                DISTINCT 
                bill."失效原因_一级" AS "label",
            FROM dwd.ontime_final_result bill
            WHERE NOT bill."失效原因_一级" IS NULL 
                AND TRIM(bill."失效原因_一级") <> ''
                AND bill."响应计算起始时间" >= (DATE_TRUNC('month', CURRENT_DATE)::TIMESTAMP_NS)
        """
    ).fetchdf()
    pie_chart_error_data = [
        make_json_reason(connect, ch['label']) for index, ch in temp_data.iterrows()
    ]
    # 生成一个整体的数据
    temp_data: pd.DataFrame = connect.sql(
        f"""
            SELECT
                bill."失效原因_一级" AS "label",
                COUNT(bill."单据编码") AS "value"
            FROM dwd.ontime_final_result bill
            WHERE NOT bill."失效原因_一级" IS NULL
                AND bill."响应计算起始时间" >= (DATE_TRUNC('month', CURRENT_DATE)::TIMESTAMP_NS)
            GROUP BY bill."失效原因_一级"
        """
    ).fetchdf()
    pie_chart_error_data.append({
        'index': 0,
        'title_name': '本月异常构型组成',
        'data': [
            {
                'label': ch['label'],
                'value': ch['value']
            }
            for index, ch in temp_data.iterrows()
        ]
    })

    # 下面几个用于显示及时率的卡片
    calibration_line_group_data = [
        make_json_back(connect, on_time_norms, "异常响应及时率", "响应计算起始时间", "是否及时响应", "响应所属组室"),
        make_json_back(connect, on_time_norms, "一次诊断及时率", "一次诊断计算起始时间", "是否及时一次诊断", "一次诊断所属组室"),
        make_json_back(connect, on_time_norms, "二次诊断及时率", "二次诊断计算起始时间", "是否及时二次诊断", "二次诊断所属组室"),
        make_json_back(connect, on_time_norms, "返工及时率", "返工计算起始时间", "是否及时返工", "返工所属组室"),
        make_json_back(connect, on_time_norms, "验收及时率", "验收计算起始时间", "是否及时验收", "验收所属组室"),
    ]
    return {
        'calibration_line_total_data': calibration_line_total_data,
        'pie_chart_no_error_data': pie_chart_no_error_data,
        'pie_chart_error_data': pie_chart_error_data,
        'calibration_line_group_data': calibration_line_group_data
    }
