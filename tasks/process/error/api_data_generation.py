import duckdb
import pymongo
import datetime
import pandas as pd
import json
from typing import Any


class api_data_generation:
    def __init__(self, duckdb_connect: duckdb.DuckDBPyConnection, mongodb_connect: pymongo.MongoClient, request_time: dict[str, datetime.timedelta]) -> None:
        self.duckdb_connect = duckdb_connect
        self.mongodb_connect = mongodb_connect
        self.request_time = request_time
        self.on_time_norms = 0.8
        self.calibration_line_total_data_config = [
            {
                "index": 0,
                "title": "未响应异常数",
                "status_colnums": "发起单单据状态",
                "status": "待响应",
                "start_time_name": "响应计算起始时间",
                "use_time_name": "响应用时",
                "request_time_name": "响应",
            },
            {
                "index": 1,
                "title": "一次诊断进行中流程数",
                "status_colnums": "处理单单据状态",
                "status": "处理中",
                "start_time_name": "一次诊断计算起始时间",
                "use_time_name": "一次诊断用时",
                "request_time_name": "一次诊断",
            },
            {
                "index": 2,
                "title": "二次诊断进行中流程数",
                "status_colnums": "处理单单据状态",
                "status": "诊断中",
                "start_time_name": "二次诊断计算起始时间",
                "use_time_name": "二次诊断用时",
                "request_time_name": "二次诊断",
            },
            {
                "index": 3,
                "title": "返工进行中流程数",
                "status_colnums": "处理单单据状态",
                "status": "整改中",
                "start_time_name": "返工计算起始时间",
                "use_time_name": "返工用时",
                "request_time_name": "返工",
            },
            {
                "index": 4,
                "title": "验收进行中流程数",
                "status_colnums": "处理单单据状态",
                "status": "验收中",
                "start_time_name": "验收计算起始时间",
                "use_time_name": "验收用时",
                "request_time_name": "验收",
            },
        ]
        self.calibration_line_group_data_config = [
            {
                "index": 0,
                "title": "异常响应及时率",
                "colunms_start": "响应计算起始时间",
                "colunms_or_not": "是否及时响应",
                "colunms_group": "响应所属组室",
            },
            {
                "index": 1,
                "title": "一次诊断及时率",
                "colunms_start": "一次诊断计算起始时间",
                "colunms_or_not": "是否及时一次诊断",
                "colunms_group": "一次诊断所属组室",
            },
            {
                "index": 2,
                "title": "二次诊断及时率",
                "colunms_start": "二次诊断计算起始时间",
                "colunms_or_not": "是否及时二次诊断",
                "colunms_group": "二次诊断所属组室",
            },
            {
                "index": 3,
                "title": "返工及时率",
                "colunms_start": "返工计算起始时间",
                "colunms_or_not": "是否及时返工",
                "colunms_group": "返工所属组室",
            },
            {
                "index": 4,
                "title": "验收及时率",
                "colunms_start": "验收计算起始时间",
                "colunms_or_not": "是否及时验收",
                "colunms_group": "验收所属组室",
            },
        ]
        self.calibration_line_pie_reason_data_config = [
            {
                "index": 0,
                "title": "本月异常构型组成",
                "colunms_name": "构型分类",
            },
            {
                "index": 1,
                "title": "本月异常项目占比",
                "colunms_name": "项目名称",
            },
            {
                "index": 2,
                "title": "本月异常责任单位占比",
                "colunms_name": "责任单位",
            },
        ]

    def process(self) -> None:
        with self.mongodb_connect.start_session() as session:
            collection = self.mongodb_connect["liteweb"]["calibration_line_total_data"]
            collection.drop(session=session)
            collection.insert_many(self.__calibration_line_total_data(), session=session)

            collection = self.mongodb_connect["liteweb"]["calibration_line_group_data"]
            collection.drop(session=session)
            collection.insert_many(self.__calibration_line_group_data(), session=session)

            collection = self.mongodb_connect["liteweb"]["calibration_line_pie_reason_data"]
            collection.drop(session=session)
            collection.insert_many(self.__calibration_line_pie_reason_data(), session=session)

            collection = self.mongodb_connect["liteweb"]["calibration_line_pie_error_data"]
            collection.drop(session=session)
            collection.insert_many(self.__calibration_line_pie_error_data(), session=session)

            collection = self.mongodb_connect["liteweb"]["calibration_line_detail_data"]
            collection.drop(session=session)
            collection.insert_many(self.__calibration_line_detail_data(), session=session)

    def __calibration_line_total_data(self) -> list[dict[str, Any]]:
        temp_data = []
        for ch in self.calibration_line_total_data_config:
            value = self.duckdb_connect.sql(
                f"""
                SELECT 
                    COUNT(bill."单据编码")
                FROM dwd.ontime_final_result bill
                WHERE bill."{ch["status_colnums"]}" == '{ch["status"]}'
            """
            ).fetchall()[0][0]
            request_time = int(self.request_time[ch["request_time_name"]].total_seconds() / 60)
            average_time = self.duckdb_connect.sql(
                f"""
                    SELECT 
                        SUM(epoch(bill."{ch["use_time_name"]}") / 60) / COUNT(bill."单据编码")
                    FROM dwd.ontime_final_result bill
                    WHERE NOT bill."{ch["use_time_name"]}" IS NULL AND bill.{ch["start_time_name"]} >= (DATE_TRUNC('month', CURRENT_DATE)::TIMESTAMP_NS)  
                """
            ).fetchall()[0][0]
            temp_data.append({
                "index": ch["index"],
                "title": ch["title"],
                "value": value,
                "request_time": request_time,
                "average_time": average_time,
            })
        return temp_data

    def __calibration_line_group_data(self) -> list[dict[str, Any]]:
        temp_data = []
        for ch in self.calibration_line_group_data_config:
            data: pd.DataFrame = self.duckdb_connect.sql(
                f"""
                    SELECT 
                        bill."{ch["colunms_group"]}" AS "name",
                        SUM(
                            CASE bill."{ch["colunms_or_not"]}" 
                                WHEN '未及时'
                                THEN 0
                                WHEN '及时'
                                THEN 1
                                ELSE 0
                            END
                        ) AS "ontime",
                        SUM(
                            CASE bill."{ch["colunms_or_not"]}" 
                                WHEN '未及时'
                                THEN 1
                                WHEN '及时'
                                THEN 1
                                ELSE 0
                            END
                        ) AS "total"
                    FROM dwd.ontime_final_result bill
                    WHERE NOT bill."{ch["colunms_start"]}" IS NULL 
                        AND NOT bill."{ch["colunms_group"]}" IS NULL
                        AND bill."{ch["colunms_start"]}" >= (DATE_TRUNC('month', CURRENT_DATE)::TIMESTAMP_NS)
                    GROUP BY bill."{ch["colunms_group"]}"
                """
            ).fetchdf()
            temp_trend = 0
            group_data = []
            for _, row in data.iterrows():
                temp_trend += row["ontime"] / row["total"] if row["total"] != 0 else 0
                group_data.append({
                    "name": row["group_name"],
                    "ontime": row["ontime"],
                    "total": row["total"],
                })
            temp_data.append({
                "index": ch["index"],
                "title": ch["title"],
                "trend": temp_trend >= self.on_time_norms,
                "group": group_data
            })
        return temp_data

    def __calibration_line_pie_reason_data(self) -> list[dict[str, Any]]:
        temp_data = []
        for ch in self.calibration_line_pie_reason_data_config:
            data: pd.DataFrame = self.duckdb_connect.sql(
                f"""
                    SELECT 
                        bill."{ch["colunms_name"]}" AS "label",
                        COUNT(bill."单据编码") AS "value",
                    FROM dwd.ontime_final_result bill
                    WHERE NOT bill."{ch["colunms_name"]}" IS NULL 
                        AND bill."响应计算起始时间" >= (DATE_TRUNC('month', CURRENT_DATE)::TIMESTAMP_NS)  
                    GROUP BY bill."{ch["colunms_name"]}"
                """
            ).fetchdf()
            res_data = []
            for _, row in data.iterrows():
                res_data.append({
                    "label": row["label"],
                    "value": row["value"],
                })
            temp_data.append({
                "index": ch["index"],
                "title": ch["title"],
                "data": res_data
            })
        return temp_data

    def __calibration_line_pie_error_data(self) -> list[dict[str, Any]]:
        node_list = {}
        data: pd.DataFrame = self.duckdb_connect.sql(
            f"""
                SELECT
                    bill."失效原因_一级" AS "name_1",
                    bill."失效原因_二级" AS "name_2",
                    COUNT(bill."单据编码") AS "value"
                FROM dwd.ontime_final_result bill
                WHERE NOT bill."失效原因_一级" IS NULL
                    AND bill."响应计算起始时间" >= (DATE_TRUNC('month', CURRENT_DATE)::TIMESTAMP_NS)
                GROUP BY bill."失效原因_一级", bill."失效原因_二级"
            """
        ).fetchdf()
        # 生成树形结构
        for _, row in data.iterrows():
            node_split = [row["name_1"]] + str(row["name_2"]).split("_")
            node = node_list
            for ch in node_split:
                if ch not in node.keys():
                    node[ch] = {}
                node = node[ch]
            node["value"] = row["value"]
        # 递归处理结构使其符合api
        def recursion(input_dict: dict):
            tree_list = []
            for key, value in input_dict.items():
                tree = {'name': key}
                if isinstance(value, dict):
                    if 'value' in value:
                        tree['value'] = value['value']
                    else:
                        tree['children'] = [recursion(value)]
                tree_list.append(tree)
            return tree_list
        return recursion(node_list)

    def __calibration_line_detail_data(self) -> list[dict[str, Any]]:
        data: pd.DataFrame = self.duckdb_connect.sql(
            "SELECT * FROM dwd.ontime_final_result"
        ).fetchdf()
        return json.loads(data.to_json(orient='records'))
