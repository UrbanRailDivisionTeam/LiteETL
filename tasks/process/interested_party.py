import datetime
import pandas as pd
import json
from typing import Any
from tasks.process import process
from utils.connect import connect_data


class interested_party_process(process):
    def __init__(self, connect: connect_data) -> None:
        super().__init__(connect.duckdb, connect.mongo, "相关方数据处理", "interested_party_process")
        self.interested_party_head_card_data_config = [
            {
                "title": "当前相关方进入事业部人数",
                "value": 0
            },
            {
                "title": "当前相关方进入车间人数",
                "value": 0
            },
            {
                "title": "当前相关方临时外出人数",
                "value": 0
            }
        ]
        self.interested_party_center_data_config = [
            {
                "index": 0,
                "title": "本月相关方工作危险源占比",
                "data": []
            },
            {
                "index": 1,
                "title": "本月相关方对接部门占比",
                "data": []
            },
            {
                "index": 2,
                "title": "本月相关方作业依据占比",
                "data": []
            },
            {
                "index": 3,
                "title": "本月相关方作业地点分布",
                "data": []
            }
        ]
        self.interested_party_datatime_dat_congif = [
            {
                "key": 0,
                "trend": True,
                "label": "+0%",
                "series": [],

            },
            {
                "key": 0,
                "trend": True,
                "label": "+0%",
                "series": []
            }
        ]

    def interested_party_node_data(self):
        interested_party_name: pd.DataFrame = self.connect.sql(
            f"""
                SELECT
                    DISTINCT
                    bill."公司名称"
                FROM ods.interested_party_review bill
                WHERE bill."计划开工日期" >= (DATE_TRUNC('month', CURRENT_DATE)::TIMESTAMP_NS)
                    AND bull."单据状态" <> "暂存"
            """
        ).fetchdf()
        location_name: pd.DataFrame = self.connect.sql(
            f"""
                SELECT
                    DISTINCT
                    bill."作业地点"
                FROM ods.interested_party_review bill
                WHERE bill."计划开工日期" >= (DATE_TRUNC('month', CURRENT_DATE)::TIMESTAMP_NS)
                    AND bull."单据状态" <> "暂存"
            """
        ).fetchdf()
        interested_party_name_list: list[str] = interested_party_name["公司名称"].to_list()
        location_list: list[str] = location_name["作业地点"].to_list()

        node_list = interested_party_name_list
        node_list += location_list
        node_list = [{"name": node} for node in node_list]
        return node_list

    def interested_party_links_data(self):
        value: pd.DataFrame = self.connect.sql(
            f"""
                SELECT
                    bill."作业地点",
                    bill."公司名称",
                    COUNT(bill."id") as "计数"
                FROM ods.interested_party_review bill
                WHERE bill."计划开工日期" >= (DATE_TRUNC('month', CURRENT_DATE)::TIMESTAMP_NS)
                    AND bull."单据状态" <> "暂存"
                GROUP BY bill."作业地点", bill."公司名称"
            """
        ).fetchdf()
        links_list = []
        for _, row in value.iterrows():
            links_list.append({
                "source": row["作业地点"],
                "target": row["公司名称"],
                "value": row["计数"],
            })
        return links_list

    def interested_party_head_card_data(self) -> list[dict[str, Any]]:
        self.interested_party_head_card_data_config[0]["value"] = int(self.connect.sql(
            f"""
                SELECT 
                    COUNT(bill."id") as _count
                FROM ods.interested_party_review bill
                WHERE bill."单据状态" = '已审核'
                    AND bill."作业状态" IN ("已进入事业部", "正在作业", "临时外出", "作业完成")
            """
        ).fetchall()[0][0])
        self.interested_party_head_card_data_config[1]["value"] = int(self.connect.sql(
            f"""
                SELECT 
                    COUNT(bill."id") as _count
                FROM ods.interested_party_review bill
                WHERE bill."单据状态" = '已审核'
                    AND bill."作业状态" IN ("已进入事业部", "正在作业", "临时外出", "作业完成")
                    AND bill."作业地点" <> "库外"
            """
        ).fetchall()[0][0])
        self.interested_party_head_card_data_config[2]["value"] = int(self.connect.sql(
            f"""
                SELECT 
                    COUNT(bill."id") as _count
                FROM ods.interested_party_review bill
                WHERE bill."单据状态" = '已审核'
                    AND bill."作业状态" = "临时外出"
            """
        ).fetchall()[0][0])
        return self.interested_party_head_card_data_config

    def interested_party_datatime_data(self):
        month = datetime.datetime.now().month - 1
        temp_data: pd.DataFrame = self.connect.sql(
            f"""
                SELECT
                    TO_CHAR(bill."计划开工日期", 'YYYY-MM') AS month,
                    COUNT(bill."id") AS _count
                FROM ods.interested_party_review bill
                WHERE bill."作业状态" IN ('已进入事业部', '正在作业', '临时外出', '作业完成', '已离开事业部')
                    AND bill."计划开工日期" >= DATE_TRUNC('year', CURRENT_DATE)::TIMESTAMP_NS
                GROUP BY TO_CHAR(bill."计划开工日期", 'YYYY-MM')
                ORDER BY month
            """
        ).fetchdf()
        temp_list = temp_data["_count"].to_list()
        self.interested_party_datatime_dat_congif[0]["series"] = temp_list
        self.interested_party_datatime_dat_congif[0]["key"] = temp_list[month]
        if month > 0:
            if temp_list[month] > temp_list[month-1]:
                self.interested_party_datatime_dat_congif[0]["trend"] = True
                self.interested_party_datatime_dat_congif[0]["label"] = "+" + str(int(int(temp_list[month]) / int(temp_list[month-1]) - 1)) + "%" if int(temp_list[month-1]) != 0 else 0
            else:
                self.interested_party_datatime_dat_congif[0]["trend"] = False
                self.interested_party_datatime_dat_congif[0]["label"] = "-" + str(int(int(temp_list[month-1]) / int(temp_list[month]) - 1)) + "%" if int(temp_list[month]) != 0 else 0
        else:
            self.interested_party_datatime_dat_congif[0]["trend"] = True
            self.interested_party_datatime_dat_congif[0]["label"] = "+0%"
        return self.interested_party_datatime_dat_congif

    def interested_party_center_data(self):
        dangerous_dataframe: pd.DataFrame = self.connect.sql(
            f"""
                SELECT 
                    bill."作业危险性"
                FROM ods.interested_party_review bill
                WHERE bill."计划开工日期" >= (DATE_TRUNC('month', CURRENT_DATE)::TIMESTAMP_NS)
            """
        ).fetchdf()
        dangerous_list: dict[str, int] = {}

        def temp_apply(row: pd.Series):
            temp_str = str(row["作业危险性"]).strip()
            if temp_str == "":
                return row
            tmep_list = temp_str.split(",")
            for ch in tmep_list:
                temp_str_ = ch.strip()
                if temp_str_ == "":
                    continue
                if temp_str_ not in dangerous_list.keys():
                    dangerous_list[temp_str_] = 1
                else:
                    dangerous_list[temp_str_] += 1
            return row
        dangerous_dataframe.apply(temp_apply, axis=1)
        self.interested_party_center_data_config[0]["data"] = [{"label": ch, "value": dangerous_list[ch]} for ch in dangerous_list.keys()]

        data_ = []
        department_dataframe: pd.DataFrame = self.connect.sql(
            f"""
                SELECT 
                    person."二级组织名称",
                    COUNT(bill."id") as "计数"
                FROM ods.interested_party_review bill
                LEFT JOIN ods.person person ON person."员工编码" = bill."事业部对接人工号"
                WHERE person."二级组织名称" IS NOT NULL AND bill."计划开工日期" >= (DATE_TRUNC('month', CURRENT_DATE)::TIMESTAMP_NS)
                GROUP BY person."二级组织名称"
            """
        ).fetchdf()
        for _, row in department_dataframe.iterrows():
            data_.append({
                "label": str(row["二级组织名称"]),
                "value": int(row["计数"])
            })
        self.interested_party_center_data_config[1]["data"] = data_

        data_ = []
        basis_dataframe: pd.DataFrame = self.connect.sql(
            f"""
                SELECT 
                    bill."作业依据",
                    COUNT(bill."id") as "计数"
                FROM ods.interested_party_review bill
                WHERE bill."作业依据" IS NOT NULL AND bill."计划开工日期" >= (DATE_TRUNC('month', CURRENT_DATE)::TIMESTAMP_NS)
                GROUP BY bill."作业依据"
            """
        ).fetchdf()
        for _, row in basis_dataframe.iterrows():
            data_.append({
                "label": str(row["作业依据"]),
                "value": int(row["计数"])
            })
        self.interested_party_center_data_config[2]["data"] = data_

        data_ = []
        location_dataframe: pd.DataFrame = self.connect.sql(
            f"""
                SELECT 
                    bill."作业地点",
                    COUNT(bill."id") as "计数"
                FROM ods.interested_party_review bill
                WHERE bill."作业地点" IS NOT NULL AND bill."计划开工日期" >= (DATE_TRUNC('month', CURRENT_DATE)::TIMESTAMP_NS)
                GROUP BY bill."作业地点"
            """
        ).fetchdf()
        for _, row in location_dataframe.iterrows():
            data_.append({
                "label": str(row["作业地点"]),
                "value": int(row["计数"])
            })
        self.interested_party_center_data_config[3]["data"] = data_
        return self.interested_party_center_data_config

    def interested_party_type_data(self):
        node_list: dict[str, dict[str, dict[str, int]]] = {}
        type_data: pd.DataFrame = self.connect.sql(
            f"""
                SELECT 
                    bill."作业类型",
                    bill."具体作业内容",
                    COUNT(bill."id") AS _count
                FROM ods.interested_party_review bill
                WHERE bill."计划开工日期" >= (DATE_TRUNC('month', CURRENT_DATE)::TIMESTAMP_NS)
                    AND bull."单据状态" <> "暂存"
                GROUP BY bill."作业类型", bill."具体作业内容"
            """
        ).fetchdf()
        for _, row in type_data.iterrows():
            if row["作业类型"] not in node_list.keys():
                node_list[row["作业类型"]] = {}
            if row["具体作业内容"] not in node_list[row["作业类型"]].keys():
                node_list[row["作业类型"]][row["具体作业内容"]] = {}
            if "value" not in node_list[row["作业类型"]][row["具体作业内容"]].keys():
                node_list[row["作业类型"]][row["具体作业内容"]]["value"] = int(row["_count"])
        return self.recursion(node_list)

    def interested_party_project_data(self):
        node_list: dict[str, dict[str, dict[str, int]]] = {}
        project_data: pd.DataFrame = self.connect.sql(
            f"""
                SELECT 
                    bill."项目名称",
                    bill."车号",
                    COUNT(bill."id") AS _count
                FROM ods.interested_party_review bill
                WHERE bill."计划开工日期" >= (DATE_TRUNC('month', CURRENT_DATE)::TIMESTAMP_NS)
                    AND bull."单据状态" <> "暂存"
                GROUP BY bill."项目名称", bill."车号"
            """
        ).fetchdf()
        for _, row in project_data.iterrows():
            if row["项目名称"] not in node_list.keys():
                node_list[row["项目名称"]] = {}
            if row["车号"] not in node_list[row["项目名称"]].keys():
                node_list[row["项目名称"]][row["车号"]] = {}
            if "value" not in node_list[row["项目名称"]][row["车号"]].keys():
                node_list[row["项目名称"]][row["车号"]]["value"] = int(row["_count"])
        return self.recursion(node_list)

    def interested_party_detail_data(self):
        data: pd.DataFrame = self.connect.sql(
            f"""
            SELECT 
                bill."id",
                bill."修改时间",
                bill."申请人姓名",
                bill."申请人身份证号",
                bill."申请人联系电话",
                bill."公司名称",
                bill."是否签订过安全承诺书",
                bill."随行人数",
                bill."是否为作业负责人",
                bill."单据状态",
                bill."作业状态",
                bill."申请作业时间",
                bill."计划开工日期",
                bill."计划开工日期上午/下午" AS "计划开工上下午",
                bill."计划完工日期",
                bill."计划完工日期上午/下午" AS "计划完工上下午",
                bill."作业地点",
                bill."作业类型",
                bill."具体作业内容",
                bill."项目名称",
                bill."车号",
                bill."台位/车道" AS "台位车道",
                bill."作业依据",
                bill."NCR/开口项/设计变更编号" AS "NCR开口项设计变更编号",
                bill."作业危险性",
                bill."是否危险作业",
                bill."是否需要监护人",
                bill."是否需要作业证",
                bill."是否携带危化品",
                bill."携带危化品类型",
                bill."事业部对接人",
                bill."事业部对接人姓名",
                bill."事业部对接人部门",
                bill."事业部对接人工号"
            FROM ods.interested_party_review bill
            """
        ).fetchdf()
        return json.loads(data.to_json(orient='records'))

    def interested_party_dangerous_detail_data(self):
        data: pd.DataFrame = self.connect.sql(
            f"""
                SELECT 
                    DISTINCT
                    bill."申请人姓名" AS "作业人员姓名",
                    bill."申请人联系电话" AS "作业人员联系电话",
                    bill."公司名称" AS "所属相关方",
                    bill."作业地点",
                    bill."台位/车道" AS "台位车道",
                    bill."作业危险性" AS "危险源类型",
                    bill."作业类型",
                    bill."具体作业内容"
                FROM ods.interested_party_review bill
                WHERE bill."作业状态" IN ("已进入事业部", "正在作业", "临时外出", "作业完成")
            """
        ).fetchdf()
        return json.loads(data.to_json(orient='records'))

    def task_run(self) -> None:
        with self.mongo.start_session() as session:
            collection = self.mongo["liteweb"]["interested_party_node_data"]
            collection.drop(session=session)
            collection.insert_many(self.interested_party_node_data(), session=session)

            collection = self.mongo["liteweb"]["interested_party_links_data"]
            collection.drop(session=session)
            collection.insert_many(self.interested_party_links_data(), session=session)

            collection = self.mongo["liteweb"]["interested_party_head_card_data"]
            collection.drop(session=session)
            collection.insert_many(self.interested_party_head_card_data(), session=session)

            collection = self.mongo["liteweb"]["interested_party_datatime_data"]
            collection.drop(session=session)
            collection.insert_many(self.interested_party_datatime_data(), session=session)

            collection = self.mongo["liteweb"]["interested_party_center_data"]
            collection.drop(session=session)
            collection.insert_many(self.interested_party_center_data(), session=session)

            collection = self.mongo["liteweb"]["interested_party_type_data"]
            collection.drop(session=session)
            collection.insert_many(self.interested_party_type_data(), session=session)

            collection = self.mongo["liteweb"]["interested_party_project_data"]
            collection.drop(session=session)
            collection.insert_many(self.interested_party_project_data(), session=session)

            collection = self.mongo["liteweb"]["interested_party_detail_data"]
            collection.drop(session=session)
            collection.insert_many(self.interested_party_detail_data(), session=session)

            collection = self.mongo["liteweb"]["interested_party_dangerous_detail_data"]
            collection.drop(session=session)
            collection.insert_many(self.interested_party_dangerous_detail_data(), session=session)

            self.update_time('interested_party', session=session)

    def recursion(self, input_dict: dict):
        tree_list = []
        for key, value in input_dict.items():
            tree = {'name': key}
            if isinstance(value, dict):
                if 'value' in value:
                    tree['value'] = value['value']
                else:
                    tree['children'] = [self.recursion(value)]
            tree_list.append(tree)
        return tree_list
