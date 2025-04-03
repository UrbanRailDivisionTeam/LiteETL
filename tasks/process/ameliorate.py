import os
import pandas as pd
from tasks.process import process
from utils.connect import connect_data
from utils.config import CONFIG

class ameliorate(process):
    def __init__(self, connect: connect_data) -> None:
        super().__init__(connect.duckdb, connect.mongo, "全员型改善数据分析", "ameliorate_process")

    def task_main(self) -> None:
        data_index = pd.read_csv(os.path.join(CONFIG.TABLE_PATH, "城轨事业部改善指标.csv"), encoding="utf-8")
        bill: pd.DataFrame = self.connect.sql(
            f"""
                SELECT 
                    ps."末级组织名称" AS "组室",
                    COUNT(bill."id") AS "提交数量"
                FROM ods.ameliorate AS bill
                left join ods.person as ps on bill."第一题案人工号" = ps."员工编码"
                WHERE bill."提案单位一级" = '城轨事业部' 
                AND bill."提交日期" >= DATE_TRUNC('MONTH', CURRENT_DATE)
                GROUP BY ps."末级组织名称"
            """
        ).fetchdf()
        data_group: pd.DataFrame = self.connect.sql(
            f"""
                SELECT
                    di."部门",
                    di."组室",
                    di."提交指标",
                    COALESCE(bill."提交数量", 0) AS "提交数量",
                    di."提交指标" - COALESCE(bill."提交数量", 0) AS "差额"
                FROM data_index AS di
                LEFT JOIN bill ON di."组室" = bill."组室"
            """
        ).fetchdf()
        data_department: pd.DataFrame = self.connect.sql(
            f"""
                SELECT
                    di."部门",
                    SUM(di."提交指标") AS "提交指标",
                    SUM(COALESCE(bill."提交数量", 0)) AS "提交数量",
                    SUM(di."提交指标" - COALESCE(bill."提交数量", 0)) AS "差额"
                FROM data_index AS di
                LEFT JOIN bill ON di."组室" = bill."组室"
                GROUP BY di."部门"
            """
        ).fetchdf()
        
        data_template = []
        for _, dep in data_department.iterrows():
            temp = {}
            temp["m_name"] = str(dep["部门"])
            temp["rate"] = int(dep["提交数量"]) / int(dep["提交指标"]) * 100 if int(dep["提交指标"]) != 0 else 100
            temp["completed"] = int(dep["提交数量"])
            temp["target"] = int(dep["提交指标"])
            temp["sub"] = []
            for _, ch in data_group.iterrows():
                if ch["部门"] == dep["部门"]:
                    temp_sub = {}
                    temp_sub["m_name"] = str(ch["组室"])
                    temp_sub["rate"] = int(ch["提交数量"]) / int(ch["提交指标"])  * 100 if int(ch["提交指标"]) != 0 else 100
                    temp_sub["completed"] = int(ch["提交数量"])
                    temp_sub["target"] = int(ch["提交指标"])
                    temp["sub"].append(temp_sub)
            data_template.append(temp)
        m_collection = self.mongo["lite_web"]["staff_improvement_analysis"]
        m_collection.delete_many({})
        m_collection.insert_many(data_template)
        
        m_collection = self.mongo["lite_web"]["staff_improvement_data"]
        m_collection.delete_many({})
        m_collection.insert_many(data_template)
        
