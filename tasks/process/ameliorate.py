import os
import pandas as pd
from pymongo.errors import OperationFailure

from tasks.process import process
from utils.config import CONFIG
from utils.connect import MONGO


class ameliorate(process):
    def __init__(self) -> None:
        super().__init__("全员型改善数据分析", "ameliorate_process")

    def task_main(self) -> None:
        data_template = []
        data_index = pd.read_csv(os.path.join(CONFIG.TABLE_PATH, "城轨事业部改善指标.csv"), encoding="utf-8")
        data_group: pd.DataFrame = self.connect.sql(
            f"""
                SELECT
                    di."部门",
                    di."组室",
                    di."提交指标",
                    COALESCE(da."提交数量", 0) AS "提交数量",
                    di."提交指标" - COALESCE(da."提交数量", 0) AS "差额"
                FROM data_index AS di
                LEFT JOIN (
                    SELECT 
                        bill."组室",
                        COUNT(bill."id") AS "提交数量"
                    FROM ods.ameliorate AS bill
                    WHERE bill."提案单位一级" = '城轨事业部' 
                    AND bill."提交日期" >= DATE_TRUNC('MONTH', CURRENT_DATE)
                    AND bill."提交日期" < DATE_TRUNC('MONTH', CURRENT_DATE) + INTERVAL '1 MONTH'
                    GROUP BY bill."组室"
                ) AS da ON di."组室" = da."组室"
            """
        ).fetchdf()
        data_department: pd.DataFrame = self.connect.sql(
            f"""
                SELECT
                    di."部门",
                    SUM(di."提交指标") AS "提交指标",
                    SUM(COALESCE(da."提交数量", 0)) AS "提交数量",
                    SUM(di."提交指标" - COALESCE(da."提交数量", 0)) AS "差额"
                FROM data_index AS di
                LEFT JOIN (
                    SELECT 
                        bill."组室",
                        COUNT(bill."id") AS "提交数量"
                    FROM ods.ameliorate AS bill
                    WHERE bill."提案单位一级" = '城轨事业部' 
                    AND bill."提交日期" >= DATE_TRUNC('MONTH', CURRENT_DATE)
                    AND bill."提交日期" < DATE_TRUNC('MONTH', CURRENT_DATE) + INTERVAL '1 MONTH'
                    GROUP BY bill."组室"
                ) AS da ON di."组室" = da."组室"
                GROUP BY di."部门"
            """
        ).fetchdf()
        for _, dep in data_department.iterrows():
            temp = {}
            temp["m_name"] = str(dep["部门"])
            temp["rate"] = int(dep["提交数量"]) / int(dep["提交指标"]) if int(dep["提交指标"]) != 0 else 1
            temp["completed"] = int(dep["提交数量"])
            temp["target"] = int(dep["提交指标"])
            temp["sub"] = []
            for _, ch in data_group.iterrows():
                if ch["部门"] == dep["部门"]:
                    temp_sub = {}
                    temp_sub["m_name"] = str(dep["组室"])
                    temp_sub["rate"] = int(dep["提交数量"]) / int(dep["提交指标"]) if int(dep["提交指标"]) != 0 else 1
                    temp_sub["completed"] = int(dep["提交数量"])
                    temp_sub["target"] = int(dep["提交指标"])
                    temp["sub"].append(temp_sub)
            data_template.append(temp)
        collection  = MONGO["lite_web"]["staff_improvement_analysis"]
        # 开启一个会话
        with MONGO.start_session() as session:
            try:
                session.start_transaction()
                collection.delete_many({}, session=session)
                collection.insert_many(data_template, session=session)
                session.commit_transaction()
            except OperationFailure as e:
                session.abort_transaction()
                self.log.error(f"数据库事务失败，已回滚：{e}")
            except Exception as e:
                session.abort_transaction()
                self.log.error(f"发生错误，已回滚：{e}")
            finally:
                session.end_session()
            
            
