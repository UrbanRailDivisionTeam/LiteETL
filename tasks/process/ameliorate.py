import os
import pandas as pd
from tasks.process import process
from utils.config import CONFIG

class ameliorate(process):
    def __init__(self) -> None:
        super().__init__("全员型改善数据分析", "ameliorate_process")
    
    def task_main(self) -> None:
        data_index = pd.read_csv(os.path.join(CONFIG.TABLE_PATH, "城轨事业部改善指标.csv"), encoding="utf-8")
        data = self.connect.sql(
            f"""
                SELECT
                    di."部门",
                    di."组室",
                    di."提交指标",
                    COALESCE(da."提交数量", 0) AS "提交数量",
                    di."提交指标" - COALESCE(da."提交数量", 0) AS "差额",
                    di."负责人",
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
        print(data)