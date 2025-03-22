import os
import pandas as pd
from tasks.process import process
from utils.config import CONFIG

class ameliorate(process):
    def __init__(self) -> None:
        super().__init__("全员型改善数据分析", "ameliorate_process")
    
    def task_main(self) -> None:
        data_index = pd.read_csv(os.path.join(CONFIG.TABLE_PATH, "城轨事业部改善指标.csv"), encoding="utf-8")
        print()
        data: pd.DataFrame = self.connect.sql(
            f"""
                SELECT 
                    bill."组室",
                    COUNT(bill."id") AS "提交数量"
                FROM ods.ameliorate AS bill
                WHERE bill."提案单位一级" = '城轨事业部' 
                AND bill."提交日期" >= DATE_TRUNC('MONTH', CURRENT_DATE)
                AND bill."提交日期" < DATE_TRUNC('MONTH', CURRENT_DATE) + INTERVAL '1 MONTH'
                GROUP BY bill."组室"
            """
        ).fetchdf()
        print(data)


# class ameliorate_process(process_base):
#     def __init__(self):
#         super().__init__()
#         self.data_group_assembly = None
#         self.data_group_project = None
#         self.data_group_complex = None
#         self.data_group = None
#         self.data_group_quality = None
#         self.data_group_delivery = None
#         self.data_department = None
#         self.data_ameliorate = None
#         self.now = datetime.datetime.now()
#         self.name = "全员型改善数据处理"
#         if DEBUG:
#             ch = "数据仓库缓存-测试环境"
#         else:
#             ch = "数据仓库缓存"
#         self.LOG = logger_init(self.name)
#         self.engine = database_connect(self.LOG).get(ch)

#     def read(self):
#         self.LOG.info("开始读取")
#         path = r'C://Users//Administrator//Documents//DB_process//城轨各部门改善指标.xlsx'
#         self.data_ameliorate = pd.read_sql_table("ods_ameliorate", self.engine)
#         self.data_department = pd.read_excel(path, sheet_name='各部门指标', dtype=object)
#         self.data_group_quality = pd.read_excel(path, sheet_name='质量技术部组室指标', dtype=object)
#         self.data_group_complex = pd.read_excel(path, sheet_name='综合管理部组室指标', dtype=object)
#         self.data_group_project = pd.read_excel(path, sheet_name='项目工程部组室指标', dtype=object)
#         self.data_group_delivery = pd.read_excel(path, sheet_name='交车车间班组指标', dtype=object)
#         self.data_group_assembly = pd.read_excel(path, sheet_name='总成车间班组指标', dtype=object)
#         return self

#     def write(self):
#         self.LOG.info("开始写入")
#         self.data_department.to_sql(name="dm_ameliorate_department", con=self.engine, index=False, if_exists='replace', chunksize=1000)
#         self.data_group.to_sql(name="dm_ameliorate_group", con=self.engine, index=False, if_exists='replace', chunksize=1000)
#         return {"dm_ameliorate_department": self.data_department, "dm_ameliorate_group": self.data_group}

#     def process(self):
#         self.LOG.info("开始处理")
#         data_ameliorate_temp = self.data_ameliorate[
#             (self.data_ameliorate["提案单位一级"] == "城轨事业部") &
#             (self.data_ameliorate["提交日期"] >= datetime.datetime(self.now.year, self.now.month, 1))
#             ]

#         def temp_apply(row):
#             for _index, _row in data_ameliorate_temp.iterrows():
#                 if row["部门"] == _row["提案单位二级"]:
#                     row["已完成数"] += 1
#             row["差额"] = row["指标"] - row["已完成数"]
#             return row

#         self.data_department.progress_apply(temp_apply, axis=1)
#         self.data_department['差额'] = self.data_department['差额'].apply(lambda x: max(0, x))

#         def temp_apply(row):
#             for _index, _row in data_ameliorate_temp.iterrows():
#                 if row["组室"] == _row["班组"]:
#                     row["已完成数"] += 1
#             row["差额"] = row["指标"] - row["已完成数"]
#             return row

#         self.data_group_quality.progress_apply(temp_apply, axis=1)
#         self.data_group_quality['差额'] = self.data_group_quality['差额'].apply(lambda x: max(0, x))
#         self.data_group_complex.progress_apply(temp_apply, axis=1)
#         self.data_group_complex['差额'] = self.data_group_complex['差额'].apply(lambda x: max(0, x))
#         self.data_group_project.progress_apply(temp_apply, axis=1)
#         self.data_group_project['差额'] = self.data_group_project['差额'].apply(lambda x: max(0, x))
#         self.data_group_delivery.progress_apply(temp_apply, axis=1)
#         self.data_group_delivery['差额'] = self.data_group_delivery['差额'].apply(lambda x: max(0, x))
#         self.data_group_assembly.progress_apply(temp_apply, axis=1)
#         self.data_group_assembly['差额'] = self.data_group_assembly['差额'].apply(lambda x: max(0, x))
#         self.data_group = pd.concat([
#             self.data_group_quality,
#             self.data_group_complex,
#             self.data_group_project,
#             self.data_group_delivery,
#             self.data_group_assembly
#         ])

#         return self
