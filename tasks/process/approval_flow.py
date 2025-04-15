import pandas as pd
import xml.etree.ElementTree as ET
from tasks.process import process
from utils.connect import connect_data


class entity_design_process(process):
    def __init__(self, connect: connect_data) -> None:
        super().__init__(connect.duckdb, connect.mongo, "苍穹数据模型处理", "entity_design_process")

    def task_main(self) -> None:
        entity_df: pd.DataFrame = self.connect.sql('SELECT * FROM ods.af_entity_design').fetchdf()

        def temp_apply(row: pd.Series) -> pd.Series:
            temp_row = {
                'id': str(row["id"]), 
                '单据名称':  str(row["单据名称"]), 
                '单据数据库表名': ""
            }
            root = ET.fromstring(row["单据xml数据"])
            Items = root[0].findall("Items")
            if len(Items) == 0:
                return pd.Series(temp_row)
            BillEntity = Items[0].findall("BillEntity")
            if len(BillEntity) == 0:
                return pd.Series(temp_row)
            TableName = BillEntity[0].findall("TableName")
            if len(TableName) == 0:
                return pd.Series(temp_row)
            temp_row["单据数据库表名"] = str(TableName[0].text)
            return pd.Series(temp_row)
        new_df = entity_df.apply(temp_apply, axis=1)
        self.connect.sql(
            f'''
                CREATE OR REPLACE TABLE dwd.entity_design_process AS SELECT * FROM new_df
            '''
        )



