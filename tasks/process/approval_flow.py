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
            temp_row = pd.Series(index=['id', '单据名称', '单据数据库表名'])
            temp_row["id"] = row["id"]
            temp_row["单据名称"] = row["单据名称"]
            temp_row["单据数据库表名"] = ET.fromstring(row["单据xml数据"]).findall("EntityMetadata")[0].findall("Items")[0].findall("TableName")[0]
            return temp_row
        new_df = entity_df.apply(temp_apply, axis=1)
        self.connect.sql(
            f'''
                CREATE OR REPLACE TABLE dwd.entity_design_process AS SELECT * FROM new_df
            '''
        )



