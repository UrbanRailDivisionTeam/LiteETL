import duckdb
import pandas as pd
import pymongo
import json
from typing import Any


class api_data_generation:
    def __init__(self, duckdb_connect: duckdb.DuckDBPyConnection, mongodb_connect: pymongo.MongoClient) -> None:
        self.duckdb_connect = duckdb_connect
        self.mongodb_connect = mongodb_connect
        self.interested_party_head_card_data_config = [
            {
                "title": "当前相关方进入事业部人数",
                "status": None
            },
            {
                "title": "当前相关方进入事业部人数",
                "status": "正在作业"
            },
            {
                "title": "当前相关方进入事业部人数",
                "status": ""
            }
        ]

    def interested_party_head_card_data(self) -> list[dict[str, Any]]:
        temp_data = []
        for ch in self.interested_party_head_card_data_config:
            _count = self.duckdb_connect.sql(
                f"""
                    SELECT 
                        COUNT(ip.id) as _count
                    FROM ods.interested_party_review ip
                    WHERE
                        ip."单据状态" = '已审核'
                        {f"""AND ip."作业状态" = '{ch["status"]}'""" if not ch["status"] is None else ""}
                """
            ).fetchall()[0][0]
            temp_data.append({
                "title": ch["title"],
                "value": _count,
            })
        return temp_data

    def interested_party_detail_data(self) -> list[dict[str, Any]]:
        data: pd.DataFrame = self.duckdb_connect.sql(
            "SELECT * FROM ods.interested_party_review"
        ).fetchdf()
        return json.loads(data.to_json(orient='records'))

    def process(self):
        with self.mongodb_connect.start_session() as session:
            collection = self.mongodb_connect["liteweb"]["interested_party_head_card_data"]
            collection.drop(session=session)
            collection.insert_many(self.interested_party_head_card_data(), session=session)

            collection = self.mongodb_connect["liteweb"]["calibration_line_detail_data"]
            collection.drop(session=session)
            collection.insert_many(self.interested_party_detail_data(), session=session)