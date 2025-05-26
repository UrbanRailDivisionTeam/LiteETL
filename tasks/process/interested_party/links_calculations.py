import duckdb
import pandas as pd

class links_calculations:
    def __init__(self, connect: duckdb.DuckDBPyConnection) -> None:
        self.connect = connect
        self.local = [
            "总成车间",
            "老调试",
            "新调试",
            "动车组调试基地",
            "磁浮厂房",
            "库外",
            "总成所属交车落车调车区域",
        ]
    
    def get_nodes(self):
        self.connect.sql(
            f"""
                SELECT
                    DISTINCT
                    bill.""
                FROM ods.interested_party_review bill
            """
        )
    
    def process(self):
        ...