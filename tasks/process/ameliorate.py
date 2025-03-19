import polars as pl
from tasks.base import task
from utils.connect import CONNECTER

class ameliorate(task):
    def __init__(self) -> None:
        super().__init__("改善数据处理")
        self.connect = CONNECTER.get("clickhouse服务")
        
    def task_main(self) -> None:
        temp_select = f"SELECT * FROM ods."
        source_data = pl.read_database(temp_select, self.connect, batch_size=10000)
        
        
    def __del__(self)-> None:
        self.connect.close()