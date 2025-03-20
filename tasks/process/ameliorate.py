from tasks.base import task
from utils.connect import DUCKDB

class ameliorate(task):
    def __init__(self) -> None:
        super().__init__("改善数据处理", "ameliorate")
        self.connect = DUCKDB.cursor()
        
    def task_main(self) -> None:
        self.connect.sql("SELECT * FROM ods.ameliorate").fetchall()
        
        
    def __del__(self)-> None:
        self.connect.close()