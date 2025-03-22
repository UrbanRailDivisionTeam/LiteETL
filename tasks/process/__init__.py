from tasks.base import task
from utils.connect import DUCKDB

class process(task):
    def __init__(self, name: str, logger_name: str) -> None:
        super().__init__(name, logger_name)
        self.connect = DUCKDB.cursor()
    
    def __del__(self) -> None:
        self.connect.close()