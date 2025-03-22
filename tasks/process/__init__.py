from tasks.base import task
from utils.connect import DUCKDB

class process(task):
    '''处理任务的基类,因为处理任务只需要本地duck连接和mongo连接,在这里做维护就不需要所有的重写一遍'''
    def __init__(self, name: str, logger_name: str) -> None:
        super().__init__(name, logger_name)
        self.connect = DUCKDB.cursor()
    
    def __del__(self) -> None:
        self.connect.close()