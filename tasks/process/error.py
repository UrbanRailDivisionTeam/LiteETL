import datetime
from tasks.process import process
from utils.connect import connect_data

def respond_process(input_time: datetime.datetime):
    ...

class alignment_error_process(process):
    def __init__(self, connect: connect_data) -> None:
        super().__init__(connect.duckdb, connect.mongo, "校线异常数据处理", "error_process")
        
    def task_main(self) -> None:
        ...