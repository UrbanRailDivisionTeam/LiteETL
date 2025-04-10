from tasks.process import process
from utils.connect import connect_data
from utils.config import CONFIG

class attendance(process):
    def __init__(self, connect: connect_data) -> None:
        super().__init__(connect.duckdb, connect.mongo, "异常数据处理", "attendance_process")
        
    def task_main(self) -> None:
        ...