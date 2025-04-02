from tasks.process import process
from utils.connect import connect_data
from utils.config import CONFIG

class personnel_effectiveness(process):
    def __init__(self, connect: connect_data) -> None:
        super().__init__(connect.duckdb, "人员效能数据处理", "personnel_effectiveness_process")
        self.mongo = connect.mongo
        
    def task_main(self) -> None:
        ...