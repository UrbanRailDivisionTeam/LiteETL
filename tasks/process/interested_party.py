from tasks.process import process
from utils.connect import connect_data
from utils.config import CONFIG

class interested_party(process):
    def __init__(self, connect: connect_data) -> None:
        super().__init__(connect.duckdb, "相关方数据处理", "interested_party_process")
        self.mongo = connect.mongo
        
    def task_main(self) -> None:
        ...