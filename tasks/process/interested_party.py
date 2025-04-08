from tasks.process import process
from utils.connect import connect_data
from utils.config import CONFIG

class interested_party(process):
    def __init__(self, connect: connect_data) -> None:
        super().__init__(connect.duckdb, connect.mongo, "相关方数据处理", "interested_party_process")
        
    def task_main(self) -> None:
        self.connect.sql(
            f"""
                SELECT COUNT(ip.id) FROM ods.interested_party ip
            """
        )