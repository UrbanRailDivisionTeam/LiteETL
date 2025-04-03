from tasks.process import process
from utils.connect import connect_data
from utils.config import CONFIG

class address_directory(process):
    def __init__(self, connect: connect_data) -> None:
        super().__init__(connect.duckdb, connect.mongo, "城轨通讯录生成", "address_directory_process")
        
    def task_main(self) -> None:
        ...