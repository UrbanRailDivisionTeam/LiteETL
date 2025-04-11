from tasks.process import process
from utils.connect import connect_data

class address_directory_process(process):
    def __init__(self, connect: connect_data) -> None:
        super().__init__(connect.duckdb, connect.mongo, "城轨通讯录生成", "address_directory_process")
        
    def task_main(self) -> None:
        ...