from tasks.process import process
from utils.connect import connect_data

class entity_design_process(process):
    def __init__(self, connect: connect_data) -> None:
        super().__init__(connect.duckdb, connect.mongo, "苍穹数据模型获取", "entity_design_process")
        
    def task_main(self) -> None:
        ...