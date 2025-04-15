from tasks.process import process
from utils.connect import connect_data


class personnel_efpersonnel_effectiveness_processfectiveness(process):
    def __init__(self, connect: connect_data) -> None:
        super().__init__(connect.duckdb, connect.mongo, "人员效能数据处理", "personnel_effectiveness_process")

    def task_main(self) -> None:
        ...
