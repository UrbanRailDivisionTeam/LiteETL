import datetime
from tasks.process import process
from utils.connect import connect_data
from tasks.process.interested_party.api_data_generation import api_data_generation
from tasks.process.interested_party.links_calculations import links_calculations


class interested_party_process(process):
    def __init__(self, connect: connect_data) -> None:
        super().__init__(connect.duckdb, connect.mongo, "相关方数据处理", "interested_party_process")
        self.generater = api_data_generation(self.connect, self.mongo)
        self.calculater = links_calculations()
    
    def task_run(self) -> None:
        self.calculater.process()
        self.generater.process()
        self.update_time('interested_party')