import datetime
from tasks.process import process
from tasks.process.error.detailed_calculations import detailed_calculations
from tasks.process.error.api_data_generation import api_data_generation
from utils.connect import connect_data


class alignment_error_process(process):
    def __init__(self, connect: connect_data) -> None:
        super().__init__(connect.duckdb, connect.mongo, "校线异常数据处理", "error_process")
        # 各个关键环节的要求响应时长与流程总时长
        self.request_time = {
            "响应": datetime.timedelta(hours=2),
            "一次诊断": datetime.timedelta(hours=2),
            "二次诊断": datetime.timedelta(hours=2),
            "返工": datetime.timedelta(hours=2),
            "验收": datetime.timedelta(hours=2),
        }
        # 响应数据中间表生成
        self.calculater = detailed_calculations(self.connect, self.request_time)
        # api数据生成的函数
        self.generater = api_data_generation(self.connect, self.mongo, self.request_time)

    def task_run(self) -> None:
        # 二有顺序要求，不能替换顺序
        self.calculater.process()
        self.generater.process()
        
        self.update_time('calibration_line')