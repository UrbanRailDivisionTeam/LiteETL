import datetime
from tasks.process import process
from tasks.process.error import detailed_calculations, display_data_generation
from utils.connect import connect_data


class alignment_error_process(process):
    def __init__(self, connect: connect_data) -> None:
        super().__init__(connect.duckdb, connect.mongo, "校线异常数据处理", "error_process")
        # 流程及格标准
        self.on_time_norms = 0.8
        # 一些经常会更改的关键名称
        self.specialization_name = {
            "发起单单据名称": "crrc_unqualify",
            "发起单流程名称": "Proc_crrc_unqualify_audit_7",
            "处理单单据名称": "crrc_unqualifydeal",
            "处理单流程名称": "Proc_crrc_unqualifydeal_audit_7",
        }
        # 各个关键环节的要求响应时长与流程总时长
        self.request_time = {
            "响应": datetime.timedelta(hours=2),
            "一次诊断": datetime.timedelta(hours=2),
            "二次诊断": datetime.timedelta(hours=2),
            "返工": datetime.timedelta(hours=2),
            "验收": datetime.timedelta(hours=2),
        }
        # 工作日的上下班时间
        self.commuting_time = [
            datetime.timedelta(hours=8, minutes=30),
            datetime.timedelta(hours=12, minutes=00),
            datetime.timedelta(hours=13, minutes=30),
            datetime.timedelta(hours=17, minutes=30)
        ]
        # 前端上最上面那一排指标卡片的数据排序
        self.head_map = {
            "未响应异常数": 0,
            "一次诊断进行中流程数": 1,
            "二次诊断进行中流程数": 2,
            "返工进行中流程数": 3,
            "验收进行中流程数": 4,
        }
        # 中间那一排指标卡片的数据排序
        self.center_map = {
            "本月异常构型组成": 0,
            "本月异常项目占比": 1,
            "本月异常责任单位占比": 2,
        }

    def task_run(self) -> None:
        detailed_calculations.process(self.connect, self.request_time, self.specialization_name,self.commuting_time)
        collection_data = display_data_generation.process(
            self.connect, 
            self.request_time, 
            self.head_map,
            self.center_map,
            self.on_time_norms
        )
            
        # 写入数据库
        with self.mongo.start_session() as session:
            collection = self.mongo["liteweb"]["calibration_line_total_data"]
            collection.drop(session=session)
            collection.insert_many(collection_data['calibration_line_total_data'], session=session)
            
            collection = self.mongo["liteweb"]["pie_chart_no_error_data"]
            collection.drop(session=session)
            collection.insert_many(collection_data['pie_chart_no_error_data'], session=session)
            
            collection = self.mongo["liteweb"]["pie_chart_error_data"]
            collection.drop(session=session)
            collection.insert_many(collection_data['pie_chart_error_data'], session=session)
            
            collection = self.mongo["liteweb"]["calibration_line_group_data"]
            collection.drop(session=session)
            collection.insert_many(collection_data['calibration_line_group_data'], session=session)
            
            self.update_time('calibration_line', session=session)