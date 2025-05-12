import duckdb
import pymongo
import datetime
from tasks.base import task


class process(task):
    '''处理任务的基类,因为处理任务只需要本地duck连接和mongo连接,在这里做维护就不需要所有的重写一遍'''

    def __init__(self, _duckdb: duckdb.DuckDBPyConnection, _mongo: pymongo.MongoClient, name: str, logger_name: str) -> None:
        super().__init__(_duckdb, name, logger_name)
        self.connect = _duckdb.cursor()
        self.mongo = _mongo

    def __del__(self) -> None:
        self.connect.close()

    def update_time(self, name: str, session=None):
        # 更新更新时间记录
        collection = self.mongo["liteweb"]["update_time"]
        collection.delete_one({'name': name}, session=session)
        collection.insert_one({
            'name': name,
            'time': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }, session=session)
