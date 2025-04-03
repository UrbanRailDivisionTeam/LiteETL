import duckdb
import pymongo
from tasks.base import task


class process(task):
    '''处理任务的基类,因为处理任务只需要本地duck连接和mongo连接,在这里做维护就不需要所有的重写一遍'''
    def __init__(self, _duckdb: duckdb.DuckDBPyConnection, _mongo: pymongo.MongoClient, name: str, logger_name: str) -> None:
        super().__init__(_duckdb, name, logger_name)
        self.connect = _duckdb.cursor()
        self.mongo = _mongo
    
    def __del__(self) -> None:
        self.connect.close()