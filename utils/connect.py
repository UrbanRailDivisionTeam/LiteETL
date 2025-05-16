import os
import pymongo
import duckdb
import sqlalchemy
from dataclasses import dataclass
from urllib.parse import quote_plus
from utils.config import CONFIG, connect_config

class connecter:
    def __init__(self) -> None:
        self._connect: dict[str, sqlalchemy.Engine] = {}
        self.make_client(CONFIG.CONNECT)

    def make_client(self, connect_config: dict[str, connect_config]) -> None:
        """
        创建数据连接的工厂类
        用于在对应进程中创建对应进程的数据库连接
        """
        for ch in connect_config:
            temp = connect_config[ch]
            if temp.dbtype == "oracle":
                connect_str = "oracle+cx_oracle://" + temp.user + ":" + quote_plus(temp.password) + "@" + temp.ip + ":" + str(temp.port) + "/?service_name=" + temp.database
                self._connect[ch] = sqlalchemy.create_engine(connect_str, poolclass=sqlalchemy.NullPool, max_overflow=-1, pool_timeout=6000, pool_recycle=3600)
            elif temp.dbtype == "sqlserver":
                connect_str = "mssql+pyodbc://" + temp.user + ":" + quote_plus(temp.password) + "@" + temp.ip + ":" + str(temp.port) + "/" + temp.database + "?driver=ODBC+Driver+17+for+SQL+Server"
                self._connect[ch] = sqlalchemy.create_engine(connect_str, poolclass=sqlalchemy.NullPool, max_overflow=-1, pool_timeout=6000, pool_recycle=3600)
            elif temp.dbtype == "mysql":
                connect_str = "mysql+mysqldb://" + temp.user + ":" + quote_plus(temp.password) + "@" + temp.ip + ":" + str(temp.port) + "/" + temp.database
                self._connect[ch] = sqlalchemy.create_engine(connect_str, poolclass=sqlalchemy.NullPool, max_overflow=-1, pool_timeout=6000, pool_recycle=3600)
            elif temp.dbtype == "pgsql":
                connect_str = "postgresql://" + temp.user + ":" + quote_plus(temp.password) + "@" + temp.ip + ":" + str(temp.port) + "/" + temp.database
                self._connect[ch] = sqlalchemy.create_engine(connect_str, poolclass=sqlalchemy.NullPool, max_overflow=-1, pool_timeout=6000, pool_recycle=3600)
            else:
                raise ValueError("不支持的数据库类型：" + temp.dbtype)

    def __getitem__(self, key: str) -> sqlalchemy.engine.Connection:
        if key not in self._connect.keys():
            raise ValueError("不存在对应的连接")
        return self._connect[key].connect()

    def __setitem__(self, key: str, value: sqlalchemy.engine.Engine) -> None:
        self._connect[key] = value

    def close_all(self) -> None:
        """关闭所有数据库连接"""
        for engine in self._connect.values():
            engine.dispose()

@dataclass
class connect_data:
    duckdb: duckdb.DuckDBPyConnection
    mongo: pymongo.MongoClient
    connect: connecter     

def make_coonect() -> connect_data:
    duckdb_connect = duckdb.connect(os.path.realpath(os.path.join(CONFIG.SOURCE_PATH, "data.db")))
    # 在创建或者连接数据库时确保对应的数据库存在
    duckdb_connect.execute("CREATE SCHEMA IF NOT EXISTS ods")
    duckdb_connect.execute("CREATE SCHEMA IF NOT EXISTS dwd")
    duckdb_connect.execute("CREATE SCHEMA IF NOT EXISTS dm")
    duckdb_connect.execute("CREATE SCHEMA IF NOT EXISTS logger")
    duckdb_connect.execute("CREATE SCHEMA IF NOT EXISTS meta")
    
    client = pymongo.MongoClient(host=CONFIG.MONGO_IP, port=27017)
    _connect = connect_data(
        duckdb = duckdb_connect,
        mongo = client,
        connect = connecter()
    )
    return _connect


