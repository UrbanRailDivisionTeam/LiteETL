import sqlalchemy
from pymongo import MongoClient
from urllib.parse import quote_plus
from utils.logger import make_logger
from utils.config import CONFIG, connect_config

class connecter:
    def __init__(self) -> None:
        self._connect: dict[str, sqlalchemy.Engine] = {}
        self.log = make_logger("数据库连接池")
        self.make_client(CONFIG.CONNECT)
        self.log.info("数据库连接池初始话完成")

    def make_client(self, connect_config: dict[str, connect_config]) -> None:
        """
        创建数据连接的工厂类
        用于在对应进程中创建对应进程的数据库连接
        """
        # 其他的连接，跟着配置文件走
        for ch in connect_config:
            temp = connect_config[ch]
            if temp.dbtype == "oracle":
                connect_str = "oracle+cx_oracle://" + temp.user + ":" + quote_plus(temp.password) + "@" + temp.ip + ":" + str(temp.port) + "/?service_name=" + temp.database
                self._connect[ch] = sqlalchemy.create_engine(connect_str, poolclass=sqlalchemy.QueuePool, pool_size=10, max_overflow=5, pool_timeout=30, pool_recycle=3600)
            elif temp.dbtype == "sqlserver":
                connect_str = "mssql+pyodbc://" + temp.user + ":" + quote_plus(temp.password) + "@" + temp.ip + ":" + str(temp.port) + "/" + temp.database + "?driver=ODBC+Driver+17+for+SQL+Server"
                self._connect[ch] = sqlalchemy.create_engine(connect_str, poolclass=sqlalchemy.QueuePool, pool_size=10, max_overflow=5, pool_timeout=30, pool_recycle=3600)
            elif temp.dbtype == "mysql":
                connect_str = "mysql+mysqldb://" + temp.user + ":" + quote_plus(temp.password) + "@" + temp.ip + ":" + str(temp.port) + "/" + temp.database
                self._connect[ch] = sqlalchemy.create_engine(connect_str, poolclass=sqlalchemy.QueuePool, pool_size=10, max_overflow=5, pool_timeout=30, pool_recycle=3600)
            elif temp.dbtype == "pgsql":
                connect_str = "postgresql://" + temp.user + ":" + quote_plus(temp.password) + "@" + temp.ip + ":" + str(temp.port) + "/" + temp.database
                self._connect[ch] = sqlalchemy.create_engine(connect_str, poolclass=sqlalchemy.QueuePool, pool_size=10, max_overflow=5, pool_timeout=30, pool_recycle=3600)
            elif temp.dbtype == "clickhouse":
                connect_str = "clickhouse://" + temp.user + ":" + quote_plus(temp.password) + "@" + temp.ip + ":" + str(temp.port) + "/" + temp.database
                self._connect[ch] = sqlalchemy.create_engine(connect_str, poolclass=sqlalchemy.QueuePool, pool_size=10, max_overflow=5, pool_timeout=30, pool_recycle=3600)
            else:
                raise ValueError("不支持的数据库类型：" + temp.dbtype)

    def get(self, key: str) -> sqlalchemy.engine.Connection:
        if key not in self._connect.keys():
            raise ValueError("不存在对应的连接")
        return self._connect[key].connect()

    def close_all(self) -> None:
        """关闭所有数据库连接"""
        for engine in self._connect.values():
            engine.dispose()


CONNECTER = connecter()

logger_name = "mongo服务"
CLIENT = MongoClient(
        f"mongodb://{CONFIG.CONNECT[logger_name].user}:{quote_plus(CONFIG.CONNECT[logger_name].password)}@{CONFIG.CONNECT[logger_name].ip}:{str(CONFIG.CONNECT[logger_name].port)}"
    ) if CONFIG.CONNECT[logger_name].user != "" else MongoClient(
        host=[f"{CONFIG.CONNECT[logger_name].ip}:{str(CONFIG.CONNECT[logger_name].port)}"]    
    )