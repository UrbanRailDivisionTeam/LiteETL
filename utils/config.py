import os
from dataclasses import dataclass

@dataclass
class connect_config:
    dbtype: str
    ip: str
    port: int
    user: str
    password: str
    database: str

@dataclass
class config:
    """全局所有的配置"""
    # 所有的连接配置
    CONNECT : dict[str, connect_config]
    # 资源文件对应的路径
    SOURCE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "source")
    # 同步配置的间隔时长
    INTERVAL_DURATION: float = 5 * 60
    # 增量同步变更量最大值，新增的行数和修改的行数之和超过这个数，增量同步就退化为全量同步
    MAX_INCREASE_CHANGE: int = 5000

debug = True
if debug:
    CONFIG = config(
        CONNECT = {
            "clickhouse服务" : connect_config(
                    dbtype="clickhouse",
                    ip="localhost",
                    port=8123,
                    user="cheakf",
                    password="Swq8855830.",
                    database="default"
                ),
            "mysql服务": connect_config(
                    dbtype="mysql",
                    ip="localhost",
                    port=3306,
                    user="cheakf",
                    password="Swq8855830.",
                    database="default"
                ),
            "pgsql服务": connect_config(
                    dbtype="mysql",
                    ip="localhost",
                    port=3306,
                    user="cheakf",
                    password="Swq8855830.",
                    database="default"
                ),
            "oracle服务": connect_config(
                    dbtype="mysql",
                    ip="localhost",
                    port=3306,
                    user="cheakf",
                    password="Swq8855830.",
                    database="default"
                ),
            "sqlserver服务": connect_config(
                    dbtype="mysql",
                    ip="localhost",
                    port=3306,
                    user="cheakf",
                    password="Swq8855830.",
                    database="default"
                ),
            "mongo服务": connect_config(
                    dbtype="mysql",
                    ip="localhost",
                    port=3306,
                    user="cheakf",
                    password="Swq8855830.",
                    database="default"
                ),
        }
    )
else:
    CONFIG = config(
        CONNECT = {
            "相关方数据库" : connect_config(
                    dbtype="pgsql",
                    ip="18.0.163.64",
                    port=10086,
                    user="postgres",
                    password="Swq8855830.",
                    database="postgres"
                ),
        }
    )