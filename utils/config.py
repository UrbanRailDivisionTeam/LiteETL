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
    # 所有的数据源连接配置
    CONNECT: dict[str, connect_config]
    # 资源文件对应的路径
    SOURCE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "source")
    SELECT_PATH = os.path.join(SOURCE_PATH, "select")
    TABLE_PATH = os.path.join(SOURCE_PATH, "table")
    # 同步配置的间隔时长
    INTERVAL_DURATION: float = 5 * 60
    # 增量同步变更量最大值，新增的行数和修改的行数之和超过这个数，增量同步就退化为全量同步
    MAX_INCREASE_CHANGE: int = 5000
    # 默认的批量大小
    DEFAULT_BATCH_SIZE = 100000


debug = True
if debug:
    CONFIG = config(
        CONNECT={
            "mysql服务": connect_config(
                dbtype="mysql",
                ip="172.24.97.186",
                port=3306,
                user="swq",
                password="Swq8855830.",
                database="crrc_secd"
            ),
            "pgsql服务": connect_config(
                dbtype="pgsql",
                ip="172.24.97.186",
                port=5432,
                user="cheakf",
                password="Swq8855830.",
                database="default"
            ),
            "oracle服务": connect_config(
                dbtype="oracle",
                ip="172.24.97.186",
                port=1521,
                user="system",
                password="Swq8855830.",
                database="default"
            ),
            "sqlserver服务": connect_config(
                dbtype="sqlserver",
                ip="172.24.97.186",
                port=1433,
                user="cheakf",
                password="Swq8855830.",
                database=""
            ),
        }
    )
else:
    CONFIG = config(
        CONNECT={
            "相关方数据库": connect_config(
                dbtype="pgsql",
                ip="18.0.163.64",
                port=10086,
                user="postgres",
                password="Swq8855830.",
                database="postgres"
            ),
            "BI与数开用数据库": connect_config(
                dbtype="mysql",
                ip="10.24.5.32",
                port=3306,
                user="cheakf",
                password="Swq8855830.",
                database="dataframe_flow_v2"
            ),
            "BI与数开用数据库_旧": connect_config(
                dbtype="mysql",
                ip="10.24.5.32",
                port=3306,
                user="cheakf",
                password="Swq8855830.",
                database="dataframe_flow"
            ),
            "BI与数开用数据库_供应商": connect_config(
                dbtype="mysql",
                ip="10.24.5.32",
                port=3306,
                user="cheakf",
                password="Swq8855830.",
                database="supplier_use"
            ),
            "EAS": connect_config(
                dbtype="oracle",
                ip="172.18.1.121",
                port=1521,
                user="easselect",
                password="easselect",
                database="eas"
            ),
            "MES": connect_config(
                dbtype="oracle",
                ip="10.24.212.17",
                port=1521,
                user="unimax_cg",
                password="unimax_cg",
                database="ORCL"
            ),
            "SHR": connect_config(
                dbtype="oracle",
                ip="10.24.204.67",
                port=1521,
                user="shr_query",
                password="shr_queryZj123!",
                database="ORCL"
            ),
            "生产辅助系统-城轨": connect_config(
                dbtype="sqlserver",
                ip="10.24.5.154",
                port=1433,
                user="metro",
                password="Metro2023!",
                database="城轨事业部"
            ),
            "生产辅助系统-中转": connect_config(
                dbtype="sqlserver",
                ip="10.24.5.154",
                port=1433,
                user="metro",
                password="Metro2023!",
                database="crrc_temp"
            ),
            "金蝶云苍穹-测试库": connect_config(
                dbtype="mysql",
                ip="10.24.204.37",
                port=3306,
                user="kingdee",
                password="kingdee2020",
                database="crrc_secd"
            ),
            "金蝶云苍穹-正式库": connect_config(
                dbtype="mysql",
                ip="10.24.206.138",
                port=3306,
                user="cosmic",
                password="Mysql@2022!",
                database="crrc_secd"
            ),
            "金蝶云苍穹-正式库-系统": connect_config(
                dbtype="mysql",
                ip="10.24.206.138",
                port=3306,
                user="cosmic",
                password="Mysql@2022!",
                database="crrc_sys"
            ),
            "机车数据库": connect_config(
                dbtype="mysql",
                ip="10.29.31.159",
                port=3306,
                user="crrc_temp",
                password="ETxkrRpCFDZ4LmMr",
                database="crrc_temp"
            ),
            "智能立库": connect_config(
                dbtype="mysql",
                ip="10.24.5.21",
                port=3307,
                user="root",
                password="jftxAdmin",
                database="smart_warehouse"
            ),
            "考勤系统": connect_config(
                dbtype="sqlserver",
                ip="10.24.7.48",
                port=1433,
                user="sa",
                password="Z@hc#8705!$",
                database="GM_MT_70"
            ),
            "数据运用平台-测试库": connect_config(
                dbtype="mysql",
                ip="10.24.7.145",
                port=3316,
                user="zjuser",
                password="zjuser@123",
                database="zj_data"
            ),
            "数据运用平台-正式库": connect_config(
                dbtype="mysql",
                ip="10.24.207.80",
                port=8082,
                user="root",
                password="WnXPLOS8ch",
                database="zj_data"
            ),
        }
    )
