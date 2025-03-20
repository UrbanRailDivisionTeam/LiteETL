import logging
import colorlog
from utils.connect import DUCKDB

class self_handler(logging.Handler):
    def __init__(self, name: str) -> None:
        logging.Handler.__init__(self)
        self.name = name
        self.cursor = DUCKDB.cursor()
        self.cursor.execute("CREATE SCHEMA IF NOT EXISTS logger")
        # 指定创建时间为默认时间戳，id自动生成
        self.cursor.execute(
            f'''
            CREATE TABLE IF NOT EXISTS logger.{self.name} (
                log_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
                level VARCHAR, 
                msg VARCHAR
            )
            '''
        )

    def emit(self, record) -> None:
        try:
            msg = self.format(record)
            temp_msg = msg.split(":")
            level = temp_msg[0]
            msg = ":".join(temp_msg[1:])
            self.cursor.execute(f"INSERT INTO logger.{self.name} (level, msg) VALUES (?, ?)", [level, msg])
        except Exception:
            self.handleError(record)


def make_logger(logger_name: str, table_name: str)-> logging.Logger:
    """生成日志的工厂方法"""
    temp_log = logging.getLogger(logger_name)
    temp_log.setLevel(logging.DEBUG)
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    console.setFormatter(
        colorlog.ColoredFormatter(
            '%(log_color)s%(levelname)s: %(asctime)s %(message)s',
            log_colors={
                'DEBUG': 'cyan',
                'INFO': 'green',
                'WARNING': 'yellow',
                'ERROR': 'red',
                'CRITICAL': 'red,bg_white',
            },
            datefmt='## %Y-%m-%d %H:%M:%S'
        ))
    temp_log.addHandler(console)
    mongoio = duckdb_handler(table_name)
    mongoio.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(levelname)s:%(message)s')
    mongoio.setFormatter(formatter)
    temp_log.addHandler(mongoio)
    return temp_log
