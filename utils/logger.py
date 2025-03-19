import logging
import traceback
import colorlog
from sqlalchemy import text
from connect import CONNECTER


class self_handler(logging.Handler):
    def __init__(self, name: str) -> None:
        logging.Handler.__init__(self)
        self.name = name
        self.connect = CONNECTER.get("clickhouse服务")
        try:
            self.connect.execute(text(f"CREATE DATABASE IF NOT EXISTS logger"))
            self.connect.execute(text(f"USE logger"))
            self.connect.execute(text(
                f"""
                    CREATE TABLE IF NOT EXISTS {name}
                    (
                        `id` UUID DEFAULT generateUUIDv4(), 
                        `m_time` DateTime, 
                        `m_level` String, 
                        `m_message` String
                    )
                    ENGINE = MergeTree()
                    ORDER BY m_time
                    SETTINGS index_granularity = 8192;
                """
            ))
            self.connect.commit()
        except Exception as e:
            print(e)
            print(str(traceback.format_exc()))
        finally:
            self.connect.rollback()
            self.connect.close()

    def emit(self, record) -> None:
        try:
            msg = self.format(record)
            temp_msg = msg.split(":")
            level = temp_msg[0]
            msg = ":".join(temp_msg[1:])
            self.connect.execute(text(
                f"""
                    INSERT INTO logger.{self.name} (m_time, m_level, m_message)
                    VALUES
                    (
                        now(), 
                        '{level}', 
                        '{msg}'
                    );
                """
            ))
            self.connect.commit()
        except Exception:
            self.connect.rollback()
            self.connect.close()
            self.handleError(record)

    def __del__(self) -> None:
        self.connect.close()


def make_logger(logger_name: str) -> logging.Logger:
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
    mongoio = self_handler(logger_name)
    mongoio.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(levelname)s:%(message)s')
    mongoio.setFormatter(formatter)
    temp_log.addHandler(mongoio)
    return temp_log
