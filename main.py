import time
from utils.connect import make_coonect
from tasks.executer import executer
from tasks.init import task_init


if __name__ == "__main__":
    while True:
        try:
            connect_data = make_coonect()
            runner = executer(connect_data.duckdb)
            runner.run(task_init(connect_data))
        except Exception as e:
            print(f"主程序出错: {e}")
        print("运行完成，等待10分钟重新启动ETL负载")
        time.sleep(600)