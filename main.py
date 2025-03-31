from utils.connect import make_coonect
from tasks.scheduler import scheduler
from tasks.init import task_init
        
if __name__ == "__main__":
    connect_data = make_coonect()
    _scheduler = scheduler(connect_data.duckdb)
    tasks = task_init(connect_data)
    _scheduler.run(tasks)