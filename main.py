import time
from tasks.scheduler import SCHEDULER
from tasks.init import task_init
        
if __name__ == "__main__":
    SCHEDULER.append(task_init())
    while True:
        if SCHEDULER.is_runed():
            break
        else:
            time.sleep(1)