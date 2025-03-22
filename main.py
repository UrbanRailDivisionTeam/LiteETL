from tasks.scheduler import SCHEDULER
from tasks.init import task_init
        
if __name__ == "__main__":
    SCHEDULER.append(task_init())