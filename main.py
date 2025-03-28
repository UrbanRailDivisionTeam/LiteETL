from tasks.scheduler import SCHEDULER
from tasks.init import task_init
        
if __name__ == "__main__":
    SCHEDULER.run(task_init())