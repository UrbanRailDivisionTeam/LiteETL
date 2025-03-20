import uvicorn
import multiprocessing as mp

def process_etl() -> None:
    from tasks.scheduler import SCHEDULER
    from tasks.init import task_init
    SCHEDULER.append(task_init())
    
def process_web() -> None:
    from web.back.app import app
    uvicorn.run(app=app, host="0.0.0.0", port=5001, reload=True)

# if __name__ == "__main__":
#     p_etl = mp.Process(target=process_etl)
#     p_web = mp.Process(target=process_web)
#     process_list = [p_etl, p_web]
    
#     for ch in process_list:
#         ch.start()
        
#     for ch in process_list:
#         ch.join()
        
if __name__ == "__main__":
    process_etl()