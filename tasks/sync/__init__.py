from collections.abc import Callable
from utils.connect import connect_data
from tasks.base import task

def init_warpper(func: Callable[[connect_data], list[task]]):
    def wrapper(connect_data: connect_data) -> dict[str, task]:
        # 调用原始函数并获取返回值
        result: list[task] = func(connect_data)
        # 自动化将列表转为字典
        keys = [ch.name for ch in result]
        return  dict(zip(keys, result))
    return wrapper
