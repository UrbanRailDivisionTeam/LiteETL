import os
import pandas as pd

from tasks.process import process
from utils.config import CONFIG
from utils.connect import MONGO

class person(process):
    def __init__(self) -> None:
        super().__init__("SHR人员信息处理", "person_process")
    
    def task_main(self) -> None:
        ...