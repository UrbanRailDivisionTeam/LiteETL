import os
from utils import read_sql
from utils.config import DEBUG
from utils.connect import connect_data
from tasks.sync import init_warpper
from tasks.base import task, extract_increase, extract_increase_data
from tasks.process.error import error_process


@init_warpper
def init(connect_data: connect_data) -> list[task]:
    return [
        error_process(connect_data)
    ]