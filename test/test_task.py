# 手动指定模块的导入路径
import sys
import os
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

def main():
    print("任务运行测试")
    from tasks.base import task
    class temp_task(task):
        def __init__(self, name):
            super().__init__(name)
            
        def task_main(self):
            print("测试")

    temp = temp_task("测试")
    temp.run()

if __name__ == "__main__":
    main()
