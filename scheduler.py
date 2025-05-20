import subprocess
import os
import time

if __name__ == "__main__":
    # 设置要运行的命令和间隔时间
    command_to_run = f"cd {os.path.dirname(os.path.abspath(__file__))} && .\\.venv\\Scripts\\activate.bat && python main.py"
    interval_seconds = 300  # 5 分钟
    while True:
        # 运行命令
        subprocess.Popen(['start', 'cmd', '/c', command_to_run], shell=True)
        # 等待指定的间隔时间
        print(f"已启动新命令行窗口运行命令：{command_to_run}")
        print(f"等待 {interval_seconds} 秒后再次运行...")
        time.sleep(interval_seconds)