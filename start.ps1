# 获取当前脚本的绝对路径
$scriptPath = Split-Path -Parent $MyInvocation.MyCommand.Path

# 定义启动命令
$command1 = @"
Set-Location -Path 'C:\Users\Administrator\Desktop\LiteETL\.venv\Scripts'
.\activate.ps1
Set-Location -Path ..\..\
python main.py
"@

$command2 = @"
Set-Location -Path 'C:\Users\Administrator\Desktop\LiteETL\.venv\Scripts'
.\activate.ps1
Set-Location -Path ..\..\
python server.py
"@

# 启动第一个PowerShell窗口运行main.py
Start-Process powershell -ArgumentList "-NoExit", "-Command", $command1

# 启动第二个PowerShell窗口运行server.py
Start-Process powershell -ArgumentList "-NoExit", "-Command", $command2

Write-Host "已启动两个PowerShell窗口"
python main.py
python server.py