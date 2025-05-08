# 获取当前脚本所在目录
$currentPath = $PSScriptRoot

# 设置要打包的文件和文件夹
$itemsToPack = @(
    "source",
    "tasks",
    "utils",
    "main.py"
)

# 设置输出的zip文件名（使用当前时间戳）
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$zipFileName = "etl_package_$timestamp.zip"
$zipFilePath = Join-Path $currentPath $zipFileName

# 检查项目是否存在
foreach ($item in $itemsToPack) {
    $itemPath = Join-Path $currentPath $item
    if (-not (Test-Path $itemPath)) {
        Write-Host "警告: 找不到 $item" -ForegroundColor Yellow
    }
}

# 创建临时目录
$tempDir = Join-Path $currentPath "temp_for_zip"
New-Item -ItemType Directory -Path $tempDir -Force | Out-Null

# 复制文件到临时目录
foreach ($item in $itemsToPack) {
    $sourcePath = Join-Path $currentPath $item
    $destPath = Join-Path $tempDir $item
    if (Test-Path $sourcePath) {
        Copy-Item -Path $sourcePath -Destination $destPath -Recurse -Force
    }
}

# 创建zip文件
Compress-Archive -Path "$tempDir\*" -DestinationPath $zipFilePath -Force

# 清理临时目录
Remove-Item -Path $tempDir -Recurse -Force

Write-Host "打包完成！文件保存在: $zipFilePath" -ForegroundColor Green