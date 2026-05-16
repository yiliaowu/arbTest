@echo off
chcp 65001 > nul
setlocal
set "ROOT=%~dp0"
:: 优化：使用系统环境变量中的 python，方便分享给别人直接克隆运行
set "PY=python"
set "LOGDIR=%ROOT%logs"
if not exist "%LOGDIR%" mkdir "%LOGDIR%"

echo =======================================
echo    LOF 基金套利系统 - 一键启动程序
echo =======================================
echo.

where %PY% >nul 2>nul
if %ERRORLEVEL% neq 0 (
    echo [错误] 找不到 Python 环境！请确保 Python 已安装并添加到了系统的 PATH 环境变量中。
    pause > nul
    exit /b 1
)

:: 注释掉此前的数据库存在性检查。
:: 新的Python代码会自动处理数据库的创建。
echo [系统] 检查数据库... 如果数据库不存在，程序将自动创建。

echo [清理] 正在结束可能残留的后台进程，彻底释放 5000 端口...
echo (提示：此操作会结束所有名为 python.exe 的进程)
taskkill /f /im python.exe > nul 2>&1

set PYTHONIOENCODING=utf-8

echo [0/6] 正在获取今日最新基础数据 (011)...
echo (这一步需要10-30秒，请稍候...)
"%PY%" -X utf8 LOF011_daily_updater.py
if errorlevel 1 (
    echo [错误] 011 数据更新脚本执行失败，请检查程序逻辑或数据库连接。
    pause > nul
    exit /b 1
)
echo 011 执行完毕！

echo [1/6] 正在获取今日LOF历史数据 (012)...
echo (这一步需要10-30秒，请稍候...)
"%PY%" -X utf8 LOF012_calculate_static_valuation.py
if errorlevel 1 (
    echo [错误] 012 历史数据计算脚本执行失败，请检查程序逻辑或数据库连接。
    pause > nul
    exit /b 1
)
echo 012 执行完毕！

echo [2/6] 启动维护面板后台 (端口 5002)...
start "LOF Admin (5002)" /D "%ROOT%" cmd /k ""%PY%" -X utf8 LOF01_admin_launcher.py"

echo [3/6] 启动实时数据与行情服务 (端口 5000)...
start "LOF Backend (5000)" /D "%ROOT%" cmd /k ""%PY%" -X utf8 LOF02_fetch_trade_data.py"

echo 等待后台服务初始化及IB连接握手 (8秒)...
timeout /t 8 > nul

echo [4/6] 正在检查历史数据并生成今日监控报表...
echo (如果需要爬取新数据，这一步可能需要10-20秒，请稍候...)
pushd "%ROOT%"
"%PY%" -X utf8 LOF03_generate_monitor_html.py > "%LOGDIR%\html_generate.log" 2>&1
if errorlevel 1 (
    echo [错误] 03 报表生成脚本执行失败！请检查日志文件获取详细错误信息:
    echo %LOGDIR%\html_generate.log
    pause > nul
    exit /b 1
)
popd
echo 报表生成完毕！

echo [5/6] 自动打开浏览器...
start "" "http://localhost:5000/"

echo.
echo =======================================
echo 系统已全部启动完毕！
echo 监控看板地址: http://localhost:5000/
echo 维护后台地址: http://localhost:5002/
echo.
echo (提示：已自动运行 011 和 012 获取今日最新数据)
echo (提示：后台服务已在隐藏窗口运行，关闭此黑色窗口不会影响系统运行。)
echo =======================================
pause > nul
endlocal