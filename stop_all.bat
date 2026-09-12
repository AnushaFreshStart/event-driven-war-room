@echo off
setlocal
cd /d "%~dp0"

echo ==============================================
echo   Stopping the Event-Driven War Room System
echo ==============================================

docker info >nul 2>&1
if errorlevel 1 (
    echo WARNING: Docker is not running; continuing with local process cleanup only.
) else (
    echo.
    echo [1/3] Stopping Docker containers...
    docker stop redpanda >nul 2>&1
    docker rm -f redpanda >nul 2>&1
)

echo.
echo [2/3] Stopping Python agent processes...
for /f "skip=1 tokens=2" %%P in ('wmic process where "name like 'python.exe'" get processid 2^>nul') do (
    taskkill /PID %%P /F >nul 2>&1
)

echo.
echo [3/3] Removing any leftover Kafka topics if needed...
if exist "%~dp0src\docker-compose.yml" (
    docker exec redpanda rpk topic delete slack-inbound slack-outbound agent-context --brokers localhost:9092 --ignore-unknown 2>nul
)

echo.
echo ==============================================
echo All processes and containers have been stopped.
echo ==============================================
pause
