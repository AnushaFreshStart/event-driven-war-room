@echo off
setlocal
cd /d "%~dp0"

echo ==============================================
echo   Stopping the Event-Driven War Room System
echo ==============================================

echo.
echo [1/3] Stopping stale app processes and resetting Redpanda...
Get-CimInstance Win32_Process |
  Where-Object { $_.Name -eq "python.exe" -and $_.ExecutablePath -like "*event-driven-war-room*" } |
  ForEach-Object { Stop-Process -Id $_.ProcessId -Force }

docker rm -f redpanda 2>nul

Set-Location "%~dp0src"
docker compose up -d

for /l %i in (1,1,30) do (
    docker exec redpanda rpk topic list --brokers localhost:9092 >nul 2>nul
    if not errorlevel 1 (
        echo Kafka ready
        goto topics
    )
    echo waiting for Kafka... %i
    timeout /t 2 /nobreak >nul
)
:topics

docker exec redpanda rpk topic create slack-inbound slack-outbound agent-context --brokers localhost:9092 --if-not-exists

docker exec redpanda rpk topic list --brokers localhost:9092

echo.
echo [2/3] Stopping Python agent processes...
for /f "skip=1 tokens=2" %%P in ('wmic process where "name like 'python.exe' and executablepath like '%%event-driven-war-room%%'" get processid 2^>nul') do (
    taskkill /PID %%P /F >nul 2>&1
)

for /f "skip=1 tokens=2" %%P in ('wmic process where "name like 'powershell.exe' and commandline like '%%event-driven-war-room%%'" get processid 2^>nul') do (
    taskkill /PID %%P /F >nul 2>&1
)

echo.
echo [3/3] Removing any leftover Kafka topics if needed...
if exist "%~dp0src\docker-compose.yml" (
    docker rm -f redpanda >nul 2>&1
    docker exec redpanda rpk topic delete slack-inbound slack-outbound agent-context --brokers localhost:9092 --ignore-unknown 2>nul
)

echo.
echo ==============================================
echo All processes and containers have been stopped.
echo ==============================================
pause
