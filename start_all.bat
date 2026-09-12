@echo off
setlocal
cd /d "%~dp0"

set "ROOT=%~dp0"
set "SRC=%ROOT%src"
set "VENV=%ROOT%.venv"

echo ==============================================
echo   Starting the Event-Driven War Room System
echo ==============================================

if not exist "%SRC%\docker-compose.yml" (
    echo ERROR: Docker compose file not found at "%SRC%\docker-compose.yml"
    pause
    exit /b 1
)

if not exist "%VENV%\Scripts\python.exe" (
    echo ERROR: Virtual environment not found at "%VENV%\Scripts\python.exe"
    echo Please run: python -m venv .venv
    pause
    exit /b 1
)

if not exist "%ROOT%.env" (
    echo WARNING: .env file not found at "%ROOT%.env"
    echo Some services may still run in mock mode, but Slack keys are expected there.
)

docker info >nul 2>&1
if errorlevel 1 (
    echo ERROR: Docker is not running or not installed.
    pause
    exit /b 1
)

echo.
echo [1/6] Removing stale Redpanda container if it still exists...
docker rm -f redpanda >nul 2>&1

echo.
echo [2/6] Starting Redpanda (Kafka) Broker...
cd /d "%SRC%"
docker-compose up -d
cd /d "%ROOT%"

if errorlevel 1 (
    echo ERROR: Failed to start the Redpanda container.
    pause
    exit /b 1
)

echo.
echo Waiting 5 seconds for Redpanda to initialize...
timeout /t 5 /nobreak > NUL

echo.
echo [3/6] Creating Kafka Topics...
docker exec redpanda rpk topic create slack-inbound slack-outbound agent-context --brokers localhost:9092 --if-not-exists
if errorlevel 1 (
    echo ERROR: Kafka topics could not be created. Check that Redpanda is healthy.
    pause
    exit /b 1
)

echo.
echo [4/6] Starting Slack Gateway...
start "Slack Gateway" /D "%SRC%" "%VENV%\Scripts\python.exe" "slack_gateway.py"

echo [5/6] Starting Scout Agent...
start "Scout Agent" /D "%SRC%" "%VENV%\Scripts\python.exe" "scout_agent.py"

echo [6/6] Starting Diagnoser Agent...
start "Diagnoser Agent" /D "%SRC%" "%VENV%\Scripts\python.exe" "diagnoser_agent.py"
start "Executive Agent" /D "%SRC%" "%VENV%\Scripts\python.exe" "executive_agent.py"

echo.
echo ==============================================
echo All agents successfully launched in background windows!
echo You can now go to your Slack Workspace to test it.
echo ==============================================
pause
