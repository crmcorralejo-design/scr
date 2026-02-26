@echo off
setlocal

set "VENV_DIR=%~dp0venv"
set "PYTHON=%VENV_DIR%\Scripts\python.exe"

echo.
echo ================================================================
echo   TESTING REDIS/MEMURAI CONNECTION
echo ================================================================
echo.

"%PYTHON%" -c "import redis; r = redis.Redis(host='localhost', port=6379, decode_responses=True); r.ping(); print('[OK] Redis/Memurai connection successful'); r.set('test_key', 'test_value'); val = r.get('test_key'); print(f'[OK] Read/Write test: {val}'); r.delete('test_key')"

if %errorlevel% neq 0 (
    echo.
    echo [ERROR] Cannot connect to Redis/Memurai
    echo.
    echo Check that:
    echo 1. Memurai is installed
    echo 2. Memurai service is running
    echo 3. Port 6379 is available
    echo.
)

pause
endlocal
