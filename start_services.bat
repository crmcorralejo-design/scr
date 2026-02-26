@echo off
echo.
echo ================================================================
echo   STARTING ALL SERVICES
echo ================================================================
echo.

set "PROJECT_DIR=%~dp0"
set "VENV_DIR=%PROJECT_DIR%venv"

if not exist "%VENV_DIR%\Scripts\activate.bat" (
    echo [ERROR] Virtual environment not found
    echo Run: install_windows_final_v4.bat
    pause
    exit /b 1
)

echo [1/3] Starting FastAPI Server...
start "FastAPI Server" cmd /k "cd /d "%PROJECT_DIR%" && "%VENV_DIR%\Scripts\activate" && uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload"
timeout /t 2 >nul

echo [2/3] Starting Celery Worker...
start "Celery Worker" cmd /k "cd /d "%PROJECT_DIR%" && "%VENV_DIR%\Scripts\activate" && celery -A app.tasks.celery_app worker --pool=solo --loglevel=info"
timeout /t 2 >nul

echo [3/3] Starting Celery Beat...
start "Celery Beat" cmd /k "cd /d "%PROJECT_DIR%" && "%VENV_DIR%\Scripts\activate" && celery -A app.tasks.celery_app beat --loglevel=info"

echo.
echo ================================================================
echo   ALL SERVICES STARTED
echo ================================================================
echo.
echo   FastAPI:      http://localhost:8000
echo   API Docs:     http://localhost:8000/docs
echo   Redoc:        http://localhost:8000/redoc
echo.
echo   To stop services: detener_todo.bat or stop_services.bat
echo.
pause
