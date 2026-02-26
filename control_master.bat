@echo off
setlocal EnableDelayedExpansion

set "PROJECT_DIR=%~dp0"
set "VENV_DIR=%PROJECT_DIR%venv"
set "PYTHON=%VENV_DIR%\Scripts\python.exe"

:MENU
cls
echo.
echo ================================================================
echo   BOOKING SCRAPER PRO - CONTROL PANEL
echo ================================================================
echo.
echo   [1]  Start FastAPI Server
echo   [2]  Start Celery Worker
echo   [3]  Start Celery Beat
echo   [4]  Stop all services
echo.
echo   [5]  Load URLs from CSV
echo   [6]  View URL status
echo   [7]  View scraped hotels
echo   [8]  Export data
echo.
echo   [9]  Test PostgreSQL
echo   [10] Test Redis/Memurai
echo   [11] View logs
echo   [12] Complete diagnostic
echo.
echo   [13] Backup database
echo   [14] Restore database
echo.
echo   [0]  Exit
echo.
echo ================================================================

set /p OPCION="Select option: "

if "%OPCION%"=="1" goto START_API
if "%OPCION%"=="2" goto START_CELERY
if "%OPCION%"=="3" goto START_BEAT
if "%OPCION%"=="4" goto STOP_ALL
if "%OPCION%"=="5" goto LOAD_URLS
if "%OPCION%"=="6" goto STATUS_URLS
if "%OPCION%"=="7" goto VIEW_HOTELS
if "%OPCION%"=="8" goto EXPORT_DATA
if "%OPCION%"=="9" goto TEST_DB
if "%OPCION%"=="10" goto TEST_REDIS
if "%OPCION%"=="11" goto VIEW_LOGS
if "%OPCION%"=="12" goto DIAGNOSTICO
if "%OPCION%"=="13" goto BACKUP
if "%OPCION%"=="14" goto RESTORE
if "%OPCION%"=="0" exit /b 0

echo Invalid option
pause
goto MENU

:START_API
start "FastAPI" cmd /k "cd /d "%PROJECT_DIR%" && "%VENV_DIR%\Scripts\activate" && uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload"
echo FastAPI started
pause
goto MENU

:START_CELERY
start "Celery Worker" cmd /k "cd /d "%PROJECT_DIR%" && "%VENV_DIR%\Scripts\activate" && celery -A app.tasks.celery_app worker --pool=solo --loglevel=info"
echo Celery Worker started
pause
goto MENU

:START_BEAT
start "Celery Beat" cmd /k "cd /d "%PROJECT_DIR%" && "%VENV_DIR%\Scripts\activate" && celery -A app.tasks.celery_app beat --loglevel=info"
echo Celery Beat started
pause
goto MENU

:STOP_ALL
taskkill /FI "WINDOWTITLE eq FastAPI*" /F >nul 2>&1
taskkill /FI "WINDOWTITLE eq Celery*" /F >nul 2>&1
echo Services stopped
pause
goto MENU

:LOAD_URLS
set /p CSV="CSV file path: "
if exist "%CSV%" "%PYTHON%" scripts\load_urls.py "%CSV%"
pause
goto MENU

:STATUS_URLS
"%PYTHON%" -c "from app.core.database import SessionLocal; from app.models.models import URLQueue; db=SessionLocal(); print('Pending:', db.query(URLQueue).filter_by(status='pending').count()); db.close()"
pause
goto MENU

:VIEW_HOTELS
"%PYTHON%" -c "from app.core.database import SessionLocal; from app.models.models import Hotel; db=SessionLocal(); print('Hotels:', db.query(Hotel).count()); db.close()"
pause
goto MENU

:EXPORT_DATA
"%PYTHON%" scripts\export_data.py
pause
goto MENU

:TEST_DB
"%PYTHON%" scripts\test_db.py
pause
goto MENU

:TEST_REDIS
"%PYTHON%" scripts\test_memurai.py
pause
goto MENU

:VIEW_LOGS
if exist "logs\app.log" type logs\app.log
pause
goto MENU

:DIAGNOSTICO
"%PYTHON%" scripts\diagnostico.py
pause
goto MENU

:BACKUP
call backup_db.bat
pause
goto MENU

:RESTORE
call restaurar_db.bat
pause
goto MENU
