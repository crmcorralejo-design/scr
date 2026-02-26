@echo off
echo.
echo ================================================================
echo   STOPPING ALL SERVICES
echo ================================================================
echo.

echo Stopping FastAPI...
taskkill /FI "WINDOWTITLE eq FastAPI*" /F >nul 2>&1
if %errorlevel% equ 0 (
    echo [OK] FastAPI stopped
) else (
    echo [INFO] FastAPI was not running
)

echo.
echo Stopping Celery Worker...
taskkill /FI "WINDOWTITLE eq Celery Worker*" /F >nul 2>&1
if %errorlevel% equ 0 (
    echo [OK] Celery Worker stopped
) else (
    echo [INFO] Celery Worker was not running
)

echo.
echo Stopping Celery Beat...
taskkill /FI "WINDOWTITLE eq Celery Beat*" /F >nul 2>&1
if %errorlevel% equ 0 (
    echo [OK] Celery Beat stopped
) else (
    echo [INFO] Celery Beat was not running
)

echo.
echo ================================================================
echo   ALL SERVICES STOPPED
echo ================================================================
echo.
pause
