@echo off
echo.
echo ================================================================
echo   STOPPING ALL SERVICES
echo ================================================================
echo.

taskkill /FI "WINDOWTITLE eq FastAPI*" /F >nul 2>&1
taskkill /FI "WINDOWTITLE eq Celery*" /F >nul 2>&1
taskkill /IM python.exe /F >nul 2>&1

echo [OK] All services stopped
echo.
pause
