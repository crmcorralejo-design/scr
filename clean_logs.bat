@echo off
echo.
echo ================================================================
echo   CLEAN LOGS
echo ================================================================
echo.
echo [1] Delete logs older than 7 days
echo [2] Delete logs older than 30 days
echo [3] Delete ALL logs
echo [0] Cancel
echo.

set /p OPTION="Select option: "

if "%OPTION%"=="1" (
    forfiles /P logs /S /M *.log /D -7 /C "cmd /c del @path" 2>nul
    echo Logs older than 7 days deleted
)
if "%OPTION%"=="2" (
    forfiles /P logs /S /M *.log /D -30 /C "cmd /c del @path" 2>nul
    echo Logs older than 30 days deleted
)
if "%OPTION%"=="3" (
    del /Q logs\*.log 2>nul
    echo All logs deleted
)

echo.
pause
