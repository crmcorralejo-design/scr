@echo off
setlocal

set "VENV_DIR=%~dp0venv"
set "PIP=%VENV_DIR%\Scripts\pip.exe"

echo.
echo ================================================================
echo   UPDATE VIRTUAL ENVIRONMENT
echo ================================================================
echo.
echo [1] Update pip, setuptools, wheel
echo [2] Update all packages
echo [3] Reinstall from requirements.txt
echo [4] Full update (1+2)
echo [0] Cancel
echo.

set /p OPTION="Select option: "

if "%OPTION%"=="1" (
    "%PIP%" install --upgrade pip setuptools wheel
)
if "%OPTION%"=="2" (
    "%PIP%" list --outdated --format=freeze | findstr /V "^-e" > outdated.txt
    for /f "delims==" %%i in (outdated.txt) do "%PIP%" install --upgrade %%i
    del outdated.txt
)
if "%OPTION%"=="3" (
    "%PIP%" install -r requirements.txt --upgrade
)
if "%OPTION%"=="4" (
    "%PIP%" install --upgrade pip setuptools wheel
    "%PIP%" install -r requirements.txt --upgrade
)

echo.
echo [OK] Update completed
echo.
pause
endlocal
