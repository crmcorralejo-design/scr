@echo off
setlocal

echo.
echo ================================================================
echo   CHECKING SYSTEM REQUIREMENTS
echo ================================================================
echo.

:: Check Python
echo [1/4] Checking Python...
python --version >nul 2>&1
if %errorlevel% equ 0 (
    for /f "tokens=2" %%i in ('python --version') do echo     [OK] Python %%i installed
) else (
    echo     [ERROR] Python not found
    echo     Download: https://www.python.org/downloads/
)
echo.

:: Check PostgreSQL
echo [2/4] Checking PostgreSQL...
where psql >nul 2>&1
if %errorlevel% equ 0 (
    for /f "tokens=3" %%i in ('psql --version') do echo     [OK] PostgreSQL %%i installed
) else (
    echo     [WARNING] PostgreSQL not in PATH
    if exist "C:\Program Files\PostgreSQL" (
        echo     [INFO] PostgreSQL found in default location
    ) else (
        echo     [ERROR] PostgreSQL not found
        echo     Download: https://www.postgresql.org/download/windows/
    )
)
echo.

:: Check Redis/Memurai
echo [3/4] Checking Redis/Memurai...
sc query Memurai >nul 2>&1
if %errorlevel% equ 0 (
    echo     [OK] Memurai service found
) else (
    sc query Redis >nul 2>&1
    if %errorlevel% equ 0 (
        echo     [OK] Redis service found
    ) else (
        echo     [WARNING] Redis/Memurai not found
        echo     Download Memurai: https://www.memurai.com/
    )
)
echo.

:: Check Git (optional)
echo [4/4] Checking Git (optional)...
where git >nul 2>&1
if %errorlevel% equ 0 (
    for /f "tokens=3" %%i in ('git --version') do echo     [OK] Git version %%i installed
) else (
    echo     [INFO] Git not installed (optional)
)
echo.

echo ================================================================
echo   REQUIREMENT CHECK COMPLETED
echo ================================================================
echo.
pause
endlocal
