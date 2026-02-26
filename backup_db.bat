@echo off
setlocal

echo.
echo ================================================================
echo   POSTGRESQL DATABASE BACKUP
echo ================================================================
echo.

set PG_PATH=
if exist "C:\Program Files\PostgreSQL\18\bin\pg_dump.exe" (
    set "PG_PATH=C:\Program Files\PostgreSQL\18\bin"
) else if exist "C:\Program Files\PostgreSQL\17\bin\pg_dump.exe" (
    set "PG_PATH=C:\Program Files\PostgreSQL\17\bin"
) else if exist "C:\Program Files\PostgreSQL\16\bin\pg_dump.exe" (
    set "PG_PATH=C:\Program Files\PostgreSQL\16\bin"
) else if exist "C:\Program Files\PostgreSQL\15\bin\pg_dump.exe" (
    set "PG_PATH=C:\Program Files\PostgreSQL\15\bin"
) else (
    echo [ERROR] PostgreSQL not found
    pause
    exit /b 1
)

echo PostgreSQL found: %PG_PATH%
echo.

if not exist "backups" mkdir backups

for /f "tokens=2 delims==" %%I in ('wmic os get localdatetime /value') do set datetime=%%I
set TIMESTAMP=%datetime:~0,8%_%datetime:~8,6%
set BACKUP_FILE=backups\backup_%TIMESTAMP%.sql

set /p PG_PASSWORD="PostgreSQL password for user postgres: "

set PGPASSWORD=%PG_PASSWORD%
"%PG_PATH%\pg_dump" -U postgres -d booking_scraper -F p -f "%BACKUP_FILE%"

if %errorlevel% equ 0 (
    echo.
    echo [OK] Backup completed: %BACKUP_FILE%
    echo.
) else (
    echo.
    echo [ERROR] Backup failed
    echo.
)

pause
endlocal
