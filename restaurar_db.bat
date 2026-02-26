@echo off
setlocal EnableDelayedExpansion

echo.
echo ================================================================
echo   POSTGRESQL DATABASE RESTORE
echo ================================================================
echo.

set PG_PATH=
if exist "C:\Program Files\PostgreSQL\18\bin\psql.exe" (
    set "PG_PATH=C:\Program Files\PostgreSQL\18\bin"
) else if exist "C:\Program Files\PostgreSQL\17\bin\psql.exe" (
    set "PG_PATH=C:\Program Files\PostgreSQL\17\bin"
) else if exist "C:\Program Files\PostgreSQL\16\bin\psql.exe" (
    set "PG_PATH=C:\Program Files\PostgreSQL\16\bin"
) else if exist "C:\Program Files\PostgreSQL\15\bin\psql.exe" (
    set "PG_PATH=C:\Program Files\PostgreSQL\15\bin"
) else (
    echo [ERROR] PostgreSQL not found
    pause
    exit /b 1
)

if not exist "backups" (
    echo [ERROR] Backups directory does not exist
    pause
    exit /b 1
)

echo Available backups:
echo.
set COUNT=0
for %%F in (backups\backup_*.sql) do (
    set /a COUNT+=1
    echo [!COUNT!] %%~nxF
    set "FILE!COUNT!=%%F"
)

if %COUNT%==0 (
    echo No backups available
    pause
    exit /b 1
)

echo.
set /p CHOICE="Select backup (1-%COUNT%): "

set "BACKUP_FILE=!FILE%CHOICE%!"

echo.
echo WARNING: This will delete all current data
set /p CONFIRM="Continue? (Y/N): "

if /i not "%CONFIRM%"=="Y" (
    echo Cancelled
    pause
    exit /b 0
)

set /p PG_PASSWORD="PostgreSQL password for user postgres: "

set PGPASSWORD=%PG_PASSWORD%
"%PG_PATH%\dropdb" -U postgres --if-exists booking_scraper
"%PG_PATH%\createdb" -U postgres booking_scraper
"%PG_PATH%\psql" -U postgres -d booking_scraper -f "%BACKUP_FILE%"

if %errorlevel% equ 0 (
    echo.
    echo [OK] Database restored
    echo.
) else (
    echo.
    echo [ERROR] Restore failed
    echo.
)

pause
endlocal
