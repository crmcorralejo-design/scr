@echo off
chcp 65001 > nul
title BookingScraper Pro v2.0 - Inicio

set ROOT=C:\BookingScraper
set VENV=%ROOT%\venv\Scripts

echo.
echo ============================================================
echo   BookingScraper Pro v2.0 - Inicio Rapido
echo   MODO SIMPLIFICADO: Solo uvicorn (sin Celery necesario)
echo   El scraping corre automaticamente dentro de FastAPI
echo ============================================================
echo.

REM -- PASO 1: Limpiar cache Python
echo [1/3] Limpiando cache Python...
for /d /r "%ROOT%" %%d in (__pycache__) do (
    if exist "%%d" rmdir /s /q "%%d"
)
for /r "%ROOT%" %%f in (*.pyc) do del /q "%%f" 2>nul
echo   OK - cache limpia

REM -- PASO 2: Verificar venv
echo.
echo [2/3] Verificando entorno virtual...
if not exist "%VENV%\python.exe" (
    echo.
    echo   ERROR: Entorno virtual no encontrado.
    echo   Ejecuta primero: instalar.bat
    echo.
    pause
    exit /b 1
)
echo   OK

REM -- PASO 3: Arrancar FastAPI (todo en uno)
echo.
echo [3/3] Arrancando FastAPI + Auto-Scraper integrado...
echo.
echo ============================================================
echo   NO necesitas Celery ni ningun otro proceso.
echo.
echo   API:     http://localhost:8000
echo   Docs:    http://localhost:8000/docs
echo   Status:  http://localhost:8000/scraping/status
echo   Salud:   http://localhost:8000/health
echo.
echo   El scraping inicia automaticamente 5s despues de arrancar.
echo   Ver progreso en esta consola.
echo.
echo   Para parar: Ctrl+C
echo ============================================================
echo.

cd /d "%ROOT%"
"%VENV%\python.exe" -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

pause
