@echo off
echo ============================================================
echo   BookingScraper Pro - Instalar cloudscraper
echo ============================================================

cd /d "%~dp0"

if not exist "venv\Scripts\python.exe" (
    echo [ERROR] Entorno virtual no encontrado.
    echo         Ejecuta primero el instalador principal.
    pause
    exit /b 1
)

echo.
echo [1/2] Instalando cloudscraper...
venv\Scripts\pip.exe install cloudscraper --break-system-packages
if %ERRORLEVEL% NEQ 0 (
    venv\Scripts\pip.exe install cloudscraper
)

echo.
echo [2/2] Verificando instalacion...
venv\Scripts\python.exe -c "import cloudscraper; print('OK - cloudscraper', cloudscraper.__version__)"

echo.
echo ============================================================
echo   Ahora ejecuta:
echo   venv\Scripts\python.exe diagnostico_scraping.py
echo ============================================================
echo.
pause
