@echo off
echo ============================================================
echo   BookingScraper Pro - Instalar EdgeDriver (Windows 11)
echo ============================================================

cd /d "%~dp0"

echo.
echo [1/2] Instalando webdriver-manager para Edge...
venv\Scripts\pip.exe install webdriver-manager --upgrade

echo.
echo [2/2] Verificando Microsoft Edge...
reg query "HKLM\SOFTWARE\Microsoft\EdgeUpdate\Clients\{56EB18F8-B008-4CBD-B6D2-8C97FE7E9062}" >nul 2>&1
if %ERRORLEVEL% EQU 0 (
    echo     OK - Microsoft Edge detectado
) else (
    echo     AVISO: Edge no detectado via registro
    echo     Si tienes Edge instalado puedes ignorar este aviso.
    echo     Verifica abriendo: microsoft-edge://settings/
)

echo.
echo ============================================================
echo   Ahora ejecuta:
echo   venv\Scripts\python.exe diagnostico_scraping.py
echo ============================================================
echo.
pause
