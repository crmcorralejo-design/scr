@echo off
echo ============================================================
echo   BookingScraper Pro - Instalar Selenium + ChromeDriver
echo ============================================================

cd /d "%~dp0"

if not exist "venv\Scripts\python.exe" (
    echo [ERROR] Entorno virtual no encontrado.
    pause
    exit /b 1
)

echo.
echo [1/3] Instalando selenium...
venv\Scripts\pip.exe install selenium webdriver-manager --break-system-packages 2>nul || venv\Scripts\pip.exe install selenium webdriver-manager

echo.
echo [2/3] Verificando instalacion...
venv\Scripts\python.exe -c "import selenium; print('OK - selenium', selenium.__version__)"
venv\Scripts\python.exe -c "from webdriver_manager.chrome import ChromeDriverManager; print('OK - webdriver-manager')"

echo.
echo [3/3] Verificando Google Chrome...
reg query "HKLM\SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths\chrome.exe" >nul 2>&1
if %ERRORLEVEL% EQU 0 (
    echo     OK - Google Chrome encontrado en el sistema
) else (
    reg query "HKCU\SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths\chrome.exe" >nul 2>&1
    if %ERRORLEVEL% EQU 0 (
        echo     OK - Google Chrome encontrado en el sistema
    ) else (
        echo     AVISO: Chrome no detectado en registro
        echo     Si tienes Chrome instalado puedes ignorar este aviso.
        echo     Si no: descargalo en https://www.google.com/chrome/
    )
)

echo.
echo ============================================================
echo   Selenium listo. Ahora:
echo   1. Edita .env y cambia USE_SELENIUM=False por True
echo   2. Ejecuta: venv\Scripts\python.exe diagnostico_scraping.py
echo ============================================================
echo.
pause
