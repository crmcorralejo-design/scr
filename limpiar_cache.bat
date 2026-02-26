@echo off
chcp 65001 > nul
title Limpiar Cache - BookingScraper Pro

echo.
echo ============================================================
echo   BookingScraper Pro - Limpieza de Cache
echo ============================================================
echo.

set ROOT=C:\BookingScraper

echo Eliminando carpetas __pycache__...
echo.

REM -- app/__pycache__
if exist "%ROOT%\app\__pycache__" (
    rmdir /s /q "%ROOT%\app\__pycache__"
    echo   [OK] app\__pycache__
) else (
    echo   [--] app\__pycache__ no existe
)

REM -- scripts/__pycache__
if exist "%ROOT%\scripts\__pycache__" (
    rmdir /s /q "%ROOT%\scripts\__pycache__"
    echo   [OK] scripts\__pycache__
) else (
    echo   [--] scripts\__pycache__ no existe
)

REM -- raiz/__pycache__ (por si acaso)
if exist "%ROOT%\__pycache__" (
    rmdir /s /q "%ROOT%\__pycache__"
    echo   [OK] \__pycache__ (raiz)
) else (
    echo   [--] \__pycache__ (raiz) no existe
)

REM -- Eliminar todos los .pyc sueltos de forma recursiva
echo.
echo Eliminando archivos .pyc sueltos...
for /r "%ROOT%" %%f in (*.pyc) do (
    del /q "%%f" 2>nul
    echo   [OK] %%f
)

REM -- Eliminar carpetas __pycache__ en subdirectorios (recursivo)
echo.
echo Revisando subdirectorios adicionales...
for /d /r "%ROOT%" %%d in (__pycache__) do (
    if exist "%%d" (
        rmdir /s /q "%%d"
        echo   [OK] %%d
    )
)

echo.
echo ============================================================
echo   Cache eliminada correctamente
echo ============================================================
echo.
echo Ahora puedes iniciar la aplicacion con: inicio_rapido.bat
echo.
pause
