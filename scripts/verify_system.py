
"""
BookingScraper/scripts/verify_system.py
Script de verificación completa del sistema
Booking Scraper Pro 
Windows 11 - Python 3.14.3
"""

import sys
import os
from pathlib import Path
import importlib

# Agregar al path
sys.path.append(str(Path(__file__).parent.parent))

def check_python_version():
    """Verificar versión de Python"""
    print("[1] Verificando Python...")
    version = sys.version_info
    print(f"    Python {version.major}.{version.minor}.{version.micro}")
    
    if version.major != 3 or version.minor < 11:
        print("    ✗ Se requiere Python 3.11 o superior")
        return False
    
    print("    ✓ Versión compatible")
    return True


def check_imports():
    """Verificar imports críticos"""
    print("\n[2] Verificando imports críticos...")
    
    modules = {
        'fastapi': 'FastAPI',
        'uvicorn': 'Uvicorn',
        'celery': 'Celery',
        'redis': 'Redis',
        'selenium': 'Selenium',
        'bs4': 'BeautifulSoup',
        'sqlalchemy': 'SQLAlchemy',
        'psycopg2': 'PostgreSQL driver',
        'PIL': 'Pillow',
        'pandas': 'Pandas',
        'loguru': 'Loguru',
        'dotenv': 'python-dotenv',
    }
    
    errors = []
    for module, name in modules.items():
        try:
            importlib.import_module(module)
            print(f"    ✓ {name}")
        except ImportError as e:
            print(f"    ✗ {name} - {e}")
            errors.append(module)
    
    if errors:
        print(f"\n    Faltan {len(errors)} módulos")
        return False
    
    return True


def check_project_structure():
    """Verificar estructura del proyecto"""
    print("\n[3] Verificando estructura del proyecto...")
    
    base_dir = Path("C:/BookingScraper")
    
    required_dirs = [
        "app",
        "app/core",
        "app/scraper",
        "app/tasks",
        "scripts",
        "data",
        "data/images",
        "data/exports",
        "data/logs",
        "logs",
        "venv",
    ]
    
    missing = []
    for directory in required_dirs:
        dir_path = base_dir / directory
        if dir_path.exists():
            print(f"    ✓ {directory}")
        else:
            print(f"    ✗ {directory} (no existe)")
            missing.append(directory)
    
    if missing:
        print(f"\n    Faltan {len(missing)} directorios")
        return False
    
    return True


def check_config_files():
    """Verificar archivos de configuración"""
    print("\n[4] Verificando archivos de configuración...")
    
    base_dir = Path("C:/BookingScraper")
    
    files = {
        ".env": "Configuración principal",
        "requirements_windows.txt": "Dependencias Python",
        "scripts/init_db.sql": "Script de base de datos",
    }
    
    missing = []
    for file, desc in files.items():
        file_path = base_dir / file
        if file_path.exists():
            print(f"    ✓ {file} - {desc}")
        else:
            print(f"    ✗ {file} - {desc} (no existe)")
            missing.append(file)
    
    if missing:
        print(f"\n    Faltan {len(missing)} archivos")
        return False
    
    return True


def check_database_connection():
    """Verificar conexión a base de datos"""
    print("\n[5] Verificando conexión a PostgreSQL...")
    
    try:
        from app.core.database import engine
        from sqlalchemy import text
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            print("    ✓ Conexión exitosa")
            return True
            
    except Exception as e:
        print(f"    ✗ Error de conexión: {e}")
        return False


def check_redis_connection():
    """Verificar conexión a Redis/Memurai"""
    print("\n[6] Verificando conexión a Redis/Memurai...")
    
    try:
        import redis
        from app.core.config import settings
        
        r = redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            decode_responses=True
        )
        
        r.ping()
        print("    ✓ Conexión exitosa")
        return True
        
    except Exception as e:
        print(f"    ✗ Error de conexión: {e}")
        return False


def check_database_tables():
    """Verificar tablas de la base de datos"""
    print("\n[7] Verificando tablas de la base de datos...")
    
    try:
        from app.core.database import engine
        from sqlalchemy import text, inspect
        
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        
        required_tables = ['url_queue', 'hotels']
        
        for table in required_tables:
            if table in tables:
                print(f"    ✓ Tabla '{table}' existe")
            else:
                print(f"    ✗ Tabla '{table}' no existe")
                return False
        
        return True
        
    except Exception as e:
        print(f"    ✗ Error verificando tablas: {e}")
        return False


def check_celery_config():
    """Verificar configuración de Celery"""
    print("\n[8] Verificando configuración de Celery...")
    
    try:
        from app.tasks.celery_app import app as celery_app
        
        print(f"    ✓ Celery configurado")
        print(f"    Broker: {celery_app.conf.broker_url}")
        print(f"    Backend: {celery_app.conf.result_backend}")
        return True
        
    except Exception as e:
        print(f"    ✗ Error: {e}")
        return False


def main():
    """Ejecutar todas las verificaciones"""
    
    print("="*70)
    print(" VERIFICACIÓN COMPLETA DEL SISTEMA")
    print(" Booking Scraper Pro - Windows 11")
    print("="*70)
    print()
    
    checks = [
        ("Versión de Python", check_python_version),
        ("Imports de módulos", check_imports),
        ("Estructura del proyecto", check_project_structure),
        ("Archivos de configuración", check_config_files),
        ("Conexión PostgreSQL", check_database_connection),
        ("Conexión Redis/Memurai", check_redis_connection),
        ("Tablas de base de datos", check_database_tables),
        ("Configuración Celery", check_celery_config),
    ]
    
    results = []
    
    for name, check_func in checks:
        try:
            result = check_func()
            results.append((name, result))
        except Exception as e:
            print(f"\n    ✗ Error inesperado: {e}")
            results.append((name, False))
    
    # Resumen
    print("\n" + "="*70)
    print(" RESUMEN")
    print("="*70)
    print()
    
    passed = sum(1 for _, r in results if r)
    total = len(results)
    
    for name, result in results:
        status = "✓" if result else "✗"
        print(f"  {status} {name}")
    
    print()
    print(f"Verificaciones: {passed}/{total} exitosas")
    
    if passed == total:
        print()
        print("="*70)
        print(" ✓ SISTEMA COMPLETAMENTE FUNCIONAL")
        print("="*70)
        return 0
    else:
        print()
        print("="*70)
        print(" ✗ SISTEMA TIENE PROBLEMAS")
        print("="*70)
        print()
        print("Revisar los errores arriba y corregir antes de usar el sistema")
        return 1


if __name__ == "__main__":
    sys.exit(main())
