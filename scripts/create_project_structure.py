
"""
BookingScraper/scripts/create_project_structure.py
Script para crear la estructura completa del proyecto
Booking Scraper Pro 
Windows 11 - Python 3.14.3
"""

import os
from pathlib import Path

def create_structure():
    """Crea toda la estructura de directorios del proyecto"""
    
    # Directorio base
    base_dir = Path("C:/BookingScraper")
    
    # Estructura de directorios
    directories = [
        # App principal
        "app",
        "app/core",
        "app/scraper",
        "app/models",
        "app/api",
        "app/api/routes",
        "app/api/dependencies",
        "app/tasks",
        
        # Scripts
        "scripts",
        "scripts/migrations",
        "scripts/backups",
        
        # Data
        "data",
        "data/images",
        "data/images/hotels",
        "data/images/rooms",
        "data/exports",
        "data/exports/csv",
        "data/exports/json",
        "data/exports/xlsx",
        "data/logs",
        "data/temp",
        
        # Logs
        "logs",
        "logs/api",
        "logs/celery",
        "logs/scraper",
        
        # Backups
        "backups",
        "backups/database",
        "backups/configs",
        
        # Documentación
        "docs",
        "docs/guides",
        "docs/api",
        
        # Tests
        "tests",
        "tests/unit",
        "tests/integration",
        
        # Configuración
        "config",
    ]
    
    # Crear directorios
    print("Creando estructura de directorios...")
    for directory in directories:
        dir_path = base_dir / directory
        dir_path.mkdir(parents=True, exist_ok=True)
        print(f"  ✓ {directory}")
    
    # Crear archivos __init__.py
    init_dirs = [
        "app",
        "app/core",
        "app/scraper",
        "app/models",
        "app/api",
        "app/api/routes",
        "app/api/dependencies",
        "app/tasks",
        "tests",
        "tests/unit",
        "tests/integration",
    ]
    
    print("\nCreando archivos __init__.py...")
    for directory in init_dirs:
        init_file = base_dir / directory / "__init__.py"
        if not init_file.exists():
            init_file.write_text(f'"""{directory.replace("/", ".")} package"""\n')
            print(f"  ✓ {directory}/__init__.py")
    
    # Crear archivos .gitkeep en directorios vacíos
    gitkeep_dirs = [
        "data/images/hotels",
        "data/images/rooms",
        "data/exports/csv",
        "data/exports/json",
        "data/exports/xlsx",
        "data/temp",
        "logs/api",
        "logs/celery",
        "logs/scraper",
        "backups/database",
        "backups/configs",
    ]
    
    print("\nCreando archivos .gitkeep...")
    for directory in gitkeep_dirs:
        gitkeep_file = base_dir / directory / ".gitkeep"
        gitkeep_file.write_text("")
        print(f"  ✓ {directory}/.gitkeep")
    
    print("\n" + "="*60)
    print("✓ Estructura completa creada en:", base_dir)
    print("="*60)
    print("\nDirectorios creados:", len(directories))
    print("Archivos __init__.py:", len(init_dirs))
    print("Archivos .gitkeep:", len(gitkeep_dirs))


if __name__ == "__main__":
    create_structure()
