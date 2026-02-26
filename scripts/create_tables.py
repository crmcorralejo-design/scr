
"""
BookingScraper/scripts/create_tables.py
Script para crear todas las tablas de la base de datos
usando los modelos de SQLAlchemy
Windows 11 - Python 3.14.3
"""

import sys
from pathlib import Path

# Agregar directorio raíz al path
sys.path.append(str(Path(__file__).parent.parent))

from app.core.database import engine
from app.models.models import Base, URLQueue, Hotel, ScrapingLog, VPNRotation, SystemMetrics

def create_all_tables():
    """Crea todas las tablas definidas en los modelos"""
    
    print("="*60)
    print("CREANDO TABLAS EN LA BASE DE DATOS")
    print("="*60)
    print()
    
    try:
        print("Conectando a la base de datos...")
        print(f"Engine: {engine.url}")
        print()
        
        print("Creando tablas...")
        Base.metadata.create_all(bind=engine)
        
        print()
        print("✓ Tablas creadas exitosamente:")
        for table_name in Base.metadata.tables.keys():
            print(f"  ✓ {table_name}")
        
        print()
        print("="*60)
        print("COMPLETADO")
        print("="*60)
        
    except Exception as e:
        print()
        print("="*60)
        print("ERROR AL CREAR TABLAS")
        print("="*60)
        print(f"Error: {e}")
        print()
        print("Verificar:")
        print("  - PostgreSQL está corriendo")
        print("  - Base de datos 'booking_scraper' existe")
        print("  - Credenciales en .env son correctas")
        print("  - Usuario tiene permisos")
        sys.exit(1)


def drop_all_tables():
    """PELIGRO: Elimina todas las tablas"""
    
    import sys
    
    print()
    print("⚠️  ADVERTENCIA ⚠️")
    print("="*60)
    print("Esto eliminará TODAS las tablas y TODOS los datos")
    print("="*60)
    print()
    
    confirm = input("Escribe 'CONFIRMAR' para continuar: ")
    
    if confirm != "CONFIRMAR":
        print("Operación cancelada")
        sys.exit(0)
    
    print()
    print("Eliminando tablas...")
    
    try:
        Base.metadata.drop_all(bind=engine)
        print("✓ Todas las tablas eliminadas")
        
    except Exception as e:
        print(f"✗ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Gestión de tablas de la base de datos")
    parser.add_argument(
        '--drop',
        action='store_true',
        help='Eliminar todas las tablas (PELIGROSO)'
    )
    
    args = parser.parse_args()
    
    if args.drop:
        drop_all_tables()
        print()
        input("Presiona Enter para crear las tablas nuevamente...")
        create_all_tables()
    else:
        create_all_tables()
