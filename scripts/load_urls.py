
"""
BookingScraper/scripts/load_urls.py
Script para carga masiva de URLs a la base de datos
Soporta CSV con/sin encabezado y archivos TXT
Compatible con PostgreSQL 15 y 18
Windows 11 - Python 3.14.3
"""

import argparse
import csv
import sys
import os
import re
from pathlib import Path
from typing import List, Tuple

try:
    import psycopg2
    from psycopg2.extras import execute_batch
except ImportError:
    print("ERROR: psycopg2 no instalado")
    print("Instalar con: pip install psycopg2-binary")
    sys.exit(1)

try:
    from dotenv import load_dotenv
    load_dotenv()
except:
    pass  # dotenv es opcional

# =============================================================================
# CONFIGURACIÓN
# =============================================================================

# Leer configuración desde variables de entorno o usar defaults
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_NAME', 'booking_scraper'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', '')
}

# =============================================================================
# FUNCIONES DE VALIDACIÓN
# =============================================================================

def is_valid_booking_url(url: str) -> bool:
    """
    Valida que sea una URL válida de Booking.com
    
    Args:
        url: URL a validar
        
    Returns:
        bool: True si es válida
    """
    if not url or not isinstance(url, str):
        return False
    
    url = url.strip()
    
    # Patrón para URLs de Booking.com
    pattern = r'^https?://(?:www\.)?booking\.com/hotel/.+\.html?$'
    
    return bool(re.match(pattern, url, re.IGNORECASE))

# =============================================================================
# CLASE PRINCIPAL
# =============================================================================

class URLLoader:
    """Cargador de URLs a la base de datos PostgreSQL"""
    
    def __init__(self, db_config: dict):
        self.db_config = db_config
        self.conn = None
        self.cursor = None
    
    def connect(self):
        """Establece conexión con PostgreSQL"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            print("✓ Conectado a PostgreSQL")
            print(f"  Base de datos: {self.db_config['database']}")
            print(f"  Usuario: {self.db_config['user']}")
            return True
        except psycopg2.Error as e:
            print(f"✗ Error conectando a PostgreSQL: {e}")
            print(f"\nVerificar:")
            print(f"  - PostgreSQL está corriendo")
            print(f"  - Base de datos '{self.db_config['database']}' existe")
            print(f"  - Usuario '{self.db_config['user']}' tiene permisos")
            print(f"  - Password es correcto")
            return False
    
    def disconnect(self):
        """Cierra la conexión"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("✓ Desconectado de PostgreSQL")
    
    def load_urls_from_csv(self, filepath: str) -> Tuple[int, int, List[str]]:
        """
        Carga URLs desde archivo CSV
        Soporta CSV con y sin encabezado
        
        Formatos aceptados:
        - Solo URLs (una por línea)
        - url,priority
        - URL en primera columna
        
        Args:
            filepath: Ruta al archivo CSV
            
        Returns:
            Tuple: (urls_añadidas, urls_duplicadas, errores)
        """
        urls_to_insert = []
        errors = []
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                # Leer primera línea para detectar formato
                first_line = f.readline().strip()
                f.seek(0)  # Volver al inicio
                
                # Detectar si tiene encabezado
                has_header = False
                if first_line.lower().startswith('url') or ',' in first_line and not first_line.startswith('http'):
                    has_header = True
                
                # Leer CSV
                reader = csv.reader(f)
                
                if has_header:
                    next(reader)  # Saltar encabezado
                
                for i, row in enumerate(reader, start=1):
                    if not row or not row[0].strip():
                        continue
                    
                    url = row[0].strip()
                    
                    # Prioridad (segunda columna si existe, sino 0)
                    priority = 0
                    if len(row) > 1:
                        try:
                            priority = int(row[1])
                        except:
                            priority = 0
                    
                    # Validar URL
                    if not is_valid_booking_url(url):
                        errors.append(f"Línea {i}: URL inválida - {url}")
                        continue
                    
                    urls_to_insert.append((url, priority))
        
        except FileNotFoundError:
            print(f"✗ Archivo no encontrado: {filepath}")
            return 0, 0, [f"Archivo no encontrado: {filepath}"]
        except Exception as e:
            print(f"✗ Error leyendo archivo: {e}")
            return 0, 0, [f"Error leyendo archivo: {e}"]
        
        if not urls_to_insert:
            print("✗ No se encontraron URLs válidas en el archivo")
            return 0, 0, errors
        
        # Insertar en base de datos
        return self._insert_urls(urls_to_insert, errors)
    
    def load_urls_from_txt(self, filepath: str) -> Tuple[int, int, List[str]]:
        """
        Carga URLs desde archivo TXT (una URL por línea)
        
        Args:
            filepath: Ruta al archivo TXT
            
        Returns:
            Tuple: (urls_añadidas, urls_duplicadas, errores)
        """
        urls_to_insert = []
        errors = []
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                for i, line in enumerate(f, start=1):
                    url = line.strip()
                    
                    if not url or url.startswith('#'):
                        continue
                    
                    if not is_valid_booking_url(url):
                        errors.append(f"Línea {i}: URL inválida - {url}")
                        continue
                    
                    urls_to_insert.append((url, 0))
        
        except FileNotFoundError:
            print(f"✗ Archivo no encontrado: {filepath}")
            return 0, 0, [f"Archivo no encontrado: {filepath}"]
        except Exception as e:
            print(f"✗ Error leyendo archivo: {e}")
            return 0, 0, [f"Error leyendo archivo: {e}"]
        
        if not urls_to_insert:
            print("✗ No se encontraron URLs válidas en el archivo")
            return 0, 0, errors
        
        return self._insert_urls(urls_to_insert, errors)
    
    def _insert_urls(self, urls: List[Tuple[str, int]], errors: List[str]) -> Tuple[int, int, List[str]]:
        """
        Inserta URLs en la base de datos
        
        Args:
            urls: Lista de tuplas (url, priority)
            errors: Lista de errores previa
            
        Returns:
            Tuple: (insertadas, duplicadas, errores)
        """
        insertadas = 0
        duplicadas = 0
        
        try:
            # Insertar en lotes
            batch_size = 100
            total = len(urls)
            
            print(f"\nInsertando {total} URLs en la base de datos...")
            
            for i in range(0, total, batch_size):
                batch = urls[i:i+batch_size]
                
                # Query con ON CONFLICT para manejar duplicados
                query = """
                INSERT INTO url_queue (url, priority, status)
                VALUES (%s, %s, 'pending')
                ON CONFLICT (url) DO NOTHING
                """
                
                # Ejecutar batch
                try:
                    execute_batch(self.cursor, query, batch)
                    self.conn.commit()
                    
                    # Contar insertadas (diferencia entre filas afectadas y tamaño del batch)
                    insertadas += self.cursor.rowcount
                    duplicadas += len(batch) - self.cursor.rowcount
                    
                    # Progress
                    progress = min(i + batch_size, total)
                    print(f"  Procesadas: {progress}/{total} URLs", end='\r')
                
                except psycopg2.Error as e:
                    self.conn.rollback()
                    errors.append(f"Error insertando batch {i}-{i+batch_size}: {e}")
            
            print()  # Nueva línea después del progress
            
        except Exception as e:
            self.conn.rollback()
            errors.append(f"Error general insertando URLs: {e}")
        
        return insertadas, duplicadas, errors
    
    def get_statistics(self):
        """Obtiene estadísticas de la cola de URLs"""
        try:
            self.cursor.execute("""
                SELECT 
                    status,
                    COUNT(*) as count
                FROM url_queue
                GROUP BY status
            """)
            
            stats = {}
            for row in self.cursor.fetchall():
                stats[row[0]] = row[1]
            
            return stats
        except:
            return {}

# =============================================================================
# FUNCIÓN PRINCIPAL
# =============================================================================

def main():
    """Función principal"""
    parser = argparse.ArgumentParser(
        description='Carga masiva de URLs de Booking.com a PostgreSQL',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso:
  
  Cargar CSV con URLs solamente:
    python load_urls.py --file urls.csv
  
  Cargar CSV con URLs y prioridades:
    python load_urls.py --file urls_con_prioridad.csv
  
  Cargar archivo TXT:
    python load_urls.py --file urls.txt

Formatos de archivo CSV aceptados:
  1. Solo URLs (una por línea, sin encabezado)
  2. Con encabezado: url,priority
  3. Con encabezado: URL,Priority

Formato de archivo TXT:
  - Una URL por línea
  - Líneas que empiecen con # son ignoradas
        """
    )
    
    parser.add_argument(
        '--file',
        type=str,
        required=True,
        help='Ruta al archivo CSV o TXT con las URLs'
    )
    
    parser.add_argument(
        '--db-host',
        type=str,
        default=DB_CONFIG['host'],
        help=f"Host de PostgreSQL (default: {DB_CONFIG['host']})"
    )
    
    parser.add_argument(
        '--db-port',
        type=int,
        default=DB_CONFIG['port'],
        help=f"Puerto de PostgreSQL (default: {DB_CONFIG['port']})"
    )
    
    parser.add_argument(
        '--db-name',
        type=str,
        default=DB_CONFIG['database'],
        help=f"Nombre de base de datos (default: {DB_CONFIG['database']})"
    )
    
    parser.add_argument(
        '--db-user',
        type=str,
        default=DB_CONFIG['user'],
        help=f"Usuario de PostgreSQL (default: {DB_CONFIG['user']})"
    )
    
    parser.add_argument(
        '--db-password',
        type=str,
        default=DB_CONFIG['password'],
        help='Password de PostgreSQL (default: desde .env)'
    )
    
    args = parser.parse_args()
    
    # Banner
    print("="*70)
    print("  CARGADOR DE URLs - Booking Scraper Pro")
    print("="*70)
    print()
    
    # Actualizar configuración con argumentos
    db_config = {
        'host': args.db_host,
        'port': args.db_port,
        'database': args.db_name,
        'user': args.db_user,
        'password': args.db_password
    }
    
    # Verificar archivo
    filepath = Path(args.file)
    if not filepath.exists():
        print(f"✗ ERROR: Archivo no encontrado: {filepath}")
        sys.exit(1)
    
    # Detectar tipo de archivo
    extension = filepath.suffix.lower()
    if extension not in ['.csv', '.txt']:
        print(f"✗ ERROR: Formato no soportado: {extension}")
        print("  Formatos aceptados: .csv, .txt")
        sys.exit(1)
    
    print(f"Archivo: {filepath}")
    print(f"Tipo: {extension}")
    print()
    
    # Crear loader y conectar
    loader = URLLoader(db_config)
    
    if not loader.connect():
        sys.exit(1)
    
    try:
        # Cargar URLs según tipo
        if extension == '.csv':
            added, duplicates, errors = loader.load_urls_from_csv(str(filepath))
        else:
            added, duplicates, errors = loader.load_urls_from_txt(str(filepath))
        
        # Mostrar resultados
        print()
        print("="*70)
        print("  RESULTADOS")
        print("="*70)
        print(f"  URLs añadidas:    {added}")
        print(f"  URLs duplicadas:  {duplicates}")
        print(f"  Errores:          {len(errors)}")
        print()
        
        if errors:
            print("Errores encontrados:")
            for error in errors[:10]:  # Mostrar solo primeros 10
                print(f"  - {error}")
            if len(errors) > 10:
                print(f"  ... y {len(errors) - 10} errores más")
            print()
        
        # Estadísticas finales
        stats = loader.get_statistics()
        if stats:
            print("Estadísticas de la cola:")
            for status, count in stats.items():
                print(f"  {status:12}: {count}")
        
        print("="*70)
        
        if added > 0:
            print("\n✓ URLs cargadas exitosamente")
            sys.exit(0)
        else:
            print("\n⚠ No se añadieron URLs nuevas")
            sys.exit(1)
    
    except KeyboardInterrupt:
        print("\n\n✗ Operación cancelada por el usuario")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Error inesperado: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        loader.disconnect()

if __name__ == '__main__':
    main()
