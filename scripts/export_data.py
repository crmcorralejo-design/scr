"""
BookingScraper/scripts/export_data.py
BookingScraper
Export Data Script - BookingScraper Pro
Exporta datos de hoteles y URLs a CSV o Excel
Windows 11 - Python 3.14.3
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime

try:
    import pandas as pd
except ImportError:
    print("[ERROR] pandas no esta instalado")
    print("Instalar con: pip install pandas openpyxl")
    sys.exit(1)

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Importar modelos
sys.path.append(str(Path(__file__).parent.parent))
from app.models import Hotel, URLQueue, ScrapingLog, VPNRotation, SystemMetric
from app.config import settings


def create_session():
    """Crear sesion de base de datos"""
    engine = create_engine(settings.DATABASE_URL)
    Session = sessionmaker(bind=engine)
    return Session()


def export_hotels(session, format='csv', output_dir='data'):
    """Exportar hoteles a CSV o Excel"""
    print(f"\n[1/4] Exportando hoteles a {format.upper()}...")
    
    # Obtener todos los hoteles
    hotels = session.query(Hotel).all()
    
    if not hotels:
        print("[WARNING] No hay hoteles para exportar")
        return None
    
    # Convertir a DataFrame
    data = []
    for hotel in hotels:
        data.append({
            'id': hotel.id,
            'url_id': hotel.url_id,
            'name': hotel.name,
            'language': hotel.language,
            'address': hotel.address,
            'description': hotel.description[:200] if hotel.description else None,
            'rating': hotel.rating,
            'total_reviews': hotel.total_reviews,
            'services': hotel.services[:100] if hotel.services else None,
            'rooms_info': hotel.rooms_info[:100] if hotel.rooms_info else None,
            'images_count': len(hotel.images_urls.split(',')) if hotel.images_urls else 0,
            'created_at': hotel.created_at,
            'updated_at': hotel.updated_at
        })
    
    df = pd.DataFrame(data)
    
    # Crear directorio si no existe
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    # Generar nombre de archivo con timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    if format == 'csv':
        filename = output_path / f'hotels_{timestamp}.csv'
        df.to_csv(filename, index=False, encoding='utf-8-sig')
    else:  # xlsx
        filename = output_path / f'hotels_{timestamp}.xlsx'
        df.to_excel(filename, index=False, engine='openpyxl')
    
    print(f"[OK] {len(hotels)} hoteles exportados a: {filename}")
    return filename


def export_urls(session, format='csv', output_dir='data'):
    """Exportar URLs a CSV o Excel"""
    print(f"\n[2/4] Exportando URLs a {format.upper()}...")
    
    urls = session.query(URLQueue).all()
    
    if not urls:
        print("[WARNING] No hay URLs para exportar")
        return None
    
    data = []
    for url in urls:
        data.append({
            'id': url.id,
            'url': url.url,
            'language': url.language,
            'status': url.status,
            'priority': url.priority,
            'retry_count': url.retry_count,
            'last_error': url.last_error,
            'created_at': url.created_at,
            'updated_at': url.updated_at,
            'scraped_at': url.scraped_at
        })
    
    df = pd.DataFrame(data)
    
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    if format == 'csv':
        filename = output_path / f'urls_{timestamp}.csv'
        df.to_csv(filename, index=False, encoding='utf-8-sig')
    else:
        filename = output_path / f'urls_{timestamp}.xlsx'
        df.to_excel(filename, index=False, engine='openpyxl')
    
    print(f"[OK] {len(urls)} URLs exportadas a: {filename}")
    return filename


def export_logs(session, format='csv', output_dir='data', limit=1000):
    """Exportar logs de scraping"""
    print(f"\n[3/4] Exportando logs (ultimos {limit}) a {format.upper()}...")
    
    logs = session.query(ScrapingLog).order_by(
        ScrapingLog.timestamp.desc()
    ).limit(limit).all()
    
    if not logs:
        print("[WARNING] No hay logs para exportar")
        return None
    
    data = []
    for log in logs:
        data.append({
            'id': log.id,
            'url_id': log.url_id,
            'status': log.status,
            'duration_seconds': log.duration_seconds,
            'items_extracted': log.items_extracted,
            'error_message': log.error_message,
            'vpn_ip': log.vpn_ip,
            'timestamp': log.timestamp
        })
    
    df = pd.DataFrame(data)
    
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    if format == 'csv':
        filename = output_path / f'logs_{timestamp}.csv'
        df.to_csv(filename, index=False, encoding='utf-8-sig')
    else:
        filename = output_path / f'logs_{timestamp}.xlsx'
        df.to_excel(filename, index=False, engine='openpyxl')
    
    print(f"[OK] {len(logs)} logs exportados a: {filename}")
    return filename


def export_statistics(session, format='csv', output_dir='data'):
    """Exportar estadisticas del sistema"""
    print(f"\n[4/4] Generando estadisticas...")
    
    stats = {
        'total_urls': session.query(URLQueue).count(),
        'urls_pending': session.query(URLQueue).filter_by(status='pending').count(),
        'urls_processing': session.query(URLQueue).filter_by(status='processing').count(),
        'urls_completed': session.query(URLQueue).filter_by(status='completed').count(),
        'urls_failed': session.query(URLQueue).filter_by(status='failed').count(),
        'total_hotels': session.query(Hotel).count(),
        'total_logs': session.query(ScrapingLog).count(),
        'total_vpn_rotations': session.query(VPNRotation).count(),
    }
    
    # Estadisticas por idioma
    languages = session.query(
        Hotel.language, 
        session.query(Hotel).filter_by(language=Hotel.language).count()
    ).distinct().all()
    
    print("\n" + "="*60)
    print("ESTADISTICAS DEL SISTEMA")
    print("="*60)
    for key, value in stats.items():
        print(f"  {key.replace('_', ' ').title()}: {value}")
    
    if languages:
        print("\nHoteles por idioma:")
        for lang, count in languages:
            print(f"  {lang}: {count}")
    
    print("="*60)
    
    # Guardar estadisticas
    df_stats = pd.DataFrame([stats])
    
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    if format == 'csv':
        filename = output_path / f'stats_{timestamp}.csv'
        df_stats.to_csv(filename, index=False, encoding='utf-8-sig')
    else:
        filename = output_path / f'stats_{timestamp}.xlsx'
        df_stats.to_excel(filename, index=False, engine='openpyxl')
    
    print(f"\n[OK] Estadisticas guardadas en: {filename}")
    return filename


def main():
    parser = argparse.ArgumentParser(
        description='Exportar datos de BookingScraper Pro'
    )
    parser.add_argument(
        '--format',
        choices=['csv', 'xlsx'],
        default='csv',
        help='Formato de exportacion (default: csv)'
    )
    parser.add_argument(
        '--output',
        default='data',
        help='Directorio de salida (default: data)'
    )
    parser.add_argument(
        '--tables',
        nargs='+',
        choices=['hotels', 'urls', 'logs', 'stats', 'all'],
        default=['all'],
        help='Tablas a exportar (default: all)'
    )
    parser.add_argument(
        '--log-limit',
        type=int,
        default=1000,
        help='Limite de logs a exportar (default: 1000)'
    )
    
    args = parser.parse_args()
    
    print("\n" + "="*60)
    print("EXPORTADOR DE DATOS - BookingScraper Pro")
    print("="*60)
    print(f"Formato: {args.format.upper()}")
    print(f"Directorio: {args.output}")
    print("="*60)
    
    try:
        # Crear sesion
        session = create_session()
        
        tables = args.tables
        if 'all' in tables:
            tables = ['hotels', 'urls', 'logs', 'stats']
        
        exported_files = []
        
        # Exportar tablas seleccionadas
        if 'hotels' in tables:
            file = export_hotels(session, args.format, args.output)
            if file:
                exported_files.append(file)
        
        if 'urls' in tables:
            file = export_urls(session, args.format, args.output)
            if file:
                exported_files.append(file)
        
        if 'logs' in tables:
            file = export_logs(session, args.format, args.output, args.log_limit)
            if file:
                exported_files.append(file)
        
        if 'stats' in tables:
            file = export_statistics(session, args.format, args.output)
            if file:
                exported_files.append(file)
        
        # Resumen final
        print("\n" + "="*60)
        print("EXPORTACION COMPLETADA")
        print("="*60)
        print(f"Archivos generados: {len(exported_files)}")
        for file in exported_files:
            print(f"  - {file}")
        print("="*60)
        
        session.close()
        
    except Exception as e:
        print(f"\n[ERROR] {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
