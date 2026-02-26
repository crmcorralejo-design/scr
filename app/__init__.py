"""
BookingScraper/app/__init__.py
BookingScraper Pro — Application Package
Windows 11 + Python 3.14.3

CORRECCIONES v1.1:
  [FIX] Añadido SystemMetrics a los exports (estaba en models.py pero faltaba aquí)
  [FIX] Imports consistentes con models.py y database.py corregidos
"""

__version__ = "1.0.0"
__author__   = "BookingScraper Team"
__platform__ = "Windows 11"

from app.config   import settings
from app.database import engine, SessionLocal, get_db
from app.models   import (
    Base,
    URLQueue,
    Hotel,
    ScrapingLog,
    VPNRotation,
    SystemMetrics,    # ✅ FIX: estaba definida en models.py pero no exportada aquí
)

__all__ = [
    # Config
    "settings",

    # Database
    "engine",
    "SessionLocal",
    "get_db",

    # Models
    "Base",
    "URLQueue",
    "Hotel",
    "ScrapingLog",
    "VPNRotation",
    "SystemMetrics",
]
