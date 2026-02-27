"""
BookingScraper/app/models.py
Modelos SQLAlchemy - BookingScraper Pro
Windows 11 + Python 3.14.3

CORRECCIONES v1.1:
  [FIX] Hotel.url_queue_id → Hotel.url_id (consistente con tasks.py y main.py)
  [FIX] URLQueue: añadidas columnas 'language', 'scraped_at', 'retry_count'
  [FIX] Relaciones back_populates: hotel↔url_queue corregidas
  [FIX] declarative_base() desde sqlalchemy.orm (no deprecated)
  [FIX] Hotel: columna 'url' añadida para acceso directo sin JOIN
  [NEW] Índices compuestos para consultas frecuentes
"""

from sqlalchemy import (
    Column, Integer, String, Text, DateTime, Boolean,
    JSON, Float, ForeignKey, Index
)
from sqlalchemy.orm import declarative_base, relationship
from datetime import datetime

Base = declarative_base()


class URLQueue(Base):
    """Cola de URLs a procesar"""
    __tablename__ = "url_queue"

    id          = Column(Integer, primary_key=True, autoincrement=True)
    url         = Column(String(512), unique=True, nullable=False, index=True)

    # Estado del proceso
    status      = Column(String(50), default="pending", index=True)
    # pending | processing | completed | failed

    priority    = Column(Integer, default=0, index=True)

    # ✅ FIX: Columnas que tasks.py usa pero faltaban en el modelo
    language    = Column(String(10), default="en")          # idioma base de la URL
    retry_count = Column(Integer, default=0)                # contador reintentos
    max_retries = Column(Integer, default=3)
    last_error  = Column(Text, nullable=True)
    scraped_at  = Column(DateTime, nullable=True)           # ✅ FIX: tasks.py lo actualiza

    # Timestamps
    created_at  = Column(DateTime, default=datetime.utcnow)
    updated_at  = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # ✅ FIX: back_populates debe coincidir con Hotel.url_queue
    hotel       = relationship("Hotel", back_populates="url_queue", uselist=False)

    # Índice compuesto para la consulta de despacho
    __table_args__ = (
        Index("ix_urlqueue_status_priority", "status", "priority"),
    )

    def __repr__(self):
        return f"<URLQueue(id={self.id}, status={self.status}, url={self.url[:60]}...)>"


class Hotel(Base):
    """Datos extraídos de hoteles - una fila por hotel+idioma"""
    __tablename__ = "hotels"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # ✅ FIX: Renombrado de url_queue_id → url_id (consistente con tasks.py y main.py)
    url_id      = Column(Integer, ForeignKey("url_queue.id"), nullable=True, index=True)

    # ✅ NEW: URL directa para evitar JOIN en consultas simples
    url         = Column(String(512), nullable=True)
    language    = Column(String(10),  default="en", index=True)

    # Información básica
    name        = Column(String(255), nullable=True, index=True)
    address     = Column(Text,        nullable=True)
    description = Column(Text,        nullable=True)

    # Puntuaciones
    rating              = Column(Float,   nullable=True)
    total_reviews       = Column(Integer, nullable=True)
    rating_category     = Column(String(100), nullable=True)
    review_scores       = Column(JSON,    nullable=True)  # {"limpieza": 9.5, ...}

    # Servicios e instalaciones
    services    = Column(JSON, nullable=True)   # ["WiFi", "Piscina", ...]
    facilities  = Column(JSON, nullable=True)   # {"Categoría": ["item1", ...]}

    # Políticas
    house_rules    = Column(Text, nullable=True)
    important_info = Column(Text, nullable=True)

    # Habitaciones
    rooms_info  = Column(JSON, nullable=True)   # [{"name": ..., "description": ...}]

    # Imágenes
    images_urls  = Column(JSON, nullable=True)  # ["https://...", ...]
    images_local = Column(JSON, nullable=True)  # rutas locales descargadas
    images_count = Column(Integer, default=0)

    # Metadatos
    scraped_at  = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at  = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # ✅ FIX: back_populates debe coincidir con URLQueue.hotel
    url_queue   = relationship("URLQueue", back_populates="hotel")

    # Índice único por URL + idioma (permite multi-idioma del mismo hotel)
    __table_args__ = (
        Index("ix_hotels_url_language", "url_id", "language", unique=True),
    )

    def __repr__(self):
        return f"<Hotel(id={self.id}, name={self.name}, lang={self.language})>"


class ScrapingLog(Base):
    """Log detallado de cada operación de scraping"""
    __tablename__ = "scraping_logs"

    id          = Column(Integer, primary_key=True, autoincrement=True)
    url_id      = Column(Integer, ForeignKey("url_queue.id"), nullable=True)

    # Operación
    status          = Column(String(50),  nullable=False)   # completed | error | retry
    language        = Column(String(10),  nullable=True)
    duration_seconds= Column(Float,       nullable=True)
    items_extracted = Column(Integer,     default=0)
    error_message   = Column(Text,        nullable=True)

    # Contexto
    http_status_code= Column(Integer,     nullable=True)
    user_agent      = Column(Text,        nullable=True)
    vpn_ip          = Column(String(50),  nullable=True)
    task_id         = Column(String(100), nullable=True)

    # Timestamp
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)

    def __repr__(self):
        return f"<ScrapingLog(id={self.id}, status={self.status})>"


class VPNRotation(Base):
    """Registro de rotaciones de VPN"""
    __tablename__ = "vpn_rotations"

    id              = Column(Integer, primary_key=True, autoincrement=True)
    old_ip          = Column(String(45), nullable=True)
    new_ip          = Column(String(45), nullable=True)
    country         = Column(String(100), nullable=True)
    rotation_reason = Column(String(100), nullable=True)
    requests_count  = Column(Integer,    default=0)
    success         = Column(Boolean,    default=True)
    error_message   = Column(Text,       nullable=True)
    rotated_at      = Column(DateTime,   default=datetime.utcnow)

    def __repr__(self):
        return f"<VPNRotation(id={self.id}, {self.old_ip} → {self.new_ip})>"


class SystemMetrics(Base):
    """Métricas del sistema (capturadas periódicamente)"""
    __tablename__ = "system_metrics"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Cola de URLs
    urls_pending    = Column(Integer, default=0)
    urls_processing = Column(Integer, default=0)
    urls_completed  = Column(Integer, default=0)
    urls_failed     = Column(Integer, default=0)

    # Producción
    hotels_scraped     = Column(Integer, default=0)
    images_downloaded  = Column(Integer, default=0)

    # Workers
    active_workers = Column(Integer, default=0)

    # Rendimiento
    avg_scraping_time   = Column(Float, nullable=True)
    total_scraping_time = Column(Float, default=0.0)

    # Recursos del sistema
    cpu_usage    = Column(Float, nullable=True)
    memory_usage = Column(Float, nullable=True)
    disk_usage   = Column(Float, nullable=True)

    recorded_at = Column(DateTime, default=datetime.utcnow, index=True)

    def __repr__(self):
        return f"<SystemMetrics(id={self.id}, completed={self.urls_completed})>"
