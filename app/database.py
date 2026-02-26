"""
BookingScraper/app/database.py
Conexión PostgreSQL con SQLAlchemy 2.0
Windows 11 nativo - psycopg3

CORRECCIONES v1.1:
  [FIX] DATABASE_URL: postgresql+psycopg:// (psycopg3)
  [FIX] Todos los raw SQL envueltos en text() → SQLAlchemy 2.0
  [FIX] Pool: QueuePool con pre_ping para reconexión automática
  [FIX] connect_args sin opciones inválidas para psycopg3
"""

from sqlalchemy import create_engine, text
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.pool import QueuePool
import os
from dotenv import load_dotenv
from loguru import logger

# Cargar variables de entorno
load_dotenv()

# ── URL DE CONEXIÓN ────────────────────────────────────────────────────────────
# ✅ FIX: postgresql+psycopg:// para psycopg3 (el prefijo postgresql:// es psycopg2)
_DB_USER     = os.getenv("DB_USER",     "postgres")
_DB_PASSWORD = os.getenv("DB_PASSWORD", "2221")
_DB_HOST     = os.getenv("DB_HOST",     "localhost")
_DB_PORT     = os.getenv("DB_PORT",     "5432")
_DB_NAME     = os.getenv("DB_NAME",     "booking_scraper")

DATABASE_URL = (
    f"postgresql+psycopg://{_DB_USER}:{_DB_PASSWORD}"
    f"@{_DB_HOST}:{_DB_PORT}/{_DB_NAME}"
)

# ── MOTOR ──────────────────────────────────────────────────────────────────────
engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,   # ✅ Reconecta automáticamente si la conexión se cayó
    pool_recycle=3600,    # Recicla conexiones cada 1 hora
    echo=os.getenv("DEBUG", "false").lower() == "true",
)

# ── SESIÓN ─────────────────────────────────────────────────────────────────────
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
)

# ── BASE ORM ───────────────────────────────────────────────────────────────────
Base = declarative_base()


# ── DEPENDENCIA FASTAPI ────────────────────────────────────────────────────────
def get_db():
    """
    Generador de sesión para FastAPI (Depends).
    Uso: db: Session = Depends(get_db)
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ── UTILIDADES ─────────────────────────────────────────────────────────────────
def test_connection() -> bool:
    """
    Prueba la conexión a PostgreSQL.
    ✅ FIX: Usa text() para SQLAlchemy 2.0
    """
    try:
        with SessionLocal() as db:
            # ✅ FIX: text() obligatorio en SQLAlchemy 2.0
            db.execute(text("SELECT 1"))
        logger.success("✓ Conexión a PostgreSQL exitosa")
        return True
    except Exception as e:
        logger.error(f"✗ Error de conexión a PostgreSQL: {e}")
        return False


def get_db_version() -> str:
    """
    Devuelve la versión de PostgreSQL.
    ✅ FIX: Usa text() para SQLAlchemy 2.0
    """
    try:
        with SessionLocal() as db:
            result = db.execute(text("SELECT version()")).fetchone()  # ✅ text()
        return result[0] if result else "Unknown"
    except Exception as e:
        return f"Error: {e}"


def execute_sql_file(filepath: str) -> bool:
    """
    Ejecuta un archivo SQL completo.
    ✅ FIX: Usa text() para SQLAlchemy 2.0
    """
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            sql_content = f.read()

        # Separar por ; para ejecutar statement a statement
        statements = [s.strip() for s in sql_content.split(";") if s.strip()]

        with SessionLocal() as db:
            for stmt in statements:
                db.execute(text(stmt))   # ✅ text()
            db.commit()

        logger.success(f"✓ Archivo SQL ejecutado: {filepath}")
        return True
    except Exception as e:
        logger.error(f"✗ Error ejecutando SQL '{filepath}': {e}")
        return False


def get_url_queue_stats() -> dict:
    """
    Devuelve estadísticas de la cola de URLs.
    ✅ FIX: Usa text() para SQLAlchemy 2.0
    """
    try:
        with SessionLocal() as db:
            result = db.execute(text("""
                SELECT
                    COUNT(*) FILTER (WHERE status = 'pending')    AS pending,
                    COUNT(*) FILTER (WHERE status = 'processing') AS processing,
                    COUNT(*) FILTER (WHERE status = 'completed')  AS completed,
                    COUNT(*) FILTER (WHERE status = 'failed')     AS failed,
                    COUNT(*)                                       AS total
                FROM url_queue
            """)).fetchone()   # ✅ text()

        return {
            "pending":    result[0] or 0,
            "processing": result[1] or 0,
            "completed":  result[2] or 0,
            "failed":     result[3] or 0,
            "total":      result[4] or 0,
        }
    except Exception as e:
        logger.error(f"Error obteniendo stats: {e}")
        return {"pending": 0, "processing": 0, "completed": 0, "failed": 0, "total": 0}


# ── TEST STANDALONE ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("  Test de Conexión a PostgreSQL")
    print("=" * 60)

    if test_connection():
        print(f"\nVersión: {get_db_version()}")
        print(f"\nEstadísticas de URL Queue:")
        stats = get_url_queue_stats()
        for k, v in stats.items():
            print(f"  {k:12s}: {v}")
    else:
        print("\n✗ No se pudo conectar. Verificar PostgreSQL y .env")

    print("=" * 60)
