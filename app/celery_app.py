"""
BookingScraper/app/celery_app.py
Instancia y configuración de Celery - BookingScraper Pro
Windows 11 + Python 3.14.3

CORRECCIONES v1.1:
  [FIX] ELIMINADAS tareas duplicadas (estaban en tasks.py también)
  [FIX] include=['app.tasks'] → carga las tareas desde tasks.py
  [FIX] Beat schedule: rutas de tarea corregidas
  [FIX] worker_pool = 'solo' obligatorio para Windows
  [FIX] result_backend_transport_options eliminado (no aplica sin Sentinel)
  [FIX] worker_concurrency = 1 (único valor válido con pool=solo)

ARRANQUE:
  Worker:  celery -A app.celery_app worker --pool=solo --loglevel=info
  Beat:    celery -A app.celery_app beat --loglevel=info
"""

from celery import Celery
from celery.schedules import crontab
import os
from pathlib import Path
import sys

# Asegurar que el directorio raíz está en el path
root_dir = Path(__file__).parent.parent
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

from dotenv import load_dotenv
load_dotenv()

# ── INSTANCIA CELERY ──────────────────────────────────────────────────────────
# ✅ FIX: include apunta a tasks.py (donde están definidas las tareas)
#         NO se incluye a sí mismo (evita importación circular)
celery_app = Celery(
    "booking_scraper",
    broker=os.getenv("CELERY_BROKER_URL",     "redis://localhost:6379/0"),
    backend=os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/1"),
    include=["app.tasks"],   # ✅ FIX: apunta a app/tasks.py
)

# ── CONFIGURACIÓN WINDOWS ─────────────────────────────────────────────────────
celery_app.conf.update(
    # Serialización
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,

    # ✅ FIX: 'solo' es el único pool compatible con Windows
    worker_pool="solo",
    # ✅ FIX: con pool='solo' la concurrencia SIEMPRE es 1
    worker_concurrency=1,
    worker_prefetch_multiplier=1,

    # Comportamiento de tareas
    task_track_started=True,
    task_time_limit=600,          # 10 min máximo por tarea
    task_soft_time_limit=540,     # 9 min → soft kill
    task_acks_late=True,          # ACK después de completar (evita pérdida)

    # Reintentos por defecto
    task_default_retry_delay=60,  # 1 min
    task_max_retries=3,

    # Resultados
    result_expires=3600,          # 1 hora

    # ── BEAT SCHEDULE (tareas periódicas) ─────────────────────────
    beat_schedule={
        # Despachar batch de URLs pendientes cada 30 segundos
        "dispatch-pending-urls": {
            "task": "app.tasks.process_pending_urls",   # ✅ FIX: ruta correcta
            "schedule": 30.0,
            "args": (5,),          # batch_size = 5
        },
        # Limpiar logs antiguos cada día a las 03:00
        "cleanup-old-logs": {
            "task": "app.tasks.cleanup_old_logs",       # ✅ FIX: ruta correcta
            "schedule": crontab(hour=3, minute=0),
        },
        # Guardar métricas del sistema cada 5 minutos
        "save-system-metrics": {
            "task": "app.tasks.save_system_metrics",    # ✅ FIX: ruta correcta
            "schedule": 300.0,
        },
    },
)


if __name__ == "__main__":
    print("=" * 60)
    print("  Celery - BookingScraper Pro (Windows)")
    print("=" * 60)
    print()
    print("Para iniciar el Worker:")
    print("  celery -A app.celery_app worker --pool=solo --loglevel=info")
    print()
    print("Para iniciar el Beat scheduler:")
    print("  celery -A app.celery_app beat --loglevel=info")
    print()
    print("Para ver tareas registradas:")
    print("  celery -A app.celery_app inspect registered")
    print("=" * 60)
