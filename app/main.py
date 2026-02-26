"""
BookingScraper/app/main.py  v2.3  [FIXED]
FastAPI Application - BookingScraper Pro

CAMBIOS v2.3 [FIX DE IDIOMA]:
  [FIX BUG #3] /urls/load: URLs normalizadas antes de insertar en url_queue.
    Nueva función _normalize_booking_url() elimina el sufijo de idioma existente
    (.es, .de, .en-gb...) de la URL. Sin este fix, las URLs .es.html almacenadas
    impedían a build_language_url() construir correctamente las URLs de otros idiomas.
    Aplica tanto al modo lista plana como al modo CSV con cabecera.

Windows 11 + Python 3.14.3

CAMBIOS v2.1:
  [NEW] /vpn/status  - estado en tiempo real de la VPN
  [NEW] /vpn/rotate  - rota la VPN manualmente desde el API
  [NEW] /vpn/connect - conecta VPN a un pais especifico
  [NEW] /scraping/test-url - prueba extraccion en una URL concreta (diagnostico)
  [FIX] /scraping/start y /scraping/force-now mejorados

CAMBIOS v2.2:
  [FIX CRITICO] /urls/load: acepta lista plana de URLs sin cabecera (formato del proyecto).
               csv.DictReader usaba la 1a URL como nombre de columna -> row.get("url")=None
               -> todas las filas se contaban como skipped -> {"inserted":0,"skipped":15}.
  [FIX] /urls/load: contador 'inserted' usa rowcount real (no incrementa en ON CONFLICT).
  [FIX] /urls/load: respuesta anade campos 'format' y 'errors' para diagnostico.
"""

import asyncio
import csv
import re
import sys
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, BackgroundTasks, Body, Depends, File, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from loguru import logger
from sqlalchemy import text
from sqlalchemy.orm import Session

sys.path.append(str(Path(__file__).parent.parent))

from app.config import settings
from app.database import get_db, test_connection

# ─────────────────────────────────────────────────────────────────────────────
# AUTO-DISPATCHER (asyncio, sin Celery)
# ─────────────────────────────────────────────────────────────────────────────

_dispatch_task: Optional[asyncio.Task] = None
_dispatcher_running: bool = False


def _sync_dispatch(batch_size: int) -> dict:
    try:
        from app.scraper_service import process_batch
        return process_batch(batch_size)
    except Exception as e:
        logger.error(f"_sync_dispatch error: {e}")
        return {"dispatched": 0, "error": str(e)}


# ─────────────────────────────────────────────────────────────────────────────
# [FIX BUG #3] Normalización de URL de Booking.com
# Elimina el sufijo de idioma existente (.es, .de, .en-gb, etc.) para que
# build_language_url() en scraper.py pueda construir correctamente las URLs
# de todos los idiomas sin producir dobles sufijos (.es.de.html) o dejar
# la URL original sin cambio (.es.html para el idioma inglés).
# ─────────────────────────────────────────────────────────────────────────────

_LANG_SUFFIX_RE = re.compile(
    r'\.[a-z]{2}(?:-[a-z]{2,4})?\.html$',
    flags=re.IGNORECASE,
)


def _normalize_booking_url(url: str) -> str:
    """
    Normaliza una URL de Booking.com eliminando el sufijo de idioma existente
    y cualquier query string previo.

    [FIX v2.3] Eliminación de sufijo de idioma existente.
    [FIX v2.4] Eliminación de query string (e.g. ?lang=en-us) antes de la normalización.
      Sin este fix, URLs como '.../hotel.it.html?lang=en-us' no matcheaban la regex
      porque el string no terminaba en '.html' sino en '?lang=en-us'.

    Ejemplos:
      .../hotel.es.html              → .../hotel.html
      .../hotel.en-gb.html           → .../hotel.html
      .../hotel.zh-cn.html           → .../hotel.html
      .../hotel.html                 → .../hotel.html  (sin cambios)
      .../hotel.it.html?lang=en-us   → .../hotel.html  (strip query + sufijo)
      .../hotel.html?lang=es         → .../hotel.html  (strip query only)
    """
    url = url.strip()
    # [FIX v2.4] Eliminar query string primero para que la regex funcione correctamente
    if "?" in url:
        url = url.split("?")[0]
    # [FIX v2.3] Eliminar sufijo de idioma
    normalized = _LANG_SUFFIX_RE.sub('.html', url)
    if not normalized.endswith('.html'):
        normalized += '.html'
    return normalized


async def _auto_dispatch_loop():
    global _dispatcher_running
    _dispatcher_running = True
    logger.info("🤖 Auto-dispatcher iniciado (ciclo 30s) — no requiere Celery")

    await asyncio.sleep(5)

    while True:
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                lambda: _sync_dispatch(settings.BATCH_SIZE)
            )
            n = result.get("dispatched", 0)
            if n > 0:
                logger.info(f"🤖 Auto-dispatch: {n} URLs enviadas al thread pool")
        except asyncio.CancelledError:
            logger.info("🤖 Auto-dispatcher detenido")
            break
        except Exception as e:
            logger.error(f"🤖 Auto-dispatch error: {e}")

        await asyncio.sleep(30)

    _dispatcher_running = False


# ─────────────────────────────────────────────────────────────────────────────
# LIFESPAN
# ─────────────────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _dispatch_task

    print("\n" + "=" * 60)
    print("  BookingScraper Pro v2.1 - Iniciando")
    print("=" * 60)
    db_ok = test_connection()
    print(f"  Base de datos  : {'✓ OK' if db_ok else '✗ ERROR'}")
    print(f"  Idiomas        : {', '.join(settings.ENABLED_LANGUAGES)}")
    print(f"  Batch size     : {settings.BATCH_SIZE}")
    print(f"  Selenium       : {'✓ ACTIVO' if settings.USE_SELENIUM else '✗ cloudscraper'}")
    print(f"  VPN            : {'✓ ACTIVO' if settings.VPN_ENABLED else '✗ desactivado'}")
    print(f"  Auto-scraper   : ✓ ACTIVO (cada 30s)")
    print(f"  Docs           : http://localhost:8000/docs")
    print(f"  VPN status     : http://localhost:8000/vpn/status")
    print(f"  Scraping status: http://localhost:8000/scraping/status")
    print("=" * 60 + "\n")

    _dispatch_task = asyncio.create_task(_auto_dispatch_loop())

    # Iniciar VPN en background si está habilitada
    if settings.VPN_ENABLED:
        async def _init_vpn():
            try:
                from app.scraper_service import _get_vpn_manager
                vpn = _get_vpn_manager()
                if vpn:
                    logger.info("🔐 VPN iniciada al arrancar")
            except Exception as e:
                logger.warning(f"⚠️ VPN init al arrancar: {e}")
        asyncio.create_task(_init_vpn())

    # Reset URLs atascadas al arrancar
    try:
        from app.database import SessionLocal
        _db = SessionLocal()
        r1 = _db.execute(text("UPDATE url_queue SET status='pending', updated_at=NOW() WHERE status='processing'"))
        r2 = _db.execute(text("UPDATE url_queue SET status='pending', retry_count=0, last_error=NULL, updated_at=NOW() WHERE status='failed'"))
        _db.commit()
        _db.close()
        if r1.rowcount or r2.rowcount:
            print(f"  ♻️  Reset al arrancar: {r1.rowcount} processing + {r2.rowcount} failed → pending")
    except Exception as _e:
        print(f"  ⚠️ Reset al arrancar falló: {_e}")

    yield

    if _dispatch_task and not _dispatch_task.done():
        _dispatch_task.cancel()
        try:
            await _dispatch_task
        except asyncio.CancelledError:
            pass
    logger.info("BookingScraper Pro detenido")


# ─────────────────────────────────────────────────────────────────────────────
# APLICACIÓN
# ─────────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="BookingScraper Pro",
    description="Sistema profesional de scraping para Booking.com - Windows 11",
    version="2.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─────────────────────────────────────────────────────────────────────────────
# HEALTH
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/", tags=["Health"])
def root():
    from app.scraper_service import get_service_stats
    svc = get_service_stats()
    return {
        "app": "BookingScraper Pro", "version": "2.1.0",
        "docs": "/docs", "status": "running",
        "auto_dispatch": _dispatcher_running,
        "processing_now": svc["active_count"],
        "vpn_enabled": settings.VPN_ENABLED,
    }


@app.get("/health", tags=["Health"])
def health_check(db: Session = Depends(get_db)):
    try:
        db.execute(text("SELECT 1"))
        db_status = "ok"
    except Exception as e:
        db_status = f"error: {e}"
    from app.scraper_service import get_service_stats
    svc = get_service_stats()
    return {
        "status":     "healthy" if db_status == "ok" else "degraded",
        "database":   db_status,
        "dispatcher": "running" if _dispatcher_running else "stopped",
        "processing": svc["active_count"],
        "timestamp":  datetime.now().isoformat(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# VPN  ← NUEVA SECCIÓN
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/vpn/status", tags=["VPN"])
def vpn_status():
    """Estado actual de la VPN y métricas de rotación."""
    from app.scraper_service import get_vpn_status
    return get_vpn_status()


@app.post("/vpn/rotate", tags=["VPN"])
def vpn_rotate():
    """Rota la VPN inmediatamente a un servidor diferente."""
    from app.scraper_service import rotate_vpn_now
    result = rotate_vpn_now()
    if not result.get("success"):
        raise HTTPException(500, result.get("reason") or result.get("error") or "Error rotando VPN")
    return result


@app.post("/vpn/connect", tags=["VPN"])
def vpn_connect(country: str = Body(default=None, embed=True)):
    """
    Conecta VPN a un país específico.
    country: 'US', 'DE', 'FR', 'NL', 'ES', 'IT', 'CA', 'SE' ... o null para aleatorio
    """
    from app.scraper_service import _get_vpn_manager, _vpn_lock
    vpn = _get_vpn_manager()
    if not vpn:
        raise HTTPException(503, "VPN_ENABLED=False o VPN no disponible")
    with _vpn_lock:
        try:
            success = vpn.connect(country)
            return {
                "success": success,
                "country": country,
                "new_ip": vpn.current_ip,
                "server": vpn.current_server,
            }
        except Exception as e:
            raise HTTPException(500, str(e))


# ─────────────────────────────────────────────────────────────────────────────
# STATISTICS
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/stats", tags=["Statistics"])
def get_stats(db: Session = Depends(get_db)):
    try:
        urls = db.execute(text("""
            SELECT
                COUNT(*)                                       AS total,
                COUNT(*) FILTER (WHERE status='pending')      AS pending,
                COUNT(*) FILTER (WHERE status='processing')   AS processing,
                COUNT(*) FILTER (WHERE status='completed')    AS completed,
                COUNT(*) FILTER (WHERE status='failed')       AS failed
            FROM url_queue
        """)).fetchone()

        hotels_total = db.execute(text("SELECT COUNT(*) FROM hotels")).scalar() or 0
        hotels_by_lang = db.execute(text(
            "SELECT language, COUNT(*) FROM hotels GROUP BY language ORDER BY COUNT(*) DESC"
        )).fetchall()

        from app.scraper_service import get_service_stats, get_vpn_status
        svc = get_service_stats()

        return {
            "url_queue": {
                "total": urls[0] or 0,
                "pending": urls[1] or 0,
                "processing": urls[2] or 0,
                "completed": urls[3] or 0,
                "failed": urls[4] or 0,
            },
            "hotels": {
                "total": hotels_total,
                "by_language": {r[0]: r[1] for r in hotels_by_lang},
            },
            "service": svc,
            "vpn": get_vpn_status(),
            "timestamp": datetime.now().isoformat(),
        }
    except Exception as e:
        raise HTTPException(500, str(e))


# ─────────────────────────────────────────────────────────────────────────────
# URLS
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/urls", tags=["URLs"])
def list_urls(
    status: Optional[str] = None,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    try:
        limit = min(limit, 500)
        if status:
            rows = db.execute(
                text("SELECT id, url, status, language, priority, retry_count, scraped_at, last_error "
                     "FROM url_queue WHERE status=:s ORDER BY id LIMIT :l OFFSET :sk"),
                {"s": status, "l": limit, "sk": skip}
            ).fetchall()
        else:
            rows = db.execute(
                text("SELECT id, url, status, language, priority, retry_count, scraped_at, last_error "
                     "FROM url_queue ORDER BY id LIMIT :l OFFSET :sk"),
                {"l": limit, "sk": skip}
            ).fetchall()
        return {"total": len(rows), "urls": [dict(r._mapping) for r in rows]}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/urls/load", tags=["URLs"])
async def load_urls(file: UploadFile = File(...), db: Session = Depends(get_db)):
    """
    Carga URLs en la cola de scraping.
    Acepta dos formatos automáticamente:
      - Lista plana: una URL por línea, sin cabecera  ← formato del proyecto
      - CSV con cabecera: columna 'url' obligatoria, opcionales 'language' y 'priority'
    Líneas en blanco y comentarios con # son ignorados.
    """
    try:
        content = await file.read()
        raw_lines = content.decode("utf-8-sig", errors="ignore").splitlines()

        # ── Detectar formato ──────────────────────────────────────────────────
        # Si la primera línea con contenido contiene "booking.com" → lista plana
        # Si contiene "url" como cabecera CSV → modo CSV
        first_data_line = next(
            (l.strip() for l in raw_lines if l.strip() and not l.strip().startswith("#")),
            ""
        )
        is_plain_list = "booking.com" in first_data_line.lower()

        inserted = 0
        skipped  = 0
        errors   = 0

        if is_plain_list:
            # ── MODO LISTA PLANA: una URL por línea, sin cabecera ─────────────
            for raw_line in raw_lines:
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue
                if "booking.com" not in line:
                    skipped += 1
                    continue
                try:
                    # [FIX BUG #3] Normalizar URL: eliminar sufijo de idioma existente
                    # (.es, .de, .en-gb...) antes de almacenar. Esto garantiza que
                    # build_language_url() pueda construir correctamente las URLs
                    # de todos los idiomas sin producir dobles sufijos (.es.de.html).
                    normalized_url = _normalize_booking_url(line)
                    if normalized_url != line:
                        logger.debug(f"  🌐 URL normalizada: {line[-50:]} → {normalized_url[-50:]}")
                    result = db.execute(
                        text("""
                            INSERT INTO url_queue (url, language, priority, status,
                                                   retry_count, max_retries, created_at, updated_at)
                            VALUES (:url, 'en', 5, 'pending', 0, 3, NOW(), NOW())
                            ON CONFLICT (url) DO NOTHING
                        """),
                        {"url": normalized_url}
                    )
                    if result.rowcount > 0:
                        inserted += 1
                    else:
                        skipped += 1    # ya existía en la BD
                except Exception as e:
                    logger.warning(f"Error insertando '{line[:60]}': {e}")
                    errors += 1

        else:
            # ── MODO CSV CON CABECERA: columna 'url' requerida ───────────────
            reader = csv.DictReader(raw_lines)
            for row in reader:
                url = (row.get("url") or row.get("URL") or "").strip()
                if not url or "booking.com" not in url:
                    skipped += 1
                    continue
                language = (row.get("language") or row.get("lang") or "en").strip()
                priority = int(row.get("priority") or 5)
                try:
                    # [FIX BUG #3] Normalizar URL en modo CSV también
                    normalized_url = _normalize_booking_url(url)
                    result = db.execute(
                        text("""
                            INSERT INTO url_queue (url, language, priority, status,
                                                   retry_count, max_retries, created_at, updated_at)
                            VALUES (:url, :lang, :pri, 'pending', 0, 3, NOW(), NOW())
                            ON CONFLICT (url) DO NOTHING
                        """),
                        {"url": normalized_url, "lang": language, "pri": priority}
                    )
                    if result.rowcount > 0:
                        inserted += 1
                    else:
                        skipped += 1    # ya existía en la BD
                except Exception as e:
                    logger.warning(f"Error insertando '{url[:60]}': {e}")
                    errors += 1

        db.commit()
        return {
            "inserted": inserted,
            "skipped":  skipped,
            "errors":   errors,
            "format":   "plain_list" if is_plain_list else "csv_with_header",
        }
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/urls/reset-failed", tags=["URLs"])
def reset_failed_urls(db: Session = Depends(get_db)):
    """Resetea todas las URLs fallidas a 'pending' para reintentar."""
    try:
        r = db.execute(text(
            "UPDATE url_queue SET status='pending', retry_count=0, last_error=NULL, updated_at=NOW() "
            "WHERE status='failed'"
        ))
        db.commit()
        return {"reset": r.rowcount}
    except Exception as e:
        raise HTTPException(500, str(e))


# ─────────────────────────────────────────────────────────────────────────────
# SCRAPING
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/scraping/start", tags=["Scraping"])
def scraping_start(
    batch_size: int = Query(default=5, ge=1, le=20),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """Despacha un batch de URLs inmediatamente (además del auto-dispatcher)."""
    def _run():
        from app.scraper_service import process_batch
        process_batch(batch_size)

    background_tasks.add_task(_run)
    return {"message": f"Batch de {batch_size} URLs despachado", "auto_dispatch": _dispatcher_running}


@app.post("/scraping/force-now", tags=["Scraping"])
def scraping_force_now(batch_size: int = Query(default=5, ge=1, le=20)):
    """Despacha un batch sincrónicamente y devuelve el resultado."""
    from app.scraper_service import process_batch
    result = process_batch(batch_size)
    return result


@app.post("/scraping/test-url", tags=["Scraping"])
def test_url(
    url: str = Body(..., embed=True),
    language: str = Body(default="en", embed=True)
):
    """
    [DIAGNÓSTICO] Prueba la extracción de una URL concreta.
    Devuelve los datos extraídos SIN guardar en BD.
    Útil para verificar que el scraper funciona con una URL específica.
    """
    try:
        from app.scraper import BookingScraper
        with BookingScraper() as scraper:
            data = scraper.scrape_hotel(url, language=language)

        if not data:
            return {"success": False, "error": "No se obtuvieron datos — posible bloqueo o URL inválida"}

        return {
            "success":      bool(data.get("name")),
            "name":         data.get("name"),
            "address":      data.get("address"),
            "description":  (data.get("description") or "")[:200],
            "rating":       data.get("rating"),
            "total_reviews": data.get("total_reviews"),
            "images_count": len(data.get("images_urls") or []),
            "html_length":  data.get("html_length"),
            "http_status":  data.get("http_status"),
            "page_title":   data.get("page_title"),
        }
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/scraping/status", tags=["Scraping"])
def scraping_status(db: Session = Depends(get_db)):
    """Estado en tiempo real del sistema de scraping."""
    try:
        from app.scraper_service import get_service_stats, get_vpn_status

        queue = db.execute(text("SELECT status, COUNT(*) FROM url_queue GROUP BY status")).fetchall()
        q = {r[0]: r[1] for r in queue}

        processing_urls = db.execute(text("""
            SELECT id, url, updated_at FROM url_queue
            WHERE status = 'processing' ORDER BY updated_at DESC LIMIT 20
        """)).fetchall()

        last_completed = db.execute(text("""
            SELECT id, url, scraped_at FROM url_queue
            WHERE status = 'completed' ORDER BY scraped_at DESC NULLS LAST LIMIT 5
        """)).fetchall()

        last_logs = db.execute(text("""
            SELECT url_id, language, status, duration_seconds, error_message, timestamp
            FROM scraping_logs ORDER BY timestamp DESC LIMIT 10
        """)).fetchall()

        svc = get_service_stats()

        return {
            "dispatcher": {"running": _dispatcher_running, "cycle_seconds": 30},
            "queue": {
                "pending":    q.get("pending",    0),
                "processing": q.get("processing", 0),
                "completed":  q.get("completed",  0),
                "failed":     q.get("failed",     0),
            },
            "service": svc,
            "vpn": get_vpn_status(),
            "currently_processing": [
                {"id": r[0], "url": r[1][:80], "since": str(r[2])} for r in processing_urls
            ],
            "recently_completed": [
                {"id": r[0], "url": r[1][:80], "at": str(r[2])} for r in last_completed
            ],
            "recent_logs": [
                {"url_id": r[0], "lang": r[1], "status": r[2],
                 "duration_s": r[3], "error": r[4], "at": str(r[5])}
                for r in last_logs
            ],
        }
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/scraping/logs", tags=["Scraping"])
def get_logs(limit: int = 100, db: Session = Depends(get_db)):
    try:
        rows = db.execute(text("""
            SELECT id, url_id, language, status, duration_seconds, items_extracted,
                   error_message, timestamp
            FROM scraping_logs ORDER BY timestamp DESC LIMIT :lim
        """), {"lim": limit}).fetchall()
        return {
            "total": len(rows),
            "logs": [{"id": r[0], "url_id": r[1], "language": r[2], "status": r[3],
                      "duration_s": r[4], "items": r[5], "error": r[6], "timestamp": str(r[7])}
                     for r in rows]
        }
    except Exception as e:
        raise HTTPException(500, str(e))


# ─────────────────────────────────────────────────────────────────────────────
# HOTELS
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/hotels", tags=["Hotels"])
def list_hotels(skip: int = 0, limit: int = 100, language: Optional[str] = None,
                db: Session = Depends(get_db)):
    try:
        limit = min(limit, 500)
        params = {"limit": limit, "skip": skip}
        if language:
            q = text("SELECT id, url_id, name, language, address, rating, total_reviews, scraped_at "
                     "FROM hotels WHERE language=:lang ORDER BY scraped_at DESC LIMIT :limit OFFSET :skip")
            params["lang"] = language
        else:
            q = text("SELECT id, url_id, name, language, address, rating, total_reviews, scraped_at "
                     "FROM hotels ORDER BY scraped_at DESC LIMIT :limit OFFSET :skip")
        rows = db.execute(q, params).fetchall()
        return {"total": len(rows), "hotels": [
            {"id": r[0], "url_id": r[1], "name": r[2], "language": r[3], "address": r[4],
             "rating": float(r[5]) if r[5] else None, "reviews": r[6], "scraped_at": str(r[7])}
            for r in rows]}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/hotels/search/", tags=["Hotels"])
def search_hotels(q: str = Query(..., min_length=2), db: Session = Depends(get_db)):
    try:
        rows = db.execute(text(
            "SELECT id, url_id, name, language, address, rating, total_reviews "
            "FROM hotels WHERE LOWER(name) LIKE :q ORDER BY name LIMIT 50"
        ), {"q": f"%{q.lower()}%"}).fetchall()
        return {"query": q, "total": len(rows), "hotels": [
            {"id": r[0], "url_id": r[1], "name": r[2], "language": r[3],
             "address": r[4], "rating": float(r[5]) if r[5] else None, "reviews": r[6]}
            for r in rows]}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/hotels/{hotel_id}", tags=["Hotels"])
def get_hotel(hotel_id: int, db: Session = Depends(get_db)):
    try:
        row = db.execute(text("SELECT * FROM hotels WHERE id=:id"), {"id": hotel_id}).fetchone()
        if not row:
            raise HTTPException(404, "Hotel no encontrado")
        return dict(row._mapping)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))


# ─────────────────────────────────────────────────────────────────────────────
# EXPORT
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/export/csv", tags=["Export"])
def export_csv(language: Optional[str] = None, db: Session = Depends(get_db)):
    try:
        p = Path(settings.EXPORTS_PATH)
        p.mkdir(parents=True, exist_ok=True)
        fname = f"hotels_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        fpath = p / fname
        params = {}
        if language:
            q = text("SELECT * FROM hotels WHERE language=:lang ORDER BY name")
            params["lang"] = language
        else:
            q = text("SELECT * FROM hotels ORDER BY name, language")
        rows = db.execute(q, params).fetchall()
        with open(fpath, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f)
            if rows:
                w.writerow(rows[0]._mapping.keys())
                for r in rows:
                    w.writerow(list(r._mapping.values()))
        return FileResponse(fpath, media_type="text/csv", filename=fname)
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/export/json", tags=["Export"])
def export_json(language: Optional[str] = None, db: Session = Depends(get_db)):
    try:
        params = {}
        if language:
            q = text("SELECT * FROM hotels WHERE language=:lang ORDER BY name")
            params["lang"] = language
        else:
            q = text("SELECT * FROM hotels ORDER BY name, language")
        rows = db.execute(q, params).fetchall()
        return {"total": len(rows), "hotels": [dict(r._mapping) for r in rows]}
    except Exception as e:
        raise HTTPException(500, str(e))


# ─────────────────────────────────────────────────────────────────────────────
# SYSTEM
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/system/info", tags=["System"])
def system_info():
    import platform
    return {
        "platform": platform.system(), "python": platform.python_version(),
        "app_version": "2.1.0",
        "config": {
            "languages": settings.ENABLED_LANGUAGES,
            "batch_size": settings.BATCH_SIZE,
            "max_concurrent": settings.MAX_CONCURRENT_TASKS,
            "use_selenium": settings.USE_SELENIUM,
            "vpn_enabled": settings.VPN_ENABLED,
            "download_images": settings.DOWNLOAD_IMAGES,
        }
    }


@app.get("/system/logs", tags=["System"])
def get_system_logs(lines: int = Query(default=100, ge=1, le=1000)):
    try:
        log_file = Path(settings.LOGS_PATH) / "api.log"
        if not log_file.exists():
            return {"logs": [], "note": "Archivo de log no encontrado"}
        with open(log_file, "r", encoding="utf-8", errors="ignore") as f:
            all_lines = f.readlines()
        return {"total": len(all_lines), "logs": all_lines[-lines:]}
    except Exception as e:
        raise HTTPException(500, str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=False)
