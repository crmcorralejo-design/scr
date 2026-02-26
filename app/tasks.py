"""
BookingScraper/app/tasks.py  (fix SQL injection v2.1 + images_count v2.2)
Tareas Celery para scraping asíncrono - BookingScraper Pro
Windows 11 + Python 3.14.3

CORRECCIONES v2.2:
  [FIX CRITICO] images_count: columna no estaba en el INSERT → siempre 0.
                Ahora se calcula con len(images_urls) y se guarda en BD.
  [FIX] Descarga de imágenes: eliminado límite artificial [:30]. Se descargan TODAS.

CORRECCIONES v1.1:
  [FIX] Columna url_id en INSERT hotels (era url_queue_id en models anterior)
  [FIX] scraped_at: columna ahora existe en URLQueue (añadida en models.py)
  [FIX] review_scores, services, facilities, rooms_info → JSON real (no str/join)
  [FIX] images_urls → JSON real (no join con coma)
  [FIX] text() en todos los raw SQL (SQLAlchemy 2.0)
  [NEW] save_system_metrics(): tarea periódica de métricas del sistema
  [NEW] cleanup_old_logs(): limpieza de logs antiguos (referenciada en beat)
  [FIX] Imports desde app.celery_app (no redefinir Celery aquí)
"""

import json
import time
import psutil
from datetime import datetime, timedelta

from sqlalchemy import text
from loguru import logger

from app.celery_app import celery_app
from app.database import SessionLocal
from app.config import settings


# ═══════════════════════════════════════════════════════════════════════════════
# TAREA PRINCIPAL: SCRAPING DE UN HOTEL
# ═══════════════════════════════════════════════════════════════════════════════

@celery_app.task(bind=True, max_retries=3, name="app.tasks.scrape_hotel_task")
def scrape_hotel_task(self, url_id: int):
    """
    Scrapea un hotel completo en todos los idiomas habilitados.
    [FIX BUG-01 v5.1] Reescritura completa para sincronizar con scraper_service.py v5.0.

    DEFECTOS CORREGIDOS respecto a v2.2:
      [FIX-A] URL construction: ahora usa build_language_url() que elimina sufijo
              existente antes de añadir el nuevo. El código anterior usaba
              base_url.replace(".html", ext+".html") que no strippeaba el sufijo
              previo y producía URLs tipo hotel.es.de.html → HTTP 404.
      [FIX-B] Ordenamiento de idiomas: inglés SIEMPRE primero.
              El código anterior iteraba ENABLED_LANGUAGES tal cual.
      [FIX-C] Detección y validación de idioma: ahora verifica detected_lang antes
              de guardar. El código anterior nunca llamaba _detect_page_language().
      [FIX-D] Condición de descarga de imágenes: ahora usa lang == DEFAULT ('en')
              hardcodeado. El código anterior usaba `lang == base_lang` donde
              base_lang venía de url_queue.language (podía ser cualquier idioma).

    NOTA ARQUITECTÓNICA:
      La solución ideal a largo plazo es delegar completamente a scraper_service.py::scrape_one():
        from app.scraper_service import scrape_one
        return scrape_one(url_id)
      Esto elimina la duplicación de lógica entre las dos rutas de ejecución.
      Mantenido como tarea independiente por compatibilidad con beats existentes.

    Args:
        url_id: ID de la URL en la tabla url_queue
    """
    db = SessionLocal()
    start_time = time.time()
    DEFAULT = settings.DEFAULT_LANGUAGE  # "en"

    try:
        # ── 1. Obtener URL de la BD ────────────────────────────────────────────
        row = db.execute(
            text("SELECT url, language FROM url_queue WHERE id = :id"),
            {"id": url_id}
        ).fetchone()

        if not row:
            logger.error(f"URL ID {url_id} no encontrada en url_queue")
            return {"error": "URL no encontrada"}

        base_url = row[0]
        # queue_lang: idioma de referencia de la URL almacenada (solo para logging)
        queue_lang = row[1] or "en"
        logger.info(f"🔄 [Celery] Procesando URL ID {url_id}: {base_url} (queue_lang={queue_lang})")

        # ── 2. Marcar como 'processing' ───────────────────────────────────────
        db.execute(
            text("""
                UPDATE url_queue
                SET status = 'processing', updated_at = NOW()
                WHERE id = :id
            """),
            {"id": url_id}
        )
        db.commit()

        # ── 3. Importar utilidades de scraping ────────────────────────────────
        from app.scraper import BookingScraper, build_language_url, _detect_page_language
        from app.image_downloader import ImageDownloader

        # [FIX-B] Ordenamiento: inglés PRIMERO, independientemente de LANGUAGES_ENABLED
        languages = settings.ENABLED_LANGUAGES
        if DEFAULT in languages:
            languages = [DEFAULT] + [l for l in languages if l != DEFAULT]
        else:
            logger.warning(
                f"  ⚠️ '{DEFAULT}' no está en LANGUAGES_ENABLED. "
                f"Insertado al inicio para garantizar descarga de imágenes."
            )
            languages = [DEFAULT] + languages

        scraped_count = 0
        first_hotel_name = None
        images_downloaded = False

        for lang in languages:
            try:
                # [FIX-A] build_language_url() elimina sufijo existente antes de añadir el nuevo
                lang_url = build_language_url(base_url, lang)
                logger.info(f"  → [{url_id}] Idioma [{lang}]: {lang_url}")

                with BookingScraper() as scraper:
                    data = scraper.scrape_hotel(lang_url, language=lang)

                if not data or not data.get("name"):
                    logger.warning(f"  ⚠️ [{url_id}][{lang}] Sin datos extraídos")
                    _log_scraping(db, url_id, lang, "no_data",
                                  time.time() - start_time, 0, "No se extrajeron datos")
                    continue

                if first_hotel_name is None:
                    first_hotel_name = data.get("name")

                # [FIX-C] Verificar idioma detectado antes de guardar
                detected = data.get("detected_lang")
                if detected and detected != lang:
                    logger.error(
                        f"  🚫 [{url_id}][{lang}] IDIOMA INCORRECTO — NO SE GUARDA: "
                        f"solicitado='{lang}', página en '{detected}'"
                    )
                    _log_scraping(db, url_id, lang, "lang_mismatch",
                                  time.time() - start_time, 0,
                                  f"Página en '{detected}', solicitado '{lang}'. NO guardado.")
                    continue

                # ── 4. Guardar hotel en BD ─────────────────────────────────────
                db.execute(
                    text("""
                        INSERT INTO hotels (
                            url_id, url, language,
                            name, address, description,
                            rating, total_reviews, rating_category,
                            review_scores, services, facilities,
                            house_rules, important_info, rooms_info,
                            images_urls, images_count, scraped_at, updated_at
                        ) VALUES (
                            :url_id, :url, :language,
                            :name, :address, :description,
                            :rating, :total_reviews, :rating_category,
                            :review_scores::jsonb, :services::jsonb, :facilities::jsonb,
                            :house_rules, :important_info, :rooms_info::jsonb,
                            :images_urls::jsonb, :images_count, NOW(), NOW()
                        )
                        ON CONFLICT (url_id, language) DO UPDATE SET
                            name            = EXCLUDED.name,
                            address         = EXCLUDED.address,
                            description     = EXCLUDED.description,
                            rating          = EXCLUDED.rating,
                            total_reviews   = EXCLUDED.total_reviews,
                            rating_category = EXCLUDED.rating_category,
                            review_scores   = EXCLUDED.review_scores,
                            services        = EXCLUDED.services,
                            facilities      = EXCLUDED.facilities,
                            house_rules     = EXCLUDED.house_rules,
                            important_info  = EXCLUDED.important_info,
                            rooms_info      = EXCLUDED.rooms_info,
                            images_urls     = EXCLUDED.images_urls,
                            images_count    = EXCLUDED.images_count,
                            updated_at      = NOW()
                    """),
                    {
                        "url_id":           url_id,
                        "url":              lang_url,
                        "language":         lang,
                        "name":             data.get("name"),
                        "address":          data.get("address"),
                        "description":      data.get("description"),
                        "rating":           data.get("rating"),
                        "total_reviews":    data.get("total_reviews"),
                        "rating_category":  data.get("rating_category"),
                        "review_scores":    json.dumps(data.get("review_scores") or {}),
                        "services":         json.dumps(data.get("services")      or []),
                        "facilities":       json.dumps(data.get("facilities")    or {}),
                        "house_rules":      data.get("house_rules"),
                        "important_info":   data.get("important_info"),
                        "rooms_info":       json.dumps(data.get("rooms")         or []),
                        "images_urls":      json.dumps(data.get("images_urls")   or []),
                        "images_count":     len(data.get("images_urls")          or []),
                    }
                )
                db.commit()

                scraped_count += 1
                duration = time.time() - start_time
                _log_scraping(db, url_id, lang, "completed", duration,
                              len(data.get("images_urls") or []))

                logger.success(
                    f"  ✓ [{url_id}][{lang}] '{first_hotel_name}' "
                    f"| rating={data.get('rating')} "
                    f"| imgs={len(data.get('images_urls') or [])}"
                )

                # ── 5. Descargar imágenes (solo en inglés confirmado) ──────────
                # [FIX-D] Condición corregida: lang == DEFAULT (hardcodeado 'en')
                # ANTES v2.2: `lang == base_lang` donde base_lang = url_queue.language
                #   (podía ser 'es', 'de', etc. → imágenes en carpeta errónea o nunca descargadas)
                # AHORA v5.1: siempre lang == 'en' → siempre carpeta hotel_{id}/en/
                if lang == DEFAULT and not images_downloaded and settings.DOWNLOAD_IMAGES:
                    img_urls = data.get("images_urls") or []
                    if img_urls:
                        try:
                            downloader = ImageDownloader()
                            results = downloader.download_images(url_id, img_urls, language=DEFAULT)
                            n_ok = len(results)
                            if n_ok > 0:
                                db.execute(
                                    text("""
                                        UPDATE hotels
                                        SET images_count = :count, updated_at = NOW()
                                        WHERE url_id = :uid AND language = :lang
                                    """),
                                    {"count": n_ok, "uid": url_id, "lang": DEFAULT}
                                )
                                db.commit()
                            logger.info(f"  📷 [{url_id}] {n_ok}/{len(img_urls)} imágenes descargadas (en/)")
                        except Exception as img_err:
                            logger.warning(f"  ⚠️ [{url_id}] Error imágenes: {img_err}")
                    images_downloaded = True

            except Exception as lang_err:
                logger.error(f"  ✗ [{url_id}][{lang}] {lang_err}")
                _log_scraping(db, url_id, lang, "error",
                              time.time() - start_time, 0, str(lang_err)[:500])
                try:
                    db.rollback()
                except Exception:
                    pass

        # ── 6. Actualizar URL queue ────────────────────────────────────────────
        new_status = "completed" if scraped_count > 0 else "failed"
        # ✅ FIX: scraped_at ahora existe en URLQueue (añadida en models.py)
        db.execute(
            text("""
                UPDATE url_queue
                SET status = :status, scraped_at = NOW(), updated_at = NOW()
                WHERE id = :id
            """),
            {"status": new_status, "id": url_id}
        )
        db.commit()

        total_duration = time.time() - start_time
        logger.success(
            f"✅ {url_id} → {new_status} | {scraped_count}/{len(languages)} idiomas "
            f"| {total_duration:.1f}s | {first_hotel_name}"
        )

        return {
            "success":    scraped_count > 0,
            "hotel_name": first_hotel_name,
            "languages":  scraped_count,
            "duration":   round(total_duration, 2),
        }

    except Exception as e:
        logger.error(f"❌ Error fatal en task URL {url_id}: {e}")
        db.rollback()

        # Marcar como fallida y registrar error
        try:
            db.execute(
                text("""
                    UPDATE url_queue
                    SET status = CASE
                            WHEN retry_count + 1 >= max_retries THEN 'failed'
                            ELSE 'pending'
                        END,
                        retry_count = retry_count + 1,
                        last_error  = :error,
                        updated_at  = NOW()
                    WHERE id = :id
                """),
                {"id": url_id, "error": str(e)[:500]}
            )
            db.commit()
        except Exception:
            pass

        # Reintentar si no superó el máximo
        if self.request.retries < self.max_retries:
            logger.info(f"🔄 Reintentando URL {url_id} (intento {self.request.retries + 1})")
            raise self.retry(exc=e, countdown=settings.RETRY_DELAY)

        return {"error": str(e)}

    finally:
        db.close()


# ═══════════════════════════════════════════════════════════════════════════════
# TAREA DE DESPACHO: BATCH DE URLs PENDIENTES
# ═══════════════════════════════════════════════════════════════════════════════

@celery_app.task(name="app.tasks.process_pending_urls")
def process_pending_urls(batch_size: int = 5):
    """
    Despacha un lote de URLs pendientes al worker.
    Ejecutada periódicamente por Celery Beat (cada 30s).

    Args:
        batch_size: Número máximo de URLs a despachar por ciclo
    """
    db = SessionLocal()
    try:
        rows = db.execute(
            text("""
                SELECT id FROM url_queue
                WHERE status = 'pending'
                  AND retry_count < max_retries
                ORDER BY priority DESC, created_at ASC
                LIMIT :limit
            """),
            {"limit": batch_size}
        ).fetchall()

        url_ids = [r[0] for r in rows]

        if not url_ids:
            logger.debug("ℹ️ No hay URLs pendientes")
            return {"dispatched": 0}

        for uid in url_ids:
            scrape_hotel_task.delay(uid)

        logger.info(f"🚀 Despachadas {len(url_ids)} tareas de scraping")
        return {"dispatched": len(url_ids), "url_ids": url_ids}

    finally:
        db.close()


# ═══════════════════════════════════════════════════════════════════════════════
# TAREA PERIÓDICA: MÉTRICAS DEL SISTEMA
# ═══════════════════════════════════════════════════════════════════════════════

@celery_app.task(name="app.tasks.save_system_metrics")
def save_system_metrics():
    """
    Captura y guarda métricas del sistema cada 5 minutos.
    Referenciada en beat_schedule de celery_app.py.
    """
    db = SessionLocal()
    try:
        # Stats de URL queue
        stats = db.execute(text("""
            SELECT
                COUNT(*) FILTER (WHERE status = 'pending')    AS pending,
                COUNT(*) FILTER (WHERE status = 'processing') AS processing,
                COUNT(*) FILTER (WHERE status = 'completed')  AS completed,
                COUNT(*) FILTER (WHERE status = 'failed')     AS failed
            FROM url_queue
        """)).fetchone()

        hotels_total = db.execute(
            text("SELECT COUNT(*) FROM hotels")
        ).scalar() or 0

        images_total = db.execute(
            text("SELECT COALESCE(SUM(images_count), 0) FROM hotels")
        ).scalar() or 0

        # Recursos del sistema
        cpu    = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory().percent
        disk   = psutil.disk_usage("C:\\").percent if psutil.disk_usage.__doc__ else 0.0

        db.execute(
            text("""
                INSERT INTO system_metrics (
                    urls_pending, urls_processing, urls_completed, urls_failed,
                    hotels_scraped, images_downloaded,
                    cpu_usage, memory_usage, disk_usage,
                    recorded_at
                ) VALUES (
                    :pending, :processing, :completed, :failed,
                    :hotels, :images,
                    :cpu, :memory, :disk,
                    NOW()
                )
            """),
            {
                "pending":    stats[0] or 0,
                "processing": stats[1] or 0,
                "completed":  stats[2] or 0,
                "failed":     stats[3] or 0,
                "hotels":     hotels_total,
                "images":     images_total,
                "cpu":        cpu,
                "memory":     memory,
                "disk":       disk,
            }
        )
        db.commit()
        logger.debug(
            f"📊 Métricas guardadas | CPU:{cpu}% MEM:{memory}% "
            f"pending:{stats[0]} completed:{stats[2]}"
        )
        return {"recorded": True}

    except Exception as e:
        logger.error(f"Error guardando métricas: {e}")
        return {"error": str(e)}
    finally:
        db.close()


# ═══════════════════════════════════════════════════════════════════════════════
# TAREA PERIÓDICA: LIMPIEZA DE LOGS
# ═══════════════════════════════════════════════════════════════════════════════

@celery_app.task(name="app.tasks.cleanup_old_logs")
def cleanup_old_logs(days: int = 30):
    """
    Elimina logs de scraping con más de `days` días de antigüedad.
    Referenciada en beat_schedule de celery_app.py.
    """
    db = SessionLocal()
    try:
        # [FIX v2.1] Usar bind parameter para evitar injection de SQL.
        # El pattern .replace() anterior era vulnerable si days llegara
        # de fuente externa. Aqui usamos el multiplicador nativo de INTERVAL.
        result = db.execute(
            text("""
                DELETE FROM scraping_logs
                WHERE timestamp < NOW() - (INTERVAL '1 day' * :days)
            """),
            {"days": days}
        )
        db.commit()
        deleted = result.rowcount
        logger.info(f"🧹 Logs limpiados: {deleted} registros eliminados (>{days} días)")
        return {"deleted": deleted}
    except Exception as e:
        logger.error(f"Error limpiando logs: {e}")
        return {"error": str(e)}
    finally:
        db.close()


# ═══════════════════════════════════════════════════════════════════════════════
# UTILIDAD INTERNA
# ═══════════════════════════════════════════════════════════════════════════════

def _log_scraping(
    db, url_id: int, language: str,
    status: str, duration: float,
    items: int, error: str = None
):
    """Inserta una línea en scraping_logs."""
    try:
        db.execute(
            text("""
                INSERT INTO scraping_logs
                    (url_id, language, status, duration_seconds, items_extracted, error_message, timestamp)
                VALUES
                    (:url_id, :lang, :status, :dur, :items, :error, NOW())
            """),
            {
                "url_id": url_id,
                "lang":   language,
                "status": status,
                "dur":    round(duration, 2),
                "items":  items,
                "error":  error,
            }
        )
        db.commit()
    except Exception as e:
        logger.debug(f"No se pudo registrar log: {e}")
