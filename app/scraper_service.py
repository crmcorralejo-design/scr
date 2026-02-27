"""
BookingScraper/app/scraper_service.py  v5.1  [FIX NO-OVERWRITE + images_count]
Servicio de scraping directo - BookingScraper Pro

CAMBIOS v5.1 [FIX INTEGRIDAD DE DATOS - NO SOBRESCRIBIR]:

  DIAGNÓSTICO v5.0 → v5.1:
  ──────────────────────────────────────────────────────────────
  ● _save_hotel() usaba ON CONFLICT (url_id, language) DO UPDATE SET que
    sobrescribía TODOS los campos de un hotel ya scraped si se ejecutaba
    de nuevo. Esto violaba el requisito "No sobrescribir datos" y podía
    contaminar registros correctos con datos de un rescraping fallido o
    parcial (por ejemplo, si el idioma incorrecto se intentaba de nuevo).
    CAUSA: Si el orden de procesamiento cambiaba, un rescraping podía
    guardar datos del idioma incorrecto sobre datos correctos previamente
    almacenados.

  [FIX #36] _save_hotel(): ON CONFLICT DO UPDATE → ON CONFLICT DO NOTHING
    ANTES v5.0: ON CONFLICT (url_id, language) DO UPDATE SET (todos los campos)
    AHORA v5.1: ON CONFLICT (url_id, language) DO NOTHING
    EFECTO: Si ya existe un registro hotel para (url_id, language), se respeta
    el dato existente. Para forzar rescraping, el operador debe eliminar el
    registro de la tabla hotels explícitamente (acción deliberada).
    JUSTIFICACIÓN: El dato correcto es el primero que se guarda con el idioma
    verificado. Los reintentos de scraping no deben sobrescribirlo.

  [FIX #37] _save_hotel(): images_count añadido al INSERT con valor 0.
    ANTES v5.0: images_count ausente del INSERT — su valor dependía del
    DEFAULT del schema de BD (podía ser NULL en lugar de 0 si el schema
    no tenía DEFAULT explícito).
    AHORA v5.1: images_count=0 explícito en el INSERT. El valor real se
    actualiza en el UPDATE posterior a la descarga de imágenes.
    EFECTO: images_count nunca es NULL después del INSERT; siempre empieza
    en 0 y se incrementa a medida que las imágenes se descargan.

CAMBIOS v5.0 [COORDINADO CON scraper.py v5.0]:

  [FIX #30] VPN: preferir UK (GB) en lugar de US para scraping en inglés.
    Con URL formato hotel.en.html?lang=en-gb, una IP del Reino Unido garantiza
    que Booking.com sirva contenido British English sin re-mapeo por GeoIP.
    process_batch() intenta conectar a UK primero; si falla, cualquier país anglófono.
    scrape_one() también actualiza preferencia VPN a UK.

  [FIX #31] _detect_page_language(): reconocer 'en-gb' como idioma 'en'.
    En el scraper, _detect_page_language() extrae primeros 2 caracteres, por lo que
    'en-gb' → 'en'. No requiere cambio. Documentado para claridad.

CAMBIOS v4.0 [FIX CRÍTICO - BLOQUEAR GUARDADO EN IDIOMA INCORRECTO]:
  [FIX #20] Mismatch de idioma → NO GUARDAR, loguear como 'lang_mismatch'
  [FIX #21] Imágenes: solo descargar cuando lang='en' Y detected_lang='en'
  [FIX #22] Retry en mismatch de idioma (solo para lang='en')
  [FIX #23] Estadísticas: nuevo campo 'lang_mismatch_blocked'

CAMBIOS v3.1: imágenes del primer idioma exitoso + images_count fix + mismatch warning.
CAMBIOS v3.0: VPN US-first + American English (en-us). ← reemplazado por v5.0 UK-first + en-gb.
CAMBIOS v2.x: mejoras incrementales.
"""

import json
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Set
from loguru import logger

from sqlalchemy import text

from app.database import SessionLocal
from app.config import settings


# ── POOL DE THREADS ────────────────────────────────────────────────────────────
# [FIX v2.3] max_workers=1: UN hotel a la vez elimina toda la contención de VPN,
#            DB pool y DNS. Aumentar a 2-3 solo cuando el sistema sea estable.
_executor = ThreadPoolExecutor(
    max_workers=1,
    thread_name_prefix="scraper"
)

# Conjunto de IDs actualmente en proceso
_lock = threading.Lock()
_active_ids: Set[int] = set()

# Contador global de estado
_stats = {
    "total_dispatched": 0,
    "total_completed": 0,
    "total_failed": 0,
    "currently_processing": 0,
    "consecutive_failures": 0,    # detecta bloqueo IP
    "hotels_since_vpn_rotate": 0, # contador para rotación periódica VPN
    "lang_mismatch_count": 0,     # mismatches de idioma consecutivos → rota VPN al llegar a 3
    "lang_mismatch_blocked": 0,   # [v4.0] registros rechazados por idioma incorrecto (no guardados)
}

# [v4.0] Máximo de reintentos cuando Booking.com devuelve idioma incorrecto para 'en'
_MAX_LANG_RETRY = 2
_stats_lock = threading.Lock()


# ── VPN MANAGER SINGLETON ──────────────────────────────────────────────────────
# Se inicializa una sola vez y se comparte entre todos los threads del scraper
_vpn_manager = None
_vpn_lock = threading.Lock()


def _get_vpn_manager():
    """
    Devuelve el VPN manager singleton (thread-safe).
    Solo se inicializa si VPN_ENABLED=True en .env
    """
    global _vpn_manager
    if not settings.VPN_ENABLED:
        return None

    with _vpn_lock:
        if _vpn_manager is None:
            try:
                from app.vpn_manager import vpn_manager_factory
                _vpn_manager = vpn_manager_factory(interactive=False)
                logger.info("✓ VPN Manager iniciado (singleton)")
            except Exception as e:
                logger.error(f"✗ Error iniciando VPN Manager: {e}")
                _vpn_manager = None
    return _vpn_manager


def rotate_vpn_now() -> Dict:
    """
    Rota la VPN inmediatamente.
    Llamado desde el endpoint /vpn/rotate de main.py
    """
    vpn = _get_vpn_manager()
    if not vpn:
        return {"success": False, "reason": "VPN_ENABLED=False o VPN no disponible"}

    with _vpn_lock:
        try:
            logger.info("🔄 Rotación VPN manual solicitada...")
            success = vpn.rotate()
            with _stats_lock:
                _stats["consecutive_failures"] = 0
                _stats["hotels_since_vpn_rotate"] = 0
            return {
                "success": success,
                "new_ip": vpn.current_ip,
                "server": vpn.current_server,
            }
        except Exception as e:
            logger.error(f"✗ Error rotando VPN: {e}")
            return {"success": False, "error": str(e)}


def get_vpn_status() -> Dict:
    """Estado de la VPN. Llamado desde /vpn/status en main.py."""
    vpn = _get_vpn_manager()
    if not vpn:
        return {
            "enabled": False,
            "reason": "VPN_ENABLED=False en .env",
        }
    try:
        return {
            "enabled": True,
            **vpn.get_status(),
            "hotels_since_rotate": _stats.get("hotels_since_vpn_rotate", 0),
            "consecutive_failures": _stats.get("consecutive_failures", 0),
        }
    except Exception as e:
        return {"enabled": True, "error": str(e)}


def _maybe_rotate_vpn(force: bool = False):
    """
    Rota la VPN si:
    - force=True (llamada explícita)
    - Se superó el límite de hoteles por servidor (VPN_ROTATE_EVERY_N default=10)
    - Hay 3+ fallos consecutivos (posible bloqueo IP)
    Thread-safe — usa _vpn_lock para evitar rotaciones simultáneas.
    """
    vpn = _get_vpn_manager()
    if not vpn:
        return

    with _stats_lock:
        consec = _stats["consecutive_failures"]
        since_rotate = _stats["hotels_since_vpn_rotate"]

    rotate_every = getattr(settings, "VPN_ROTATE_EVERY_N", 10)
    too_many_failures = consec >= 3

    if not (force or since_rotate >= rotate_every or too_many_failures):
        return

    reason = "manual" if force else ("bloqueo_ip" if too_many_failures else "periodica")
    logger.info(f"🔄 Rotando VPN (motivo={reason}, fallos_consec={consec}, hoteles={since_rotate})...")

    with _vpn_lock:
        try:
            success = vpn.rotate()
            if success:
                with _stats_lock:
                    _stats["consecutive_failures"] = 0
                    _stats["hotels_since_vpn_rotate"] = 0
                logger.success(f"✓ VPN rotada → IP: {vpn.current_ip}")
            else:
                logger.warning("⚠️ Rotación VPN falló — continuando con IP actual")
        except Exception as e:
            logger.error(f"✗ Error en rotación VPN: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# PUNTO DE ENTRADA: PROCESAR BATCH
# ═══════════════════════════════════════════════════════════════════════════════

def process_batch(batch_size: int = 5) -> Dict:
    """
    Obtiene URLs pendientes de la BD y las envía al thread pool.
    Thread-safe. Puede llamarse desde asyncio (via run_in_executor).
    """
    # [FIX v5.0] VPN al iniciar el primer batch: conectar a UK preferentemente.
    # Con URL formato hotel.en.html?lang=en-gb, una IP del Reino Unido garantiza
    # que Booking.com sirva British English sin re-mapeo por GeoIP.
    # ANTES v3.0: US-first para en-us → fallaba por Cloudflare con IP americana
    # AHORA v5.0: UK-first para en-gb → IP anglófona nativa para formato en-gb
    vpn = _get_vpn_manager()
    if vpn and settings.VPN_ENABLED:
        try:
            if not vpn.verify_vpn_active():
                logger.warning("⚠️ VPN inactiva al procesar batch — conectando a UK...")
                # Preferir UK para scraping en inglés GB; si falla, connect() elige aleatorio
                success = vpn.connect("UK")
                if not success:
                    logger.warning("⚠️ Conexión a UK falló — intentando cualquier país...")
                    vpn.connect()
        except Exception as e:
            logger.warning(f"⚠️ Error verificando VPN: {e}")

    db = SessionLocal()
    try:
        rows = db.execute(
            text("""
                SELECT id FROM url_queue
                WHERE  status = 'pending'
                  AND  retry_count < max_retries
                ORDER BY priority DESC, created_at ASC
                LIMIT  :limit
            """),
            {"limit": batch_size}
        ).fetchall()

        url_ids = [r[0] for r in rows]

        if not url_ids:
            logger.debug("ℹ️ No hay URLs pendientes para despachar")
            return {"dispatched": 0, "message": "No hay URLs pendientes"}

        # Filtrar IDs ya en proceso
        with _lock:
            new_ids = [uid for uid in url_ids if uid not in _active_ids]
            _active_ids.update(new_ids)

        if not new_ids:
            return {"dispatched": 0, "message": "Todas las URLs ya están en proceso"}

        # Marcar como 'processing' en BD
        for uid in new_ids:
            db.execute(
                text("""
                    UPDATE url_queue
                    SET status = 'processing', updated_at = NOW()
                    WHERE id = :id
                """),
                {"id": uid}
            )
        db.commit()

        # Enviar al pool de threads
        for uid in new_ids:
            _executor.submit(_run_safe, uid)

        with _stats_lock:
            _stats["total_dispatched"] += len(new_ids)
            _stats["currently_processing"] += len(new_ids)

        logger.info(f"🚀 Despachadas {len(new_ids)} URLs al thread pool")
        return {"dispatched": len(new_ids), "url_ids": new_ids}

    except Exception as e:
        logger.error(f"Error en process_batch: {e}")
        return {"dispatched": 0, "error": str(e)}
    finally:
        db.close()


# ═══════════════════════════════════════════════════════════════════════════════
# SCRAPING DE UN HOTEL INDIVIDUAL
# ═══════════════════════════════════════════════════════════════════════════════

def _run_safe(url_id: int):
    """Wrapper que libera el ID del set _active_ids al terminar."""
    try:
        scrape_one(url_id)
    except Exception as e:
        logger.error(f"Error inesperado en _run_safe({url_id}): {e}")
    finally:
        with _lock:
            _active_ids.discard(url_id)
        with _stats_lock:
            _stats["currently_processing"] = max(0, _stats["currently_processing"] - 1)


def scrape_one(url_id: int) -> Dict:
    """
    Scrapea un hotel completo en todos los idiomas habilitados.

    CAMBIOS v2.1:
    - Con Selenium: crea UN SOLO driver por hotel (no uno por idioma)
    - Con cloudscraper: reinicia sesión si hay bloqueo repetido
    - Registra fallos consecutivos para disparar rotación VPN
    """
    db = SessionLocal()
    start_time = time.time()

    try:
        row = db.execute(
            text("SELECT url, language FROM url_queue WHERE id = :id"),
            {"id": url_id}
        ).fetchone()

        if not row:
            logger.error(f"URL ID {url_id} no encontrada en url_queue")
            return {"error": "URL no encontrada"}

        base_url   = row[0]
        # [FIX BUG #2] queue_lang es el idioma de la URL almacenada. Se usa para
        # logging y trazabilidad. La normalización real ocurre en build_language_url()
        # que ahora elimina el sufijo existente antes de añadir el nuevo (ver BUG #1).
        queue_lang = row[1] or "en"
        logger.info(f"\n{'─'*60}")
        logger.info(f"🏨 Iniciando scraping | ID={url_id} | lang_queue={queue_lang} | {base_url}")
        logger.info(f"{'─'*60}")

        # ── Verificar VPN antes de este hotel ─────────────────────────────────
        vpn = _get_vpn_manager()
        if vpn and settings.VPN_ENABLED:
            try:
                # [FIX v2.3] Proteger con _vpn_lock: evita que múltiples threads
                # llamen reconnect simultáneamente → múltiples connect() simultáneos
                # → NordVPN CLI colapsaba → DNS inestable → ERR_NAME_NOT_RESOLVED
                with _vpn_lock:
                    vpn.reconnect_if_disconnected()
            except Exception as vpn_err:
                logger.warning(f"⚠️ VPN check error: {vpn_err}")

        from app.scraper import BookingScraper, build_language_url

        languages = settings.ENABLED_LANGUAGES
        scraped_count = 0
        hotel_name = None
        lang_failures = 0

        # [FIX v2.4] Ingles SIEMPRE primero, sin excepcion.
        # Garantiza: (a) imagenes descargadas con sesion/cookies de la URL base (.html),
        # (b) independiente del orden en LANGUAGES_ENABLED del .env.
        # Si 'en' no esta en la lista, se inserta al inicio automaticamente.
        DEFAULT = settings.DEFAULT_LANGUAGE  # "en"
        if DEFAULT in languages:
            # Mover 'en' al frente si no esta ya
            languages = [DEFAULT] + [l for l in languages if l != DEFAULT]
        else:
            # 'en' no configurado → insertar al inicio para garantizar descarga de imagenes
            logger.warning(
                f"  ⚠️ '{DEFAULT}' no esta en LANGUAGES_ENABLED. "
                f"Se inserta al inicio para descarga de imagenes."
            )
            languages = [DEFAULT] + languages

        # [v3.1] Flag: imágenes descargadas del PRIMER idioma exitoso.
        # [v4.0] Cambiado: solo se descargan cuando lang='en' Y idioma confirmado correcto.
        images_downloaded = False

        # [v4.0] Contador de reintentos por mismatch de idioma (solo para lang='en')
        lang_retry_count = 0

        # ── [FIX] Con Selenium: crear driver UNA sola vez por hotel ──────────
        # Esto evita abrir/cerrar Brave 18 veces por hotel
        if settings.USE_SELENIUM:
            scraper_instance = BookingScraper()
            scraper_context = scraper_instance  # ya es una instancia, no context manager
        else:
            scraper_instance = None

        try:
            for lang in languages:
                lang_url = build_language_url(base_url, lang)
                logger.info(f"  → [{url_id}] Idioma [{lang}]: {lang_url}")

                try:
                    if settings.USE_SELENIUM:
                        # Reusar el driver ya abierto
                        data = scraper_instance.scrape_hotel(lang_url, language=lang)
                    else:
                        # cloudscraper: usar context manager (gestiona sesión)
                        with BookingScraper() as scraper:
                            data = scraper.scrape_hotel(lang_url, language=lang)

                    if not data or not data.get("name"):
                        logger.warning(f"  ⚠️ [{url_id}][{lang}] Sin datos")
                        _log(db, url_id, lang, "no_data",
                             time.time() - start_time, 0, "Sin datos extraídos")
                        lang_failures += 1
                        continue

                    if hotel_name is None:
                        hotel_name = data["name"]

                    # [v4.0 FIX #20] Verificar idioma ANTES de guardar.
                    # ANTES v3.1: mismatch → warning + guardar igualmente (¡INCORRECTO!)
                    # AHORA v4.0: mismatch → NO guardar + log 'lang_mismatch'
                    detected = data.get("detected_lang")
                    if detected and detected != lang:
                        logger.error(
                            f"  🚫 [{url_id}][{lang}] IDIOMA INCORRECTO — NO SE GUARDA: "
                            f"solicitado='{lang}', página en '{detected}' "
                            f"(GeoIP/VPN devuelve sesión en idioma incorrecto)"
                        )
                        with _stats_lock:
                            _stats["lang_mismatch_count"] = _stats.get("lang_mismatch_count", 0) + 1
                            _stats["lang_mismatch_blocked"] = _stats.get("lang_mismatch_blocked", 0) + 1

                        # Loguear en BD como mismatch (no como 'completed')
                        _log(db, url_id, lang, "lang_mismatch",
                             time.time() - start_time, 0,
                             f"Página en '{detected}', solicitado '{lang}'. Dato NO guardado.")

                        # [v4.0 FIX #22] Retry solo para inglés (idioma principal)
                        # Los otros idiomas no se reintentan (menos críticos)
                        #
                        # [FIX BUG-02 v5.1] ELIMINADO el bloque `continue` de reintento.
                        # CAUSA DEL BUG ORIGINAL: `continue` en `for lang in languages:`
                        #   avanza al SIGUIENTE idioma — NO reintenta el mismo lang.
                        #   El comentario anterior "# ← Reintentar el mismo lang" era INCORRECTO.
                        #
                        # POR QUÉ EL BLOQUE ES REDUNDANTE:
                        #   scraper.py::BookingScraperCloudScraper.scrape_hotel() ya maneja
                        #   internamente el retry por mismatch de idioma para lang='en' mediante
                        #   `lang_mismatch_retries` dentro del bucle `for attempt in range(...)`,
                        #   donde `continue` SÍ reintenta correctamente el mismo URL.
                        #   Cuando scrape_hotel() agota esos reintentos, retorna None.
                        #   Aquí en scraper_service.py solo necesitamos manejar ese None.
                        #
                        # COMPORTAMIENTO ACTUAL:
                        #   Si llegamos aquí con `detected != lang`, significa que scraper.py
                        #   ya intentó N veces y devolvió datos con idioma incorrecto (no None).
                        #   No guardamos los datos. Para 'en', rotamos VPN si hay mismatches acumulados.
                        if lang == DEFAULT:
                            with _stats_lock:
                                _stats["lang_mismatch_count"] = _stats.get("lang_mismatch_count", 0) + 1
                            if _stats.get("lang_mismatch_count", 0) >= 3:
                                logger.warning(
                                    f"  🔄 [{url_id}] {_stats['lang_mismatch_count']} mismatches "
                                    f"de idioma consecutivos → rotando VPN..."
                                )
                                _maybe_rotate_vpn(force=True)
                                with _stats_lock:
                                    _stats["lang_mismatch_count"] = 0

                        lang_failures += 1
                        lang_retry_count = 0  # reset para próximo idioma
                        continue  # ← SKIP SAVE — no guardar dato en idioma incorrecto
                    else:
                        # Idioma correcto → reset contadores
                        with _stats_lock:
                            _stats["lang_mismatch_count"] = 0
                        lang_retry_count = 0

                    # Guardar en BD
                    # [FIX #36 v5.1] _save_hotel() usa ON CONFLICT DO NOTHING:
                    # si el registro ya existe para (url_id, lang), se preserva
                    # el dato existente sin sobrescribirlo.
                    saved = _save_hotel(db, url_id, lang_url, lang, data)
                    if saved:
                        scraped_count += 1
                        lang_failures = 0  # reset fallos consecutivos por idioma
                    else:
                        # Registro ya existía — no se sobrescribe (req. 6d)
                        logger.debug(
                            f"  ℹ️ [{url_id}][{lang}] Registro ya existe — "
                            f"preservando dato existente (ON CONFLICT DO NOTHING)"
                        )
                        # Se cuenta igual como procesado exitoso
                        scraped_count += 1

                    imgs_count = len(data.get("images_urls") or [])
                    duration = time.time() - start_time
                    # items_extracted = 1 por registro guardado (no numero de imagenes)
                    _log(db, url_id, lang, "completed", duration, 1)

                    logger.success(
                        f"  ✓ [{url_id}][{lang}] '{hotel_name}' "
                        f"| rating={data.get('rating')} "
                        f"| imgs={imgs_count}"
                    )

                    # ── Descarga de imágenes ────────────────────────────────
                    # [v4.0 FIX #21] Solo descargar imágenes cuando:
                    #   1. lang == DEFAULT ('en')  — idioma principal
                    #   2. detected_lang == 'en'   — página confirmada en inglés
                    #   3. Aún no se han descargado para este hotel
                    # ANTES v3.1: "primer idioma exitoso" (podía ser ES con contenido ES)
                    # AHORA v4.0: solo inglés confirmado → carpeta destino SIEMPRE 'en'
                    if lang == DEFAULT and not images_downloaded and settings.DOWNLOAD_IMAGES:
                        imgs = data.get("images_urls") or []
                        if imgs:
                            driver = scraper_instance.driver if settings.USE_SELENIUM else None
                            # Hardcodear lang='en' como carpeta destino (regla: siempre en/)
                            n_downloaded = _download_images(url_id, imgs, DEFAULT, driver=driver)
                            if n_downloaded and n_downloaded > 0:
                                try:
                                    db.execute(
                                        text("""
                                            UPDATE hotels
                                            SET images_count = :count, updated_at = NOW()
                                            WHERE url_id = :url_id AND language = :lang
                                        """),
                                        {"count": n_downloaded, "url_id": url_id, "lang": DEFAULT}
                                    )
                                    db.commit()
                                    logger.debug(
                                        f"  📷 [{url_id}] images_count={n_downloaded} "
                                        f"actualizado en BD (lang='{DEFAULT}', carpeta=en/)"
                                    )
                                except Exception as upd_err:
                                    logger.debug(f"  ⚠️ No se pudo actualizar images_count: {upd_err}")
                        images_downloaded = True
                        logger.debug(f"  📷 [{url_id}] Imágenes descargadas y marcadas (lang='{DEFAULT}')")

                except Exception as lang_err:
                    err_str = str(lang_err)
                    logger.error(f"  ✗ [{url_id}][{lang}] {err_str[:200]}")
                    try:
                        db.rollback()
                    except Exception:
                        pass

                    # [FIX v2.3] Session Selenium muerta (browser crasheo) → recrear driver y reintentar
                    if settings.USE_SELENIUM and "invalid session id" in err_str.lower():
                        logger.warning(f"  ⚠️ [{url_id}][{lang}] Brave crasheo — recreando driver y reintentando...")
                        try:
                            scraper_instance.close()
                        except Exception:
                            pass
                        try:
                            scraper_instance = BookingScraper()
                            data = scraper_instance.scrape_hotel(lang_url, language=lang)
                            if data and data.get("name"):
                                if hotel_name is None:
                                    hotel_name = data["name"]
                                _save_hotel(db, url_id, lang_url, lang, data)
                                scraped_count += 1
                                lang_failures = 0
                                duration = time.time() - start_time
                                _log(db, url_id, lang, "completed", duration,
                                     len(data.get("images_urls") or []))
                                logger.success(
                                    f"  ✓ [{url_id}][{lang}] '{hotel_name}' (recuperado) "
                                    f"| rating={data.get('rating')}"
                                )
                                # [FIX v3.1] Recovery: descargar imágenes SIEMPRE como lang=DEFAULT ('en')
                                # [FIX BUG-04 v5.1] El código original usaba `lang` (el idioma
                                # actual del bucle) en lugar de DEFAULT. Si el crash ocurrió
                                # procesando 'es' o 'de', las imágenes se guardaban en hotel_{id}/es/
                                # en vez de hotel_{id}/en/. Regla del sistema: siempre carpeta 'en/'.
                                if not images_downloaded and settings.DOWNLOAD_IMAGES:
                                    imgs = data.get("images_urls") or []
                                    if imgs:
                                        driver = scraper_instance.driver if settings.USE_SELENIUM else None
                                        n_downloaded = _download_images(url_id, imgs, DEFAULT, driver=driver)
                                        if n_downloaded and n_downloaded > 0:
                                            try:
                                                db.execute(
                                                    text("UPDATE hotels SET images_count = :c, updated_at = NOW() WHERE url_id = :u"),
                                                    {"c": n_downloaded, "u": url_id}
                                                )
                                                db.commit()
                                            except Exception:
                                                pass
                                    images_downloaded = True
                                continue  # siguiente idioma con exito
                        except Exception as retry_err:
                            logger.error(f"  ✗ [{url_id}][{lang}] Reintento fallido: {retry_err}")
                            try:
                                db.rollback()
                            except Exception:
                                pass

                    _log(db, url_id, lang, "error",
                         time.time() - start_time, 0, err_str[:500])
                    lang_failures += 1

                    if lang_failures >= 3:
                        logger.warning(f"  ⚠️ [{url_id}] {lang_failures} fallos seguidos — posible bloqueo IP")
                        with _stats_lock:
                            _stats["consecutive_failures"] += 1
                        _maybe_rotate_vpn()

        finally:
            # Cerrar el driver Selenium al terminar TODOS los idiomas del hotel
            if settings.USE_SELENIUM and scraper_instance is not None:
                try:
                    scraper_instance.close()
                    logger.debug(f"  ✓ Driver Selenium cerrado para hotel {url_id}")
                except Exception:
                    pass

        # ── Actualizar estado final ────────────────────────────────────────────
        final_status = "completed" if scraped_count > 0 else "failed"
        db.execute(
            text("""
                UPDATE url_queue
                SET status = :status, scraped_at = NOW(), updated_at = NOW()
                WHERE id = :id
            """),
            {"status": final_status, "id": url_id}
        )
        db.commit()

        total_dur = time.time() - start_time

        if scraped_count > 0:
            with _stats_lock:
                _stats["total_completed"] += 1
                _stats["consecutive_failures"] = 0  # éxito → reset fallos
                _stats["hotels_since_vpn_rotate"] += 1

            # ¿Es momento de rotar la VPN periódicamente?
            _maybe_rotate_vpn()

            logger.success(
                f"✅ [{url_id}] COMPLETADO | '{hotel_name}' "
                f"| {scraped_count}/{len(languages)} idiomas "
                f"| {total_dur:.1f}s"
            )
        else:
            with _stats_lock:
                _stats["total_failed"] += 1
                _stats["consecutive_failures"] += 1
            _maybe_rotate_vpn()  # fallo total → intentar rotar
            logger.error(f"✗ [{url_id}] FALLIDO | {total_dur:.1f}s")

        return {
            "success":    scraped_count > 0,
            "hotel_name": hotel_name,
            "languages":  scraped_count,
            "duration":   round(total_dur, 2),
        }

    except Exception as e:
        logger.error(f"❌ Error fatal URL {url_id}: {e}", exc_info=True)
        db.rollback()
        with _stats_lock:
            _stats["total_failed"] += 1
            _stats["consecutive_failures"] += 1
        _maybe_rotate_vpn()
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
        return {"error": str(e)}

    finally:
        db.close()


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS INTERNOS
# ═══════════════════════════════════════════════════════════════════════════════

def _save_hotel(db, url_id: int, url: str, lang: str, data: Dict) -> bool:
    """
    Inserta un hotel en la BD SI no existe ya un registro para (url_id, language).

    [FIX #36 v5.1] Cambiado de ON CONFLICT DO UPDATE a ON CONFLICT DO NOTHING.

    ANTES v5.0: ON CONFLICT (url_id, language) DO UPDATE SET (todos los campos)
      - Problema: Sobrescribía datos correctos previamente scrapeados.
      - Si el orden de idiomas cambiaba o un rescraping fallaba a medias,
        los datos correctos del primer scraping se perdían.

    AHORA v5.1: ON CONFLICT (url_id, language) DO NOTHING
      - Preserva el dato existente si ya hay un registro para (url_id, lang).
      - Para forzar rescraping, se debe eliminar el registro manualmente.
      - Garantiza integridad: el primer dato correcto nunca se sobrescribe
        accidentalmente por reintentos, fallos parciales o cambios de orden.

    [FIX #37 v5.1] images_count=0 explícito en INSERT.
      - ANTES: images_count ausente del INSERT → dependía del DEFAULT del schema.
      - AHORA: images_count=0 siempre al insertar. Se actualiza con el valor real
        después de la descarga de imágenes mediante UPDATE separado.

    Returns:
        True si se insertó un registro nuevo.
        False si ya existía un registro (ON CONFLICT DO NOTHING → 0 rowcount).
    """
    result = db.execute(
        text("""
            INSERT INTO hotels (
                url_id, url, language,
                name, address, description,
                rating, total_reviews, rating_category,
                review_scores, services, facilities,
                house_rules, important_info,
                rooms_info, images_urls, images_count,
                scraped_at, updated_at
            ) VALUES (
                :url_id, :url, :language,
                :name, :address, :description,
                :rating, :total_reviews, :rating_category,
                CAST(:review_scores AS jsonb), CAST(:services AS jsonb), CAST(:facilities AS jsonb),
                :house_rules, :important_info,
                CAST(:rooms_info AS jsonb), CAST(:images_urls AS jsonb), 0,
                NOW(), NOW()
            )
            ON CONFLICT (url_id, language) DO NOTHING
        """),
        {
            "url_id":           url_id,
            "url":              url,
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
        }
    )
    db.commit()
    return result.rowcount > 0


def _download_images(url_id: int, img_urls: List[str], lang: str, driver=None) -> int:
    """
    Descarga imagenes usando la sesion del browser Brave (cookies validas).
    [FIX v2.4] Booking.com CDN (bstatic.com) bloquea requests directos.
    Al pasar el driver Selenium, se extraen sus cookies y referer para
    que la descarga sea autenticada como una peticion normal del browser.
    [FIX v3.1] Retorna el número de imágenes descargadas exitosamente
    para actualizar images_count en BD.
    """
    if not img_urls:
        return 0

    try:
        from app.image_downloader import ImageDownloader
        import requests as _req

        # Construir sesion autenticada con cookies del browser Brave
        session = _req.Session()
        session.headers.update({
            "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                               "AppleWebKit/537.36 (KHTML, like Gecko) "
                               "Chrome/124.0.0.0 Safari/537.36",
            "Referer":         "https://www.booking.com/",
            "Accept":          "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9,*;q=0.5",
            "sec-fetch-dest":  "image",
            "sec-fetch-mode":  "no-cors",
            "sec-fetch-site":  "cross-site",
        })

        # Extraer cookies del driver Brave (si está disponible)
        if driver:
            try:
                browser_cookies = driver.get_cookies()
                for c in browser_cookies:
                    session.cookies.set(
                        c["name"], c["value"],
                        domain=c.get("domain", ".booking.com"),
                    )
                logger.debug(f"  📷 [{url_id}] {len(browser_cookies)} cookies extraídas del browser")
            except Exception as ce:
                logger.debug(f"  📷 [{url_id}] No se pudieron extraer cookies: {ce}")

        dl = ImageDownloader()
        results = dl.download_images(url_id, img_urls, language=lang, session=session)
        ok = len(results)
        logger.info(f"  📷 [{url_id}] {ok}/{len(img_urls)} imágenes descargadas")
        return ok

    except Exception as e:
        logger.warning(f"  ⚠️ Error descargando imágenes [{url_id}]: {e}")
        return 0


def _log(db, url_id: int, language: str, status: str,
         duration: float, items: int, error: str = None):
    """Inserta una línea en scraping_logs."""
    try:
        db.execute(
            text("""
                INSERT INTO scraping_logs
                    (url_id, language, status, duration_seconds,
                     items_extracted, error_message, timestamp)
                VALUES
                    (:url_id, :lang, :status, :dur,
                     :items, :error, NOW())
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
        logger.debug(f"No se pudo insertar log: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# ESTADO DEL SERVICIO
# ═══════════════════════════════════════════════════════════════════════════════

def get_service_stats() -> Dict:
    """Devuelve estadísticas en tiempo real del servicio de scraping."""
    with _lock:
        active = list(_active_ids)
    with _stats_lock:
        s = _stats.copy()
    s["active_ids"] = active
    s["active_count"] = len(active)
    return s
