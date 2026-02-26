"""
BookingScraper/app/scraper.py  v5.0  [FIX CRÍTICO - Inglés en-gb + False Positive Block]
Scraper HTTP para Booking.com - BookingScraper Pro

CAMBIOS v5.0 [FIX CRÍTICO - INGLÉS NUNCA SE GUARDABA]:

  DIAGNÓSTICO CONFIRMADO (CSV exportado de BD + logs de consola):
  ──────────────────────────────────────────────────────────────
  ● BD tiene 0 filas con language='en'. Inglés fallaba en TODOS los hoteles.
  ● Log: "Página de bloqueo detectada (Cloudflare/CAPTCHA, 2,685,415b)"
    Pero 2.6MB NO puede ser un captcha (captcha pesa 30-80KB). FALSO POSITIVO.
    CAUSA: "checking your browser" aparece en texto de banners de consentimiento
    de Booking.com EN INGLÉS. En otros idiomas la frase está traducida → pasan.
    Resultado: inglés siempre detectado como "bloqueado", otros idiomas OK.
  ● URL inglés anterior: hotel.html?lang=en-us (sin sufijo en ruta)
    URL correcta (evidencia CSV ejemplo): hotel.en.html?lang=en-gb
    La URL sin sufijo recibe tratamiento diferente de CDN Booking.com/Cloudflare.

  [FIX #24 → CORREGIDO v5.1] build_language_url(): inglés genera hotel.en-gb.html?lang=en-gb
    ANTES v4.x: LANGUAGE_EXT["en"]="" → hotel.html?lang=en-us (sin sufijo = URL base)
    ANTES v5.0: LANGUAGE_EXT["en"]=".en" → hotel.en.html?lang=en-gb (sufijo corto no reconocido)
    AHORA v5.1: LANGUAGE_EXT["en"]=".en-gb" → hotel.en-gb.html?lang=en-gb (locale completo)
    CAUSA RAÍZ v5.0: sufijo corto '.en' no es una URL canónica de Booking.com;
    el CDN requiere el sufijo de locale completo '.en-gb' para IPs europeas.
    EVIDENCIA: log de consola confirmó generación incorrecta:
      "→ [369] Idioma [en]: .../garden-hill-resort-amp-spa.en.html?lang=en-gb"
    URL correcta: .../garden-hill-resort-amp-spa.en-gb.html?lang=en-gb

  [FIX #25] LANG_COOKIE_LOCALE["en"] = "en-gb" (antes "en-us")
    Coherente con URL y cookie: hotel.en.html?lang=en-gb + selectedLanguage=en-gb

  [FIX #26] BOOKING_BYPASS_COOKIES["selectedLanguage"] = "en-gb" (antes "en-us")

  [FIX #27] LANG_ACCEPT["en"] = "en-GB,en;q=0.9" (antes "en-US,en;q=0.9")

  [FIX #28] _is_blocked() páginas >500KB: ELIMINAR "checking your browser"
    Esta frase aparece en el HTML normal de Booking.com EN INGLÉS (banners GDPR,
    textos de soporte). Con IPs europeas solo aparece en versión inglesa, causando
    que inglés siempre fuera marcado como bloqueado mientras otros idiomas pasaban.
    Señales grandes mantenidas: "just a moment", "enable javascript", "ddos-guard".

  [FIX #29] Selenium scrape_hotel(): saltar _is_blocked() cuando loaded=True
    Si _wait_for_hotel_content() ya confirmó contenido de hotel (loaded=True),
    la llamada posterior a _is_blocked() es innecesaria y causa falsos positivos.
    ANTES: _is_blocked() llamado incondicionalmente → falso positivo en inglés
    AHORA: `if not loaded and _is_blocked(...)` → solo si hotel NO fue detectado

CAMBIOS v4.0 [FIX CRÍTICO - ENGLISH ENFORCEMENT + LANG MISMATCH BLOCK]:
  [FIX #20] mismatch de idioma → NO GUARDAR + log 'lang_mismatch'
  [FIX #21] Imágenes solo cuando lang='en' confirmado
  [FIX #22] Retry por mismatch (solo lang='en')
  [FIX #23] LANG_MISMATCH_MAX_RETRIES constante

CAMBIOS v3.1 [FIX DETECCIÓN DE BLOQUEO + VERIFICACIÓN DE IDIOMA]:
  [FIX CRÍTICO #14] BLOCK_SIGNALS: eliminadas "cookie-consent" y "privacymanager".
  [FIX CRÍTICO #15] _is_blocked(): umbral _BLOCK_CHECK_MAX_BYTES = 500KB.
  [NEW #16] _detect_page_language(): detecta idioma real de la página.

CAMBIOS v3.0 [FIX IDIOMA INGLÉS]: VPN US-first + American English (en-us). ← revertido v5.0
CAMBIOS v2.9: ?lang=LOCALE en URL + eliminación pre-navegación a homepage.
CAMBIOS v2.x: mejoras incrementales.
"""

import random
import re
import time
from pathlib import Path
from typing import Optional, Dict

from loguru import logger

from app.config import settings


# ─────────────────────────────────────────────────────────────────────────────
# User-Agents Chrome real en Windows 11
# ─────────────────────────────────────────────────────────────────────────────
USER_AGENTS_WIN = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

# ─────────────────────────────────────────────────────────────────────────────
# [FIX BUG #4] Mapeo de código ISO 639-1 → locale que Booking.com acepta
# en la cookie selectedLanguage y parámetro ?lang=.
# [FIX v5.1] "en" → "en-gb" (British English) — coherente con LANGUAGE_EXT["en"]=".en-gb"
# URL final para inglés: hotel.en-gb.html?lang=en-gb (sufijo de ruta = locale = cookie)
# CAUSA RAÍZ v5.0: LANGUAGE_EXT["en"]=".en" generaba hotel.en.html (sufijo no canónico).
# Con v5.1 los tres vectores de idioma son coherentes: ruta, ?lang= y cookie selectedLanguage.
# ─────────────────────────────────────────────────────────────────────────────
LANG_COOKIE_LOCALE: dict = {
    "en": "en-gb",   "es": "es",    "de": "de",    "fr": "fr",
    "it": "it",      "pt": "pt-pt", "nl": "nl",    "ru": "ru",
    "ar": "ar",      "tr": "tr",    "hu": "hu",    "pl": "pl",
    "zh": "zh-cn",   "no": "nb",    "fi": "fi",    "sv": "sv",
    "da": "da",      "ja": "ja",    "ko": "ko",
}

# [FIX BUG #5] Mapeo completo ISO 639-1 → header Accept-Language.
# El diccionario anterior solo cubría 8 de 19 idiomas; los restantes 11
# quedaban con "en-US,en;q=0.9", lo que indicaba al servidor que se
# prefería inglés, anulando la URL y la cookie de idioma.
LANG_ACCEPT: dict = {
    "en": "en-GB,en;q=0.9",
    "es": "es-ES,es;q=0.9,en;q=0.8",
    "de": "de-DE,de;q=0.9,en;q=0.8",
    "fr": "fr-FR,fr;q=0.9,en;q=0.8",
    "it": "it-IT,it;q=0.9,en;q=0.8",
    "pt": "pt-PT,pt;q=0.9,en;q=0.8",
    "nl": "nl-NL,nl;q=0.9,en;q=0.8",
    "ru": "ru-RU,ru;q=0.9,en;q=0.8",
    "ar": "ar-SA,ar;q=0.9,en;q=0.8",
    "tr": "tr-TR,tr;q=0.9,en;q=0.8",
    "hu": "hu-HU,hu;q=0.9,en;q=0.8",
    "pl": "pl-PL,pl;q=0.9,en;q=0.8",
    "zh": "zh-CN,zh;q=0.9,en;q=0.8",
    "no": "nb-NO,nb;q=0.9,no;q=0.8,en;q=0.7",
    "fi": "fi-FI,fi;q=0.9,en;q=0.8",
    "sv": "sv-SE,sv;q=0.9,en;q=0.8",
    "da": "da-DK,da;q=0.9,en;q=0.8",
    "ja": "ja-JP,ja;q=0.9,en;q=0.8",
    "ko": "ko-KR,ko;q=0.9,en;q=0.8",
}

BOOKING_BYPASS_COOKIES_BASE = {
    "OptanonAlertBoxClosed": "2024-01-01T00:00:00.000Z",
    "OptanonConsent":        "isGpcEnabled=0&datestamp=Mon+Jan+01+2024&version=202401.1.0&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1",
    "bkng_sso_ses":          "e30=",
    "cors":                  "1",
    # [FIX v5.0] "en-gb" = British English. Coherente con LANG_COOKIE_LOCALE["en"]="en-gb"
    # y URL formato hotel.en.html?lang=en-gb. Antes "en-us" causaba bloqueos Cloudflare.
    "selectedLanguage":      "en-gb",
    "selectedCurrency":      "EUR",
}
# [FIX BUG-05 v5.1] Las cookies _ga y _gid se generan POR SESIÓN en _get_session(),
# no al importar el módulo. Tenerlas como constante de módulo significa que todas
# las sesiones del proceso comparten el mismo GA ID → fingerprinting detectable.
# La función get_bypass_cookies() genera valores únicos en cada llamada.

def get_bypass_cookies() -> dict:
    """Retorna cookies de bypass con GA ID único por llamada (anti-fingerprinting)."""
    cookies = dict(BOOKING_BYPASS_COOKIES_BASE)
    cookies["_ga"]  = f"GA1.1.{random.randint(100000000, 999999999)}.{int(time.time()) - random.randint(86400, 2592000)}"
    cookies["_gid"] = f"GA1.1.{random.randint(100000000, 999999999)}.{int(time.time()) - random.randint(3600, 86400)}"
    return cookies

# Alias retrocompatible para referencias existentes (se prefiere get_bypass_cookies())
BOOKING_BYPASS_COOKIES = BOOKING_BYPASS_COOKIES_BASE

# Señales de página real de hotel (usado para validar HTML recibido)
HOTEL_PAGE_SIGNALS = [
    "property-description",
    "hp_facilities_box",
    "maxotelroomarea",
    "reviewscore",
    "review-score",
    "b2hotelpage",
    "hoteldetails",
]

# Señales de página de bloqueo REAL (Cloudflare, captcha, acceso denegado).
# [FIX v3.1 CRÍTICO] ELIMINADAS: "cookie-consent" y "privacymanager".
#   Estas cadenas aparecen en el JavaScript de OneTrust/GDPR de TODAS las páginas
#   de Booking.com (incluidas páginas de hotel válidas de 2MB+).
#   Su presencia NO indica bloqueo. Antes causaban falsos positivos que descartaban
#   páginas en inglés correctamente cargadas (títulos "updated prices 2026"),
#   mientras que las mismas páginas en español (donde OneTrust quizás usa clases
#   CSS distintas) pasaban el filtro. Resultado: inglés siempre fallaba, idiomas
#   de fallback (español primero) se guardaban incorrectamente como lang=en.
BLOCK_SIGNALS = [
    # [FIX BUG-03 v5.1] Solo señales INEQUÍVOCAS de páginas de bloqueo real.
    # ELIMINADAS respecto a v5.0:
    #   "checking your browser" → aparece en banners GDPR de Booking.com EN INGLÉS
    #     en TODAS las páginas válidas (no solo en Cloudflare challenges).
    #     Causaba falsos positivos en inglés para respuestas <500KB.
    #   "ray id" → Cloudflare incluye CF-Ray en headers de TODAS las respuestas (no
    #     solo en páginas de error). Puede aparecer en HTML de depuración de páginas
    #     válidas. No es señal fiable de bloqueo.
    "just a moment",                 # Cloudflare challenge JS inequívoco
    "access denied",                 # HTTP 403 explicito en body
    "403 forbidden",                 # Nginx/Apache 403 en body
    "please verify you are a human", # CAPTCHA humano — nunca en página de hotel
    "enable javascript",             # Cloudflare JS challenge — nunca en página de hotel
    "ddos-guard",                    # DDoS-Guard challenge — inequívoco
]

# Umbral mínimo en bytes para aplicar _is_blocked():
# Una página de Cloudflare challenge tiene ~30-80KB. Una página real de hotel
# tiene 1-3MB. Por debajo de 500KB sí verificamos bloqueo; por encima, es imposible
# que sea un captcha → se omite la verificación para evitar falsos positivos.
_BLOCK_CHECK_MAX_BYTES = 500_000

# [FIX v4.0] Idioma predeterminado que DEBE cumplirse estrictamente.
# Si Booking.com devuelve otro idioma, se reintenta en lugar de retornar datos incorrectos.
_DEFAULT_LANGUAGE = "en"
# Máximo de reintentos extra por mismatch de idioma (además de los reintentos normales)
_LANG_MISMATCH_MAX_RETRIES = 2


def _is_hotel_page(html: str) -> bool:
    html_low = html.lower()
    return any(s in html_low for s in HOTEL_PAGE_SIGNALS)


def _is_blocked(html: str) -> bool:
    """
    [FIX v3.1] Devuelve True SOLO si la página es un bloqueo/captcha real.
    Páginas > 500KB NO son páginas de captcha (que pesan 30-80KB) — se
    ignora el check para evitar falsos positivos con texto normal del hotel.
    """
    if len(html) > _BLOCK_CHECK_MAX_BYTES:
        # Página demasiado grande para ser un captcha/challenge real.
        # Solo buscamos señales INEQUÍVOCAS de Cloudflare en páginas grandes.
        # [FIX v5.0] ELIMINADO "checking your browser": aparece en textos normales
        # de Booking.com en inglés (banners de consentimiento, texto de soporte),
        # causando falsos positivos que bloqueaban scraping en inglés consistentemente
        # mientras otros idiomas pasaban (la frase solo aparece en versión inglesa).
        html_low = html.lower()
        return any(s in html_low for s in [
            "just a moment", "enable javascript",
            "ddos-guard",
        ])
    html_low = html.lower()
    return any(s in html_low for s in BLOCK_SIGNALS)


def _detect_page_language(html: str) -> Optional[str]:
    """
    [v4.0] Detecta el idioma real de la página recibida desde Booking.com.
    Booking.com puede ignorar ?lang= si el GeoIP de la IP/VPN contradice el parámetro.

    Estrategias en orden de fiabilidad:
      1. Atributo lang del <html> (e.g. lang="es", lang="en-US")
         - Ignora valores como "x-default", "und", "" o lang < 2 chars
      2. Meta og:locale (e.g. <meta property="og:locale" content="es_ES">)
      3. Meta http-equiv Content-Language
      4. Sufijo de idioma en URL canónica (og:url o link[rel=canonical])
         - https://www.booking.com/hotel/sc/foo.es.html → "es"
         - https://www.booking.com/hotel/sc/foo.html    → "en" (URL base = inglés)
      5. Presencia de texto inequívoco de Booking.com en DOM por idioma

    Retorna el código ISO 639-1 de 2 letras (en, es, de, fr, it...) o None.

    [FIX v4.0] Ignora valores inválidos: "x-default", "und", lang < 2 chars,
    para evitar falsos positivos de mismatch que bloquearían el scraping.
    """
    _INVALID_LANGS = {"x-default", "und", "xx", "zz", "qaa", ""}

    # Estrategia 1: <html lang="...">
    m = re.search(r'<html[^>]+\blang=["\']([a-zA-Z]{2,10}(?:-[a-zA-Z0-9]{2,8})?)["\']',
                  html[:3000], re.IGNORECASE)
    if m:
        lang_raw = m.group(1).lower().strip()
        code = lang_raw[:2]
        if code not in _INVALID_LANGS and len(code) == 2 and code.isalpha():
            return code  # "es-ES" → "es", "en-US" → "en"

    # Estrategia 2: og:locale  e.g. content="es_ES" o content="en_US"
    m = re.search(
        r'property=["\']og:locale["\'][^>]+content=["\']([a-zA-Z]{2,5}(?:[_-][a-zA-Z]{2,4})?)["\']',
        html[:8000], re.IGNORECASE
    )
    if not m:
        m = re.search(
            r'content=["\']([a-zA-Z]{2,5}(?:[_-][a-zA-Z]{2,4})?)["\'][^>]+property=["\']og:locale["\']',
            html[:8000], re.IGNORECASE
        )
    if m:
        lang_raw = m.group(1).lower().replace("_", "-")
        code = lang_raw[:2]
        if code not in _INVALID_LANGS and len(code) == 2 and code.isalpha():
            return code

    # Estrategia 3: Content-Language meta
    m = re.search(r'http-equiv=["\']Content-Language["\'][^>]+content=["\']([a-zA-Z]{2})',
                  html[:8000], re.IGNORECASE)
    if m:
        code = m.group(1).lower()
        if code not in _INVALID_LANGS:
            return code

    # Estrategia 4: Sufijo de idioma en la URL canónica
    # og:url o canonical link → .es.html → "es"; .html (sin sufijo) → "en"
    for url_search in [
        r'property=["\']og:url["\'][^>]+content=["\']([^"\']+)["\']',
        r'content=["\']([^"\']+)["\'][^>]+property=["\']og:url["\']',
        r'rel=["\']canonical["\'][^>]+href=["\']([^"\']+)["\']',
        r'href=["\']([^"\']+)["\'][^>]+rel=["\']canonical["\']',
    ]:
        mu = re.search(url_search, html[:8000], re.IGNORECASE)
        if mu:
            canon_url = mu.group(1)
            # Buscar sufijo de idioma: .es.html, .de.html, .en-gb.html, etc.
            ms = re.search(r'\.([a-z]{2}(?:-[a-z]{2,4})?)\.(html?)(\?|$)',
                           canon_url, re.IGNORECASE)
            if ms:
                code = ms.group(1).lower()[:2]
                if code not in _INVALID_LANGS and code.isalpha():
                    return code
            elif re.search(r'\.(html?)(\?|$)', canon_url, re.IGNORECASE):
                # URL termina en .html sin sufijo de idioma → es la URL base = inglés
                return "en"

    # Estrategia 5: Señales de texto del DOM específicas por idioma (Booking.com)
    # Estas frases son inequívocas y solo aparecen en el DOM del idioma correspondiente
    _LANG_SIGNALS = {
        "es": ["Ver disponibilidad", "Normas de la casa", "Servicios", "Valoración de los huéspedes"],
        "de": ["Verfügbarkeit prüfen", "Hausregeln", "Ausstattung", "Bewertungen"],
        "fr": ["Vérifier la disponibilité", "Règlement", "Services", "Commentaires"],
        "it": ["Verifica disponibilità", "Regole della casa", "Servizi", "Recensioni"],
        "pt": ["Verificar disponibilidade", "Regras da casa", "Serviços", "Avaliações"],
        "nl": ["Beschikbaarheid controleren", "Huisregels", "Diensten", "Beoordelingen"],
        "en": ["Check availability", "House rules", "Facilities", "Guest reviews"],
    }
    html_check = html[2000:50000]  # Saltar cabecera HTML, buscar en el cuerpo
    for lang_code, signals in _LANG_SIGNALS.items():
        matches = sum(1 for sig in signals if sig in html_check)
        if matches >= 2:  # Al menos 2 señales coinciden → idioma detectado
            return lang_code

    return None


# ─────────────────────────────────────────────────────────────────────────────
# SCRAPER CLOUDSCRAPER
# ─────────────────────────────────────────────────────────────────────────────

class BookingScraperCloudScraper:
    """
    Scraper usando cloudscraper.
    v2.3: la sesión se reinicia si recibe 403 (sesión 'envenenada').
    """

    def __init__(self, timeout: int = None):
        self.timeout = timeout or settings.BROWSER_TIMEOUT
        self._session = None
        self._blocked_count = 0  # NEW: contador de bloqueos para forzar reset de sesión

    def _get_session(self, force_new: bool = False):
        """
        Devuelve la sesión de cloudscraper.
        Si force_new=True o la sesión anterior fue bloqueada, crea una nueva.
        """
        if self._session is not None and not force_new:
            return self._session

        try:
            import cloudscraper

            # Si había sesión vieja, cerrarla
            if self._session is not None:
                try:
                    self._session.close()
                except Exception:
                    pass
                logger.debug("  🔄 Reseteando sesión cloudscraper (era bloqueada)")

            self._session = cloudscraper.create_scraper(
                browser={
                    "browser": "chrome",
                    "platform": "windows",
                    "desktop": True,
                },
                delay=5,
            )
            # Inyectar cookies GDPR con User-Agent aleatorio
            ua = random.choice(USER_AGENTS_WIN)
            self._session.headers.update({"User-Agent": ua})

            # [FIX BUG-05 v5.1] Usar get_bypass_cookies() → GA ID único por sesión
            for k, v in get_bypass_cookies().items():
                self._session.cookies.set(k, v, domain=".booking.com")

            self._blocked_count = 0
            logger.debug(f"  ✓ Nueva sesión cloudscraper | UA: {ua[:60]}")
            return self._session

        except ImportError:
            logger.error("❌ cloudscraper no instalado: pip install cloudscraper --break-system-packages")
            raise

    @staticmethod
    def _save_debug_html(url: str, html: str, label: str = ""):
        try:
            debug_dir = Path(settings.LOGS_PATH) / "debug"
            debug_dir.mkdir(parents=True, exist_ok=True)
            slug  = url.split("/")[-1][:40].replace(".", "_")
            ts    = int(time.time())
            fname = f"{label}_{slug}_{ts}.html" if label else f"{slug}_{ts}.html"
            fpath = debug_dir / fname
            fpath.write_text(html[:120000], encoding="utf-8", errors="ignore")
            logger.debug(f"  📄 HTML guardado: {fpath.name}")
        except Exception:
            pass

    def scrape_hotel(self, url: str, language: str = "en") -> Optional[Dict]:
        logger.info(f"🔍 [cloudscraper] {url}")

        # [FIX v4.0] Para el idioma predeterminado (en), se permiten reintentos
        # adicionales cuando Booking.com devuelve idioma incorrecto (GeoIP override).
        lang_mismatch_retries = 0

        for attempt in range(1, settings.MAX_RETRIES + 1):
            # [FIX] Si ya hubo 2+ bloqueos con esta sesión, forzar sesión nueva
            force_new = (attempt > 1 and self._blocked_count >= 2)
            try:
                delay = random.uniform(settings.MIN_REQUEST_DELAY, settings.MAX_REQUEST_DELAY)
                if attempt > 1:
                    delay = min(delay * attempt * 1.5, 25.0)
                    logger.info(f"  ⏳ Reintento {attempt} — esperando {delay:.1f}s...")
                time.sleep(delay)

                session  = self._get_session(force_new=force_new)

                # [FIX BUG #4 + v3.0] Actualizar cookie selectedLanguage con locale correcto.
                locale = LANG_COOKIE_LOCALE.get(language, language)
                session.cookies.set("selectedLanguage", locale, domain=".booking.com")

                response = session.get(
                    url,
                    timeout=self.timeout,
                    headers={
                        # [FIX BUG #5] Usar LANG_ACCEPT completo (19 idiomas)
                        "Accept-Language": LANG_ACCEPT.get(language, "en-US,en;q=0.9"),
                        "Referer":         "https://www.google.com/search?q=booking+hotel",
                        "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    },
                    allow_redirects=True,
                )

                status   = response.status_code
                html_content = response.text
                html_len     = len(html_content)

                logger.debug(f"  HTTP {status} | {html_len:,} bytes")

                # ── Manejo de estados HTTP ─────────────────────────────────────
                if status == 403:
                    self._blocked_count += 1
                    logger.warning(
                        f"  ⚠️ HTTP 403 — bloqueado (bloqueos={self._blocked_count}) "
                        f"— sesión será reseteada en próximo intento"
                    )
                    self._save_debug_html(url, html_content, "403")
                    time.sleep(random.uniform(15, 30))
                    continue

                if status == 429:
                    wait = int(response.headers.get("Retry-After", 90))
                    logger.warning(f"  ⚠️ HTTP 429 — Rate Limit, esperando {wait}s")
                    time.sleep(wait)
                    continue

                if status == 404:
                    logger.error("  ✗ HTTP 404 — URL no existe")
                    return None

                if status >= 500:
                    logger.warning(f"  ⚠️ HTTP {status} — error servidor")
                    time.sleep(random.uniform(10, 20))
                    continue

                # ── Validar contenido recibido ─────────────────────────────────
                if html_len < 5000:
                    logger.warning(f"  ⚠️ HTML corto ({html_len} bytes)")
                    self._save_debug_html(url, html_content, "short")
                    self._blocked_count += 1
                    time.sleep(random.uniform(8, 15))
                    continue

                if _is_blocked(html_content):
                    logger.warning(f"  ⚠️ Página de bloqueo/captcha detectada")
                    self._save_debug_html(url, html_content, "blocked")
                    self._blocked_count += 1
                    time.sleep(random.uniform(20, 40))
                    continue

                if not _is_hotel_page(html_content):
                    logger.warning(f"  ⚠️ HTML no parece página de hotel ({html_len} bytes)")
                    self._save_debug_html(url, html_content, "not_hotel")
                    if attempt < settings.MAX_RETRIES:
                        time.sleep(random.uniform(5, 10))
                        continue

                # ── Extracción ─────────────────────────────────────────────────
                m     = re.search(r"<title[^>]*>(.*?)</title>", html_content, re.I | re.S)
                title = m.group(1).strip() if m else "(sin título)"
                logger.debug(f"  📄 '{title}' | {html_len:,}b")

                # [FIX v4.0] Verificar idioma de página recibida — ESTRICTO para lang='en'
                detected_lang = _detect_page_language(html_content)
                if detected_lang and detected_lang != language:
                    logger.warning(
                        f"  ⚠️ IDIOMA INCORRECTO: solicitado='{language}' "
                        f"recibido='{detected_lang}' | URL={url}"
                    )
                    # [FIX v4.0] Para el idioma predeterminado (en), REINTENTAR en lugar
                    # de retornar datos incorrectos. Forzar sesión nueva y esperar más.
                    if language == _DEFAULT_LANGUAGE and lang_mismatch_retries < _LANG_MISMATCH_MAX_RETRIES:
                        lang_mismatch_retries += 1
                        self._blocked_count += 1  # forzar nueva sesión en siguiente intento
                        self._save_debug_html(url, html_content, f"lang_mismatch_{detected_lang}")
                        wait = random.uniform(20, 40)
                        logger.warning(
                            f"  🔄 Reintento por idioma incorrecto [{lang_mismatch_retries}/"
                            f"{_LANG_MISMATCH_MAX_RETRIES}] — esperando {wait:.0f}s con sesión nueva"
                        )
                        time.sleep(wait)
                        continue  # REINTENTAR con sesión nueva (force_new se activará por _blocked_count)
                    else:
                        # Agotados los reintentos de idioma → retornar None
                        # para que el caller (scraper_service) decida qué hacer
                        if language == _DEFAULT_LANGUAGE:
                            logger.error(
                                f"  ✗ Reintentos de idioma agotados para '{language}'. "
                                f"Booking.com devuelve '{detected_lang}'. "
                                f"Posible causa: sin VPN o IP geolocalizada fuera de zona anglófona."
                            )
                            self._save_debug_html(url, html_content, f"lang_fail_{detected_lang}")
                            return None  # [v4.0] No retornar datos en idioma incorrecto para 'en'

                from app.extractor import BookingExtractor
                extractor = BookingExtractor(html_content, language)
                data      = extractor.extract_all()
                data["url"]           = url
                data["http_status"]   = status
                data["html_length"]   = html_len
                data["page_title"]    = title
                data["detected_lang"] = detected_lang  # [v3.1] trazabilidad

                if data.get("name"):
                    self._blocked_count = 0  # éxito → reset contador bloqueos
                    logger.success(
                        f"  ✓ '{data['name']}' | rating={data.get('rating')} | {html_len:,}b"
                    )
                else:
                    logger.warning(f"  ⚠️ Sin nombre | '{title}' | {html_len:,}b")
                    self._save_debug_html(url, html_content, "no_name")

                return data

            except Exception as e:
                logger.error(f"  ✗ Error (intento {attempt}): {e}")
                if attempt < settings.MAX_RETRIES:
                    time.sleep(random.uniform(3, 8))

        logger.error(f"✗ Reintentos agotados: {url}")
        return None

    def close(self):
        if self._session:
            try:
                self._session.close()
            except Exception:
                pass

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


# ─────────────────────────────────────────────────────────────────────────────
# SCRAPER SELENIUM
# ─────────────────────────────────────────────────────────────────────────────

class BookingScraperSelenium:
    """
    Scraper con ChromeDriver real.
    v2.3: _wait_for_hotel_content mejorado — detecta título og/title como señal,
    no solo elementos internos del hotel.
    """

    def __init__(self):
        self.driver = None
        self._setup_driver()

    BROWSER_PATHS = {
        "brave":  [
            r"C:\Program Files\BraveSoftware\Brave-Browser\Application\brave.exe",
            r"C:\Program Files (x86)\BraveSoftware\Brave-Browser\Application\brave.exe",
        ],
        "opera":  [
            r"C:\Users\SA\AppData\Local\Programs\Opera\opera.exe",
            r"C:\Program Files\Opera\opera.exe",
        ],
        "chrome": [
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
            r"C:\Users\SA\AppData\Local\Google\Chrome\Application\chrome.exe",
        ],
        "edge":   [
            r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
            r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
        ],
    }

    def _setup_driver(self):
        order = [
            ("Brave",  self._try_brave),
            ("Chrome", self._try_chrome),
            ("Edge",   self._try_edge),
            ("Opera",  self._try_opera),
        ]
        errors = []
        for name, fn in order:
            try:
                fn()
                return
            except Exception as e:
                msg = str(e).splitlines()[0][:120]
                logger.warning(f"  {name} no disponible: {msg}")
                errors.append(f"{name}: {msg}")

        raise RuntimeError(
            "Ningún navegador disponible.\n"
            "Instala Brave: https://brave.com/\n"
            "O Google Chrome: https://www.google.com/chrome/"
        )

    @staticmethod
    def _gpu_flags():
        return [
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--disable-gpu-sandbox",
            "--use-gl=swiftshader",
            "--disable-software-rasterizer",
            "--disable-blink-features=AutomationControlled",
            "--disable-extensions",
            "--disable-infobars",
            "--start-maximized",
            "--disable-background-networking",
            "--disable-sync",
            "--metrics-recording-only",
            "--no-first-run",
            "--safebrowsing-disable-auto-update",
            "--log-level=3",
        ]

    def _chrome_options_with_binary(self, binary_path: str):
        from selenium.webdriver.chrome.options import Options
        o = Options()
        o.binary_location = binary_path
        if settings.HEADLESS_BROWSER:
            o.add_argument("--headless=new")
        for flag in self._gpu_flags():
            o.add_argument(flag)
        o.add_argument(f"--user-agent={random.choice(USER_AGENTS_WIN)}")
        o.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
        o.add_experimental_option("useAutomationExtension", False)
        return o

    def _edge_options(self):
        from selenium.webdriver.edge.options import Options
        o = Options()
        if settings.HEADLESS_BROWSER:
            o.add_argument("--headless=new")
        for flag in self._gpu_flags():
            o.add_argument(flag)
        o.add_argument(f"--user-agent={random.choice(USER_AGENTS_WIN)}")
        o.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
        o.add_experimental_option("useAutomationExtension", False)
        return o

    def _find_binary(self, browser: str) -> str:
        import os
        for path in self.BROWSER_PATHS.get(browser, []):
            if os.path.exists(path):
                return path
        raise FileNotFoundError(f"Ejecutable de {browser} no encontrado")

    def _try_brave(self):
        from selenium import webdriver
        binary = self._find_binary("brave")
        options = self._chrome_options_with_binary(binary)
        self.driver = webdriver.Chrome(options=options)
        self.driver.implicitly_wait(10)
        self.driver.set_page_load_timeout(settings.BROWSER_TIMEOUT)
        logger.success("✓ Brave iniciado")
        return self.driver  # [FIX v2.5] necesario para recovery block en scrape_hotel()

    def _try_chrome(self):
        from selenium import webdriver
        binary = self._find_binary("chrome")
        options = self._chrome_options_with_binary(binary)
        self.driver = webdriver.Chrome(options=options)
        self.driver.implicitly_wait(10)
        self.driver.set_page_load_timeout(settings.BROWSER_TIMEOUT)
        logger.success("✓ Chrome iniciado")
        return self.driver  # [FIX v2.5]

    def _try_edge(self):
        from selenium import webdriver
        self.driver = webdriver.Edge(options=self._edge_options())
        self.driver.implicitly_wait(10)
        self.driver.set_page_load_timeout(settings.BROWSER_TIMEOUT)
        logger.success("✓ Edge iniciado")
        return self.driver  # [FIX v2.5]

    def _try_opera(self):
        from selenium import webdriver
        binary = self._find_binary("opera")
        options = self._chrome_options_with_binary(binary)
        self.driver = webdriver.Chrome(options=options)
        self.driver.implicitly_wait(10)
        self.driver.set_page_load_timeout(settings.BROWSER_TIMEOUT)
        logger.success("✓ Opera iniciado")
        return self.driver  # [FIX v2.5]

    def scrape_hotel(self, url: str, language: str = "en") -> Optional[Dict]:
        """
        v4.0: retry por idioma incorrecto + v2.3: retry interno con re-navegación.
        """
        if self.driver is None:
            logger.error("✗ Driver Selenium no disponible")
            return None

        # [FIX v4.0] Contador de reintentos por idioma incorrecto (para lang='en')
        lang_mismatch_retries = 0

        for attempt in range(1, 4):  # hasta 3 reintentos por URL
            try:
                logger.info(f"🔍 [Selenium] {url} (intento {attempt})")

                if attempt > 1:
                    wait_s = random.uniform(10, 20) * attempt
                    logger.info(f"  ⏳ Esperando {wait_s:.0f}s antes de reintentar...")
                    time.sleep(wait_s)
                else:
                    time.sleep(random.uniform(
                        settings.MIN_REQUEST_DELAY,
                        settings.MAX_REQUEST_DELAY
                    ))

                # [v3.0 FIX CRÍTICO] Configuración de idioma en 3 niveles:
                try:
                    # Nivel 1: Accept-Language header via CDP
                    self.driver.execute_cdp_cmd(
                        "Network.setExtraHTTPHeaders",
                        {"headers": {"Accept-Language": LANG_ACCEPT.get(language, "en-US,en;q=0.9")}}
                    )
                except Exception as cdp_err:
                    logger.debug(f"  ⚠️ CDP Accept-Language no aplicado: {cdp_err}")

                try:
                    # Nivel 2: Inyectar cookie via CDP (funciona desde about:blank)
                    locale = LANG_COOKIE_LOCALE.get(language, language)
                    import time as _time
                    expire_ts = int(_time.time()) + 86400 * 365  # 1 año
                    self.driver.execute_cdp_cmd("Network.setCookies", {
                        "cookies": [
                            {
                                "name":     "selectedLanguage",
                                "value":    locale,
                                "domain":   ".booking.com",
                                "path":     "/",
                                "secure":   False,
                                "httpOnly": False,
                                "expires":  expire_ts,
                            }
                        ]
                    })
                    logger.debug(f"  🌐 Cookie CDP inyectada: selectedLanguage={locale}")
                except Exception as cdp_cookie_err:
                    logger.debug(f"  ⚠️ CDP setCookies falló ({cdp_cookie_err}), usando fallback add_cookie")
                    try:
                        current_url = self.driver.current_url or ""
                        if "booking.com" in current_url:
                            locale = LANG_COOKIE_LOCALE.get(language, language)
                            try:
                                self.driver.delete_cookie("selectedLanguage")
                            except Exception:
                                pass
                            self.driver.add_cookie({
                                "name":   "selectedLanguage",
                                "value":  locale,
                                "domain": ".booking.com",
                                "path":   "/",
                            })
                            logger.debug(f"  🌐 Cookie idioma (fallback): {language} → {locale}")
                    except Exception as lang_err:
                        logger.debug(f"  ⚠️ Fallback cookie también falló: {lang_err}")

                self.driver.get(url)

                # Espera inteligente a contenido del hotel
                loaded = self._wait_for_hotel_content()

                if not loaded:
                    html_check = self.driver.page_source.lower()
                    # [FIX BUG-03 v5.1] Eliminado "checking your browser": aparece en GDPR
                    # banners de Booking.com en inglés → false positive en versión inglesa.
                    if any(s in html_check for s in ["just a moment", "enable javascript",
                                                     "ddos-guard", "access denied"]):
                        logger.warning(f"  ⚠️ Cloudflare challenge detectado (intento {attempt})")
                        continue  # retry

                self._close_popups()
                self._scroll_page()

                # [v2.6] Abrir galería completa para capturar TODAS las imágenes.
                self._open_gallery_and_extract_images()

                html_content = self.driver.page_source
                html_len     = len(html_content)
                page_title   = self.driver.title or ""

                logger.debug(f"  📄 '{page_title}' | {html_len:,} bytes")

                if html_len < 5000:
                    logger.warning(f"  ⚠️ HTML muy corto ({html_len}b) — posible bloqueo")
                    continue

                # [FIX v5.0] Solo verificar bloqueo si el hotel NO fue detectado por
                # _wait_for_hotel_content(). Si loaded=True, el contenido del hotel ya
                # fue confirmado → _is_blocked() nunca aplica (y causa falsos positivos
                # en páginas >500KB en inglés que contienen frases ambiguas de Cloudflare
                # en el texto normal de Booking.com como banners de consentimiento).
                if not loaded and _is_blocked(html_content):
                    logger.warning(f"  ⚠️ Página de bloqueo detectada (Cloudflare/CAPTCHA, {html_len:,}b)")
                    continue

                # [FIX v4.0] Verificar que el idioma de la página coincide con el solicitado.
                detected_lang = _detect_page_language(html_content)
                if detected_lang and detected_lang != language:
                    logger.warning(
                        f"  ⚠️ IDIOMA INCORRECTO: solicitado='{language}' "
                        f"recibido='{detected_lang}' | URL={url}"
                    )
                    # [FIX v4.0] Para el idioma predeterminado (en), REINTENTAR
                    if language == _DEFAULT_LANGUAGE and lang_mismatch_retries < _LANG_MISMATCH_MAX_RETRIES:
                        lang_mismatch_retries += 1
                        self._save_debug_html(url, html_content)
                        wait = random.uniform(20, 40)
                        logger.warning(
                            f"  🔄 Reintento por idioma incorrecto [{lang_mismatch_retries}/"
                            f"{_LANG_MISMATCH_MAX_RETRIES}] — esperando {wait:.0f}s"
                        )
                        time.sleep(wait)
                        continue  # REINTENTAR
                    elif language == _DEFAULT_LANGUAGE:
                        logger.error(
                            f"  ✗ Reintentos de idioma agotados para '{language}'. "
                            f"Booking.com devuelve '{detected_lang}'. "
                            f"Posible causa: sin VPN o IP geolocalizada fuera de zona anglófona."
                        )
                        self._save_debug_html(url, html_content)
                        return None  # [v4.0] No retornar datos en idioma incorrecto para 'en'

                from app.extractor import BookingExtractor
                extractor = BookingExtractor(html_content, language)
                data      = extractor.extract_all()
                data["url"]              = url
                data["html_length"]      = html_len
                data["page_title"]       = page_title
                data["detected_lang"]    = detected_lang  # para trazabilidad

                if data.get("name"):
                    logger.success(
                        f"  ✓ '{data['name']}' | rating={data.get('rating')} | {html_len:,}b"
                    )
                    return data
                else:
                    logger.warning(f"  ⚠️ Sin nombre extraído | '{page_title}' | {html_len:,}b")
                    self._save_debug_html(url, html_content)
                    if page_title and "booking.com" in page_title.lower():
                        return data

            except Exception as e:
                err_msg = str(e)
                logger.error(f"  ✗ Selenium error (intento {attempt}): {err_msg[:200]}")

                if "invalid session id" in err_msg.lower():
                    logger.warning(f"  ⚠️ Session Brave muerta (intento {attempt}) — recreando driver...")
                    try:
                        self.close()
                    except Exception:
                        pass
                    try:
                        self.driver = None
                        success = False
                        for browser_name, try_func in [
                            ("Brave",  self._try_brave),
                            ("Chrome", self._try_chrome),
                            ("Edge",   self._try_edge),
                        ]:
                            try:
                                self.driver = try_func()
                                if self.driver:
                                    logger.info(f"  ✓ {browser_name} reiniciado (intento {attempt})")
                                    success = True
                                    break
                            except Exception as be:
                                logger.debug(f"  {browser_name} fallo al reiniciar: {be}")
                        if not success:
                            logger.error("  ✗ No se pudo reiniciar ningun browser — abortando hotel")
                            return None
                    except Exception as re_err:
                        logger.error(f"  ✗ Error recreando driver: {re_err}")
                        return None
                    continue

                if attempt < 3:
                    time.sleep(random.uniform(5, 10))

        logger.error(f"✗ Reintentos Selenium agotados: {url}")
        return None

    def _wait_for_hotel_content(self, timeout: int = 30) -> bool:
        """
        v2.3: Detecta contenido de hotel O simplemente que la página cargó
        (via título o cualquier elemento conocido).
        Devuelve True si parece página real de hotel, False si no.
        """
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.common.by import By
        import selenium.common.exceptions as SE

        # Selectores en orden de más fiable a menos
        selectors = [
            # Estructura 2024/2025 con data-testid
            (By.CSS_SELECTOR,  "[data-testid='title']"),
            (By.CSS_SELECTOR,  "[data-testid='property-description']"),
            (By.CSS_SELECTOR,  "[data-testid='review-score-component']"),
            # Estructura clásica
            (By.ID,            "hp_facilities_box"),
            (By.CSS_SELECTOR,  "h2.pp-header__title"),
            (By.CSS_SELECTOR,  "#maxotelRoomArea"),
            # Señal genérica de que la página terminó de cargar
            (By.CSS_SELECTOR,  "[id='b2hotelPage']"),
            (By.CSS_SELECTOR,  ".bui-review-score"),
        ]

        wait = WebDriverWait(self.driver, timeout)
        for by, selector in selectors:
            try:
                wait.until(EC.presence_of_element_located((by, selector)))
                logger.debug(f"  ✓ Hotel detectado via: {selector}")
                time.sleep(1.5)
                return True
            except SE.TimeoutException:
                continue
            except Exception:
                continue

        # Fallback: esperar a que el <title> contenga "booking.com"
        try:
            WebDriverWait(self.driver, 10).until(
                lambda d: "booking.com" in (d.title or "").lower()
            )
            logger.debug("  ✓ Título booking.com detectado (fallback)")
            time.sleep(settings.PAGE_LOAD_WAIT)
            return True
        except Exception:
            pass

        logger.debug("  ⚠️ Ningún selector de hotel detectado, usando delay base")
        time.sleep(settings.PAGE_LOAD_WAIT)
        return False

    def _save_debug_html(self, url: str, html: str):
        try:
            debug_dir = Path(settings.LOGS_PATH) / "debug"
            debug_dir.mkdir(parents=True, exist_ok=True)
            slug  = url.split("/")[-1][:40].replace(".", "_")
            ts    = int(time.time())
            fpath = debug_dir / f"selenium_{slug}_{ts}.html"
            fpath.write_text(html[:120000], encoding="utf-8", errors="ignore")
            logger.debug(f"  📄 HTML Selenium guardado: {fpath.name}")
        except Exception:
            pass

    def _open_gallery_and_extract_images(self) -> bool:
        """
        [v2.6 - NEW] Abre el modal de galería completa de Booking.com y hace scroll
        para que se carguen todas las imágenes lazy (photo book).

        Booking.com muestra ~8 fotos en la página principal. Las restantes (hasta 90+)
        solo aparecen en el modal GalleryGridViewModal que se abre al hacer clic en
        cualquier foto del carrusel o en el botón "Ver todas las fotos".

        Returns:
            True si el modal se abrió y se completó el scroll; False en caso contrario.
        """
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        import selenium.common.exceptions as SE

        MODAL_SELECTOR = "[data-testid='GalleryGridViewModal-wrapper']"

        # --- Selectores del trigger de la galería (Booking.com 2024-2026) ---
        gallery_triggers = [
            # Botón explícito "Ver todas las fotos" / "See all X photos"
            "[data-testid='bui-gallery-modal-trigger']",
            "[data-testid='hp-gallery-open-bui']",
            "button[data-testid*='photo']",
            # Zona hero de fotos (click en primera foto)
            "[data-testid='b2hotelPage-hero-photos-wrapper']",
            "[data-testid='photosCarouselGalleryImage']",
            ".bh-photo-grid-thumb",
            # Fallback: primera imagen del CDN bstatic visible
            "img[src*='bstatic.com/xdata/images/hotel/']",
        ]

        opened = False
        for selector in gallery_triggers:
            try:
                elem = WebDriverWait(self.driver, 5).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                )
                self.driver.execute_script("arguments[0].scrollIntoView(true);", elem)
                time.sleep(0.5)
                self.driver.execute_script("arguments[0].click();", elem)
                time.sleep(2)

                # Confirmar que el modal se abrió
                WebDriverWait(self.driver, 8).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, MODAL_SELECTOR))
                )
                logger.info(f"  📸 Galería abierta via: {selector}")
                opened = True
                break
            except Exception:
                continue

        if not opened:
            logger.debug("  ⚠️ No se pudo abrir el modal de galería — solo fotos de página principal")
            return False

        # --- Scroll dentro del modal para cargar imágenes lazy ---
        try:
            modal = self.driver.find_element(By.CSS_SELECTOR, MODAL_SELECTOR)
            prev_count = 0
            for _ in range(40):  # máximo 40 scrolls (~3000 imágenes)
                self.driver.execute_script("arguments[0].scrollTop += 900;", modal)
                time.sleep(0.25)
                imgs = modal.find_elements(By.TAG_NAME, "img")
                if len(imgs) == prev_count and _ > 5:
                    break  # sin nuevas imágenes → llegamos al fondo
                prev_count = len(imgs)

            final_count = len(modal.find_elements(By.TAG_NAME, "img"))
            logger.info(f"  📷 Galería completa: {final_count} imágenes en DOM")
        except Exception as e:
            logger.debug(f"  ⚠️ Error en scroll galería: {e}")

        time.sleep(0.5)
        return True

    def _close_popups(self):
        from selenium.webdriver.common.by import By
        for selector in [
            "button[aria-label='Dismiss sign-in info.']",
            "button[data-testid='close-banner']",
            "button.bui_button_close",
            "[data-testid='cookie-consent-accept']",
            "button#onetrust-accept-btn-handler",
        ]:
            try:
                self.driver.find_element(By.CSS_SELECTOR, selector).click()
                time.sleep(0.5)
                break
            except Exception:
                continue

    def _scroll_page(self):
        try:
            for i in range(settings.SCROLL_ITERATIONS):
                self.driver.execute_script(f"window.scrollTo(0, {(i+1)*1000});")
                time.sleep(0.3)
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(0.5)
            # Volver arriba para capturar header
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(0.3)
        except Exception:
            pass

    def close(self):
        if self.driver:
            try:
                self.driver.quit()
                self.driver = None
            except Exception:
                pass

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


# ─────────────────────────────────────────────────────────────────────────────
# FÁBRICA
# ─────────────────────────────────────────────────────────────────────────────

class BookingScraper:
    """Selecciona el scraper según USE_SELENIUM en .env."""

    def __new__(cls):
        if settings.USE_SELENIUM:
            logger.info("Scraper: Selenium (USE_SELENIUM=True)")
            return BookingScraperSelenium()
        else:
            logger.info("Scraper: cloudscraper (bypass Cloudflare)")
            return BookingScraperCloudScraper()


# ─────────────────────────────────────────────────────────────────────────────
# URL MULTI-IDIOMA
# ─────────────────────────────────────────────────────────────────────────────

def build_language_url(base_url: str, language: str) -> str:
    """
    Construye la URL de Booking.com para el idioma solicitado.

    [FIX BUG #1 CRÍTICO] Antes de añadir el sufijo de idioma nuevo, se elimina
    cualquier sufijo de idioma existente en la URL (.es, .de, .fr, .en-gb, etc.).
    Sin este paso, una URL base como '.../hotel.es.html' genera:
      - Para 'en': devuelve '.../hotel.es.html' sin cambios  ← siempre español
      - Para 'de': devuelve '.../hotel.es.de.html'           ← 404 en Booking.com
      - Para 'fr': devuelve '.../hotel.es.fr.html'           ← 404 en Booking.com

    Patrón eliminado: .<2 letras>[opcional: -<2-4 letras>].html
    Ejemplos: .es.html, .de.html, .en-gb.html, .zh-cn.html, .pt-br.html

    [FIX v2.9] Se añade ?lang=LOCALE a la URL para forzar el idioma server-side
    en Booking.com 2025/2026. El parámetro ?lang= tiene precedencia sobre la
    cookie selectedLanguage y el Accept-Language header.

    [FIX v5.1] Para lang='en': genera hotel.en-gb.html?lang=en-gb.
    LANGUAGE_EXT["en"]=".en-gb" garantiza que los tres vectores de idioma sean
    coherentes: sufijo de ruta (.en-gb), parámetro (?lang=en-gb) y cookie
    (selectedLanguage=en-gb). El sufijo corto '.en' de v5.0 no era una URL
    canónica reconocida por el CDN de Booking.com para IPs europeas.
    """
    # PASO 1: Eliminar sufijo de idioma existente Y query params previos
    stripped = base_url.strip()
    if "?" in stripped:
        stripped = stripped.split("?")[0]

    clean_url = re.sub(
        r'\.[a-z]{2}(?:-[a-z]{2,4})?\.html$',
        '.html',
        stripped,
        flags=re.IGNORECASE,
    )
    if not clean_url.endswith('.html'):
        clean_url += '.html'

    # PASO 2: Añadir sufijo del idioma solicitado
    ext = settings.LANGUAGE_EXT.get(language, f".{language}")
    if not ext:     # ext vacío (no debería ocurrir con config actual) → sin sufijo
        base = clean_url
    else:
        base = clean_url[:-5] + ext + '.html'

    # PASO 3: Añadir ?lang=LOCALE para forzado server-side (máxima prioridad)
    # [FIX v5.1] "en" → sufijo ".en-gb" + locale "en-gb". Resultado para "en":
    # hotel.en-gb.html?lang=en-gb (sufijo de ruta coherente con locale y cookie)
    # ANTES v5.0: hotel.en.html?lang=en-gb → sufijo corto '.en' no canónico en Booking.com
    locale = LANG_COOKIE_LOCALE.get(language, language)
    return f"{base}?lang={locale}"
