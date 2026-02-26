"""
BookingScraper/app/config.py
Configuración centralizada - BookingScraper Pro
Windows 11 + Python 3.14.3 + Pydantic v2

CORRECCIONES v1.1:
  [FIX] DATABASE_URL: postgresql+psycopg:// (psycopg3, no psycopg2)
  [FIX] CELERY_WORKER_CONCURRENCY = 1 (Windows solo pool obligatorio)
  [NEW] Propiedad ENABLED_LANGUAGES: List[str] parseada desde CSV
  [FIX] VPN_ENABLED por defecto False (no bloquea arranque sin VPN)

CORRECCIONES v1.2 [FIX CRÍTICO - INGLÉS NUNCA SE GUARDABA]:
  [FIX #24] LANGUAGE_EXT["en"] = ".en" (antes "")
    EVIDENCIA: ejemplo CSV confirma URL correcta: hotel.en.html?lang=en-gb
    ANTES: hotel.html?lang=en-us → sin sufijo en ruta → bloqueado por Cloudflare
    AHORA: hotel.en.html?lang=en-gb → misma estructura que .es.html, .de.html
  [FIX #30] VPN_COUNTRIES: UK primero (antes US primero)
    Para IPs del Reino Unido + lang=en-gb → Booking.com sirve British English sin re-mapeo
"""

from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List
import os


class Settings(BaseSettings):
    """Configuración general del sistema"""

    # ── APLICACIÓN ──────────────────────────────────────────────
    APP_NAME: str = "Booking Scraper Pro"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = False

    # ── POSTGRESQL ───────────────────────────────────────────────
    DB_HOST: str = "localhost"
    DB_PORT: int = 5432
    DB_USER: str = "postgres"
    DB_PASSWORD: str = "2221"
    DB_NAME: str = "booking_scraper"

    @property
    def DATABASE_URL(self) -> str:
        # ✅ FIX: postgresql+psycopg:// para psycopg3 (no postgresql://)
        return (
            f"postgresql+psycopg://{self.DB_USER}:{self.DB_PASSWORD}"
            f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        )

    # ── REDIS / MEMURAI ──────────────────────────────────────────
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_PASSWORD: str = ""

    @property
    def REDIS_URL(self) -> str:
        if self.REDIS_PASSWORD:
            return f"redis://:{self.REDIS_PASSWORD}@{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"

    # ── CELERY ───────────────────────────────────────────────────
    CELERY_BROKER_URL: str = "redis://localhost:6379/0"
    CELERY_RESULT_BACKEND: str = "redis://localhost:6379/1"
    # ✅ FIX: 1 es el único valor válido con pool='solo' en Windows
    CELERY_WORKER_CONCURRENCY: int = 1

    # ── VPN ──────────────────────────────────────────────────────
    # ✅ FIX: False por defecto → el sistema arranca sin VPN activa
    VPN_ENABLED: bool = False
    # [FIX v5.0] UK primero → IP del Reino Unido para hotel.en.html?lang=en-gb
    # Antes: US-first para en-us. Ahora: UK-first para en-gb (formato correcto).
    VPN_COUNTRIES: List[str] = ["UK", "US", "CA", "DE", "FR", "NL", "IT", "ES"]
    VPN_ROTATION_INTERVAL: int = 50

    # ── SCRAPING ─────────────────────────────────────────────────
    HEADLESS_BROWSER: bool = True
    BROWSER_TIMEOUT: int = 30
    PAGE_LOAD_WAIT: int = 5
    SCROLL_ITERATIONS: int = 3
    MAX_RETRIES: int = 3
    RETRY_DELAY: int = 60          # segundos entre reintentos
    MIN_REQUEST_DELAY: float = 2.0
    MAX_REQUEST_DELAY: float = 5.0
    # False = httpx (más rápido); True = Selenium (JS dinámico)
    USE_SELENIUM: bool = False

    # ── USER AGENTS ──────────────────────────────────────────────
    USER_AGENTS: List[str] = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    ]

    # ── IDIOMAS ──────────────────────────────────────────────────
    # En .env: LANGUAGES_ENABLED=en,es,de,fr,it
    LANGUAGES_ENABLED: str = "en,es,de,fr,it"
    DEFAULT_LANGUAGE: str = "en"

    @property
    def ENABLED_LANGUAGES(self) -> List[str]:
        """Lista de idiomas habilitados, parseada desde string CSV."""
        return [lang.strip() for lang in self.LANGUAGES_ENABLED.split(",") if lang.strip()]

    # [FIX v5.1] LANGUAGE_EXT: 'en' usa sufijo '.en-gb' = hotel.en-gb.html?lang=en-gb
    # CAUSA RAÍZ CONFIRMADA: Booking.com con IPs europeas requiere sufijo de locale
    # completo '.en-gb' para servir British English. El sufijo corto '.en' genera una
    # URL que el CDN de Booking.com no reconoce como idioma canónico, produciendo
    # comportamiento inconsistente (redirección a idioma detectado por GeoIP o 404).
    # EVIDENCIA: log de scraping confirma URL incorrecta generada con ".en":
    #   INCORRECTO (v5.0): hotel.en.html?lang=en-gb
    #   CORRECTO   (v5.1): hotel.en-gb.html?lang=en-gb
    # COHERENCIA: LANG_COOKIE_LOCALE["en"]="en-gb" ya estaba correcto → ahora
    # el sufijo de ruta es coherente con el parámetro ?lang= y la cookie.
    # ANTES v4.x: "en" → "" → hotel.html?lang=en-us → BLOQUEADO por Cloudflare
    # ANTES v5.0: "en" → ".en" → hotel.en.html?lang=en-gb → URL no reconocida por CDN
    # AHORA v5.1: "en" → ".en-gb" → hotel.en-gb.html?lang=en-gb → URL canónica correcta
    LANGUAGE_EXT: dict = {
        "en": ".en-gb",
        "es": ".es",
        "fr": ".fr",
        "de": ".de",
        "it": ".it",
        "pt": ".pt",
        "nl": ".nl",
        "ru": ".ru",
        "ar": ".ar",
        "tr": ".tr",
        "hu": ".hu",
        "pl": ".pl",
        "zh": ".zh",
        "no": ".no",
        "fi": ".fi",
        "sv": ".sv",
        "da": ".da",
        "ja": ".ja",
        "ko": ".ko",
    }

    # ── XPATHS (especificación del proyecto) ─────────────────────
    XPATHS: dict = {
        "hotel_name":       "//div[@id='wrap-hotelpage-top']/div[2]/div[1]/div[2]/h2[1]",
        "address":          "//*[@id='wrap-hotelpage-top']/div[2]/div/div[3]/div/div/div/div/span[1]/button/div",
        "description":      "//p[@data-testid='property-description']",
        "reviews":          "//div[@data-testid='review-score-component']",
        "review_subscores": "//div[@data-testid='ReviewSubscoresDesktop']/following-sibling::div[1]",
        "facilities":       "//*[@id='hp_facilities_box']",
        "policies":         "//*[@id='policies']",
        "important_info":   "//*[@id='important_info']",
        "rooms":            "//*[@id='maxotelRoomArea']",
        "gallery":          "//div[@data-testid='GalleryGridViewModal-wrapper']",
        "hotel_page":       "//*[@id='b2hotelPage']",
    }

    # CSS Selectors alternativos
    CSS_SELECTORS: dict = {
        "gallery_button":  "[data-testid='gallery-button']",
        "gallery_images":  "img[data-testid='gallery-image']",
        "facility_items":  "div.facility-item",
        "room_cards":      "div.room-card",
    }

    # ── RUTAS (Windows absolutas) ─────────────────────────────────
    BASE_DATA_PATH: str = "C:\\BookingScraper\\data"
    IMAGES_PATH:    str = "C:\\BookingScraper\\data\\images"
    EXPORTS_PATH:   str = "C:\\BookingScraper\\data\\exports"
    LOGS_PATH:      str = "C:\\BookingScraper\\data\\logs"

    # ── IMÁGENES ─────────────────────────────────────────────────
    IMAGE_QUALITY:    int = 85
    IMAGE_MAX_WIDTH:  int = 1920
    IMAGE_MAX_HEIGHT: int = 1080
    DOWNLOAD_IMAGES:  bool = True
    MAX_IMAGE_WORKERS: int = 5

    # ── BATCH ────────────────────────────────────────────────────
    BATCH_SIZE:           int = 5
    MAX_CONCURRENT_TASKS: int = 3

    # ── EXPORTACIÓN ──────────────────────────────────────────────
    EXPORT_FORMATS: List[str] = ["csv", "json", "excel"]

    # ── LOGGING ──────────────────────────────────────────────────
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = (
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan> - "
        "<level>{message}</level>"
    )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="allow",
    )


# Instancia global
settings = Settings()


def create_directories() -> None:
    """Crea los directorios de datos si no existen."""
    for path in [
        settings.BASE_DATA_PATH,
        settings.IMAGES_PATH,
        settings.EXPORTS_PATH,
        settings.LOGS_PATH,
    ]:
        os.makedirs(path, exist_ok=True)


create_directories()
