"""
BookingScraper/app/image_downloader.py
Descargador de imagenes optimizado para Booking.com
Windows 11 + Python 3.14.3

CORRECCIONES v1.4 [REGLA IDIOMA INGLÉS]:
  [FIX CRÍTICO] Regla del sistema: "las imágenes SOLO se descargan con el idioma inglés".
    download_images() ahora tiene default language='en' y añade un guard que registra
    un warning si se llama con language != 'en', garantizando trazabilidad.
    El caller (scraper_service.py v4.0) ya garantiza que solo llama con lang='en',
    pero esta defensa adicional previene regresiones futuras.
    El directorio de destino es SIEMPRE hotel_{id}/en/ independientemente del parámetro.

CORRECCIONES v1.3:
  [FIX] Filtro de dimensiones minimas: descarta imagenes < 200x150 px.
  [NEW] MIN_WIDTH / MIN_HEIGHT configurables como constantes de clase.

CORRECCIONES v1.2:
  [FIX CRITICO] download_images() acepta session=requests.Session opcional.
  [FIX] _download_single() usa la sesion compartida en lugar de requests.get()

CORRECCIONES v1.1:
  [OK] Logica correcta - sin bugs en el original
  [FIX] Import verificado: from app.config import settings
  [FIX] Pillow Image.Resampling.LANCZOS (API moderna, no ANTIALIAS)
  [FIX] _get_extension: maneja None como formato
  [NEW] Soporte para extension de idioma en subdirectorios
"""

import hashlib
import io
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional

import requests
from loguru import logger
from PIL import Image

from app.config import settings   # ✅ Import path correcto


class ImageDownloader:
    """Gestor de descarga y optimización de imágenes del hotel."""

    # Dimensiones minimas aceptadas — descarta iconos, avatares y tracking pixels
    MIN_WIDTH:  int = 200   # px
    MIN_HEIGHT: int = 150   # px

    def __init__(self, base_path: str = None, quality: int = None):
        """
        Args:
            base_path: Ruta base para guardar imágenes.
            quality:   Calidad JPEG (0–100).
        """
        self.base_path   = Path(base_path or settings.IMAGES_PATH)
        self.quality     = quality or settings.IMAGE_QUALITY
        self.max_width   = settings.IMAGE_MAX_WIDTH
        self.max_height  = settings.IMAGE_MAX_HEIGHT
        self.max_workers = settings.MAX_IMAGE_WORKERS

        self.base_path.mkdir(parents=True, exist_ok=True)

        self._reset_stats()
        logger.info(f"ImageDownloader iniciado | ruta: {self.base_path}")

    def _reset_stats(self):
        self.stats = {"total": 0, "success": 0, "failed": 0, "skipped": 0}

    # ── DESCARGA EN PARALELO ──────────────────────────────────────────────────

    def download_images(
        self,
        hotel_id: int,
        image_urls: List[str],
        language: str = "en",
        room_id: Optional[int] = None,
        session=None,  # [v1.2] requests.Session con cookies del browser Brave
    ) -> List[Dict]:
        """
        Descarga imágenes en paralelo con deduplicación.

        [v1.4 REGLA IDIOMA] Las imágenes SOLO se descargan con idioma inglés ('en').
        Esta función acepta 'language' pero siempre usa 'en' como subdirectorio.
        El caller (scraper_service.py) ya garantiza que solo llama para lang='en'.

        Args:
            hotel_id:   ID del hotel.
            image_urls: Lista de URLs.
            language:   Parámetro mantenido por compatibilidad. SIEMPRE se usa 'en'.
            room_id:    ID de habitación (opcional).

        Returns:
            Lista de resultados con metadatos de cada imagen.
        """
        if not image_urls:
            logger.warning(f"Sin imágenes para hotel {hotel_id}")
            return []

        # [v1.4 FIX] Regla del sistema: imágenes SOLO bajo carpeta 'en'
        # Si se llama con otro idioma, advertir y forzar 'en'
        _IMAGES_LANGUAGE = "en"
        if language != _IMAGES_LANGUAGE:
            logger.warning(
                f"  📷 [{hotel_id}] download_images() llamado con language='{language}' "
                f"— forzando a '{_IMAGES_LANGUAGE}' (regla: imágenes solo en inglés)"
            )
            language = _IMAGES_LANGUAGE

        self._reset_stats()
        self.stats["total"] = len(image_urls)
        # Guardar la sesion para que _download_single la use
        self._session = session

        # Directorio destino — SIEMPRE bajo 'en/'
        hotel_dir = self.base_path / f"hotel_{hotel_id}" / language
        if room_id:
            target_dir = hotel_dir / f"room_{room_id}"
        else:
            target_dir = hotel_dir
        target_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"📷 Descargando {len(image_urls)} imágenes | hotel={hotel_id} lang={language} | dir={target_dir}")

        results: List[Dict] = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self._download_single, url, target_dir, idx, hotel_id, room_id): url
                for idx, url in enumerate(image_urls)
            }
            for future in as_completed(futures):
                url = futures[future]
                try:
                    result = future.result()
                    if result:
                        results.append(result)
                        self.stats["success"] += 1
                    else:
                        self.stats["failed"] += 1
                except Exception as e:
                    logger.error(f"Error descargando {url}: {e}")
                    self.stats["failed"] += 1

        logger.success(
            f"✓ Descarga completa | {self.stats['success']}/{self.stats['total']} OK "
            f"| {self.stats['failed']} fallidas | {self.stats['skipped']} saltadas"
        )
        return results

    # ── DESCARGA INDIVIDUAL ───────────────────────────────────────────────────

    def _download_single(
        self,
        url: str,
        save_dir: Path,
        index: int,
        hotel_id: int,
        room_id: Optional[int],
    ) -> Optional[Dict]:
        """Descarga y optimiza una imagen individual."""
        try:
            url_hash = hashlib.md5(url.encode()).hexdigest()[:12]

            # Deduplicación por hash
            existing = list(save_dir.glob(f"*{url_hash}*"))
            if existing:
                logger.debug(f"⏭️ Ya existe: {existing[0].name}")
                self.stats["skipped"] += 1
                return None

            # Descargar usando la sesion autenticada (cookies de Brave) o requests directo
            _requester = getattr(self, "_session", None) or requests
            response = _requester.get(
                url,
                timeout=30,
                stream=True,
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                                       "Chrome/124.0.0.0 Safari/537.36"},
            )
            if response.status_code != 200:
                logger.warning(f"HTTP {response.status_code}: {url[:80]}")
                return None

            # Abrir imagen
            img = Image.open(io.BytesIO(response.content))
            original_format = img.format or "JPEG"
            ext = self._get_extension(original_format)

            # [v1.3] Descartar imagenes demasiado pequeñas (iconos, avatares, tracking pixels)
            w_orig, h_orig = img.size
            if w_orig < self.MIN_WIDTH or h_orig < self.MIN_HEIGHT:
                logger.debug(
                    f"⏭️ Descartada imagen pequeña {w_orig}×{h_orig}px "
                    f"(min {self.MIN_WIDTH}×{self.MIN_HEIGHT}): {url[:60]}"
                )
                self.stats["skipped"] += 1
                return None

            # Redimensionar si excede límites
            img = self._resize_image(img)

            # Convertir RGBA → RGB para JPEG
            if img.mode in ("RGBA", "LA", "P") and ext in ("jpg", "jpeg"):
                bg = Image.new("RGB", img.size, (255, 255, 255))
                if img.mode == "P":
                    img = img.convert("RGBA")
                bg.paste(img, mask=img.split()[-1])
                img = bg
            elif img.mode not in ("RGB", "L") and ext in ("jpg", "jpeg"):
                img = img.convert("RGB")

            # Guardar
            filename = f"img_{index:04d}_{url_hash}.{ext}"
            filepath = save_dir / filename
            save_kwargs: Dict = {"optimize": True, "quality": self.quality}
            if ext in ("jpg", "jpeg"):
                save_kwargs["progressive"] = True

            img.save(filepath, **save_kwargs)

            size = filepath.stat().st_size
            w, h = img.size
            logger.debug(f"✓ {filename} ({size:,} bytes, {w}×{h})")

            return {
                "url":        url,
                "local_path": str(filepath.relative_to(self.base_path)),
                "filename":   filename,
                "file_size":  size,
                "width":      w,
                "height":     h,
                "format":     ext.upper(),
                "hotel_id":   hotel_id,
                "room_id":    room_id,
            }

        except requests.RequestException as e:
            logger.warning(f"Error de red: {e}")
        except Image.UnidentifiedImageError:
            logger.warning(f"Formato no reconocido: {url[:80]}")
        except Exception as e:
            logger.error(f"Error inesperado: {e}")

        return None

    # ── UTILIDADES ────────────────────────────────────────────────────────────

    def _resize_image(self, img: Image.Image) -> Image.Image:
        """Redimensiona si supera los límites configurados."""
        w, h = img.size
        if w <= self.max_width and h <= self.max_height:
            return img
        ratio = min(self.max_width / w, self.max_height / h)
        new_size = (int(w * ratio), int(h * ratio))
        # ✅ FIX: Image.Resampling.LANCZOS (ANTIALIAS eliminado en Pillow 10)
        return img.resize(new_size, Image.Resampling.LANCZOS)

    def _get_extension(self, fmt: Optional[str]) -> str:
        """Convierte formato PIL a extensión de archivo."""
        if not fmt:
            return "jpg"
        return {
            "JPEG": "jpg", "JPG": "jpg",
            "PNG":  "png", "WEBP": "webp",
            "GIF":  "gif", "BMP":  "bmp",
            "TIFF": "tiff",
        }.get(fmt.upper(), "jpg")

    def verify_image(self, filepath: Path) -> bool:
        """Verifica integridad de una imagen."""
        try:
            with Image.open(filepath) as img:
                img.verify()
            return True
        except Exception:
            return False

    def cleanup_invalid_images(self, directory: Path) -> int:
        """Elimina imágenes corruptas y devuelve el conteo eliminado."""
        removed = 0
        for pattern in ("*.jpg", "*.jpeg", "*.png", "*.webp"):
            for fp in directory.rglob(pattern):
                if not self.verify_image(fp):
                    fp.unlink()
                    removed += 1
                    logger.debug(f"🗑️ Eliminada imagen corrupta: {fp.name}")
        return removed

    def get_hotel_images_count(self, hotel_id: int) -> int:
        """Devuelve el número de imágenes descargadas para un hotel."""
        hotel_dir = self.base_path / f"hotel_{hotel_id}"
        if not hotel_dir.exists():
            return 0
        return sum(1 for f in hotel_dir.rglob("*") if f.is_file())

    def get_total_size_bytes(self, hotel_id: Optional[int] = None) -> int:
        """Tamaño total en bytes (hotel específico o todos)."""
        base_dir = self.base_path / f"hotel_{hotel_id}" if hotel_id else self.base_path
        if not base_dir.exists():
            return 0
        return sum(f.stat().st_size for f in base_dir.rglob("*") if f.is_file())

    def get_statistics(self) -> Dict:
        """Estadísticas de la última descarga."""
        return self.stats.copy()


# ── TEST STANDALONE ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("✅ image_downloader.py cargado correctamente")
