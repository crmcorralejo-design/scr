"""
BookingScraper/app/extractor.py  v3.0
Extractor de datos HTML de Booking.com
Windows 11 + Python 3.14.3

CAMBIOS v3.0 [VALIDACIÓN DE IDIOMA EN CAMPOS EXTRAÍDOS]:

  DIAGNÓSTICO CONFIRMADO con datos reales (CSV exportado de BD):
  ─────────────────────────────────────────────────────────────
  ● url_id=322, lang=en (id=637):
      rating_category = 'Exceptional'  ← CORRECTO (en inglés ✓)
      description / services / facilities / house_rules ← TODO EN ESPAÑOL ✗
    CAUSA: Booking.com con IP española sirve el bloque de puntuación
    (review-score-component) en inglés según la URL, pero los bloques de
    contenido textual (description, facilities, house_rules) en el idioma
    de la sesión HTTP (español). El scraper guardaba sin validar el idioma.

  ● url_id=321 / 323 / 324: sin registro EN en absoluto.
    El inglés nunca se guardó (scrape fallido o skipped).

  ● url_id=323 lang=it y lang=es: descripción en INGLÉS guardada bajo
    idioma incorrecto. El scraper recibió página en inglés pero la guardó
    como 'it'/'es' porque detected_lang no estaba validado contra requested_lang.

  [FIX #A - CRÍTICO] Nuevo método _validate_lang(text, language) → bool
    Detecta el idioma de un texto usando diccionarios de señales por idioma
    (EN, ES, DE, FR, IT, PT, NL, RU). Retorna False si el texto pertenece
    claramente a un idioma distinto al solicitado.
    Umbral: si señales negativas >= 3 Y superan señales positivas → rechazado.
    Textos cortos (<30 chars) o sin señales claras → aceptados por defecto.

  [FIX #B] extract_description(): cada fallback valida idioma con _validate_lang.
    Si el texto no está en el idioma solicitado, pasa al siguiente fallback.
    Si ningún fallback pasa → retorna None (mejor vacío que descripción incorrecta).
    JSON-LD description: advertido como unreliable cuando IP != país solicitado.

  [FIX #C] extract_services(): valida idioma de la lista completa antes de retornar.
    Nuevo helper _filter_by_language() evalúa muestra de los primeros 10 ítems.
    Si la muestra falla → lista vacía (no guardar servicios en idioma incorrecto).

  [FIX #D] extract_facilities(): valida idioma de categoría + primeros ítems.
    Categorías en idioma incorrecto se descartan individualmente.

  [FIX #E] extract_house_rules(): valida idioma antes de retornar.

  [FIX #F] extract_important_info(): añadidos selectores modernos 2026
    (data-testid="ImportantInfo", "important-information") + validación idioma.

  [FIX #G] _SERVICE_NOISE_RE: ampliado con 20+ términos UI adicionales en EN/ES/DE/FR/IT.

CAMBIOS v2.8: extract_description() usa find(attrs=...) sin restricción de tag.
              JSON-LD como fallback. Eliminado og:description (idioma sesión = incorrecto).
CAMBIOS v2.7: extract_name() + extract_rating_category() mejorados.
CAMBIOS v2.6: extract_images() sin límite artificial.
CAMBIOS v2.1-2.5: mejoras incrementales (ver historial git).
"""

import re
import json
from typing import Dict, List, Optional

from bs4 import BeautifulSoup
from loguru import logger


class BookingExtractor:
    """
    Extractor de datos de hoteles desde HTML de Booking.com.
    Soporta multi-idioma con múltiples selectores fallback.
    Estructura Booking.com 2025/2026.
    """

    def __init__(self, html_content: str, language: str = "en"):
        self.html_content = html_content
        self.language     = language
        self.tree         = None
        self.soup: Optional[BeautifulSoup] = None
        self._parse(html_content)

    def _parse(self, html_content: str):
        """Parsea el HTML."""
        try:
            from lxml import html as lhtml
            self.tree = lhtml.fromstring(html_content)
        except Exception:
            self.tree = None

        try:
            self.soup = BeautifulSoup(html_content, "lxml")
        except Exception:
            try:
                self.soup = BeautifulSoup(html_content, "html.parser")
            except Exception as e:
                logger.error(f"Error parseando HTML: {e}")
                self.soup = None

    # ── VALIDACIÓN DE IDIOMA ─────────────────────────────────────────────────
    # [v3.0] Señales de texto de alta frecuencia y baja ambigüedad por idioma.
    # Sólo palabras comunes en descripciones de Booking.com.
    _LANG_SIGNALS: dict = {
        "en": {
            "pos": ["the ", " and ", " with ", "hotel", "beach", "pool",
                    "breakfast", "free ", "offers", "features", "located",
                    "includes", "available", "property", "resort", "swimming",
                    "outdoor", "rooms", "guests", "access", "views"],
            "neg": ["está ", "dispone", "habitaci", "alojamiento", "ofrece ",
                    "también", "piscina", "desayuno", "normas", "entrada ",
                    "salida ", "disponibilidad", "aceptamos", "cancelaci",
                    "auch ", "verfüg", "unterkunft", "l'hôtel", "dispose",
                    "camera ", "spiaggia"],
        },
        "es": {
            "pos": ["está ", "dispone", "habitaci", "alojamiento", "ofrece ",
                    "también", "piscina", "desayuno", "normas", "disponibilidad",
                    "cancelaci", "entrada ", "salida ", "recepci", "servicios"],
            "neg": ["the hotel", "swimming pool", "free wifi", "checkout",
                    "breakfast included", "outdoor pool", "das hotel",
                    "l'hôtel", "dispose de"],
        },
        "de": {
            "pos": ["das ", " und ", "mit ", "bietet", "verfüg", "zimmer",
                    "strand", "kostenlos", "frühstück", "unterkunft", "auch ",
                    "befindet", "ausstattung", "bewertung", "angebot"],
            "neg": ["está ", "dispone", "habitaci", "desayuno",
                    "the hotel", "swimming pool", "l'hôtel", "dispose"],
        },
        "fr": {
            "pos": ["l'hôtel", "les ", "avec ", "dispose", "offre ", "plage",
                    "petit-déjeuner", "gratuit", "chambres", "piscine",
                    "l'établissement", "situé", "propose"],
            "neg": ["está ", "dispone", "habitaci", "desayuno",
                    "the hotel", "swimming pool", "das hotel"],
        },
        "it": {
            "pos": ["l'hotel", "della ", "con ", "dispone", "offre ", "spiaggia",
                    "colazione", "piscina", "gratuito", "camere", "struttura",
                    "situato", "propone"],
            "neg": ["está ", "habitaci", "desayuno", "the hotel", "swimming pool"],
        },
        "pt": {
            "pos": ["o hotel", "com ", "possui", "praia", "café da manhã",
                    "piscina", "quartos", "localizado", "gratuito"],
            "neg": ["está ", "habitaci"],
        },
        "nl": {
            "pos": ["het hotel", "met ", "beschikt", "strand", "ontbijt",
                    "zwembad", "gratis", "kamers", "gelegen"],
            "neg": ["está ", "habitaci"],
        },
        "ru": {
            "pos": ["отель", "пляж", "бассейн", "завтрак", "номер", "расположен"],
            "neg": [],
        },
    }

    def _validate_lang(self, text: str, language: str = None) -> bool:
        """
        [v3.0] Valida que un texto extraído esté en el idioma esperado.

        Algoritmo:
          1. Texto muy corto (<30 chars) → aceptar (no hay suficiente señal)
          2. Calcular score_pos: hits de señales del idioma solicitado
          3. Calcular score_neg: hits de señales de otros idiomas
          4. Si score_neg >= 3 AND score_neg > score_pos → texto en idioma incorrecto → False
          5. En cualquier otro caso → True

        Returns:
            True  → texto en idioma correcto o indeterminado
            False → texto claramente en idioma incorrecto
        """
        lang = (language or self.language or "en").lower()
        if not text or len(text.strip()) < 30:
            return True

        text_lower = text.lower()
        signals = self._LANG_SIGNALS.get(lang, {})
        pos_signals = signals.get("pos", [])
        neg_signals = signals.get("neg", [])

        score_pos = sum(1 for s in pos_signals if s in text_lower)
        score_neg = sum(1 for s in neg_signals if s in text_lower)

        if score_neg >= 3 and score_neg > score_pos:
            logger.debug(
                f"  [lang_validate] RECHAZADO lang='{lang}' "
                f"pos={score_pos} neg={score_neg} | '{text[:70]}...'"
            )
            return False
        return True

    def _filter_by_language(self, items: list) -> list:
        """
        [v3.0] Evalúa una lista de strings como muestra. Si la muestra
        no pasa _validate_lang, devuelve lista vacía (todos incorrectos).
        """
        if not items:
            return []
        # Concatenar muestra para evaluación conjunta
        sample = " ".join(items[:10])
        if not self._validate_lang(sample):
            logger.debug(
                f"  [lang_validate] Lista de {len(items)} ítems descartada "
                f"(idioma incorrecto para '{self.language}')"
            )
            return []
        return items

    # ── UTILIDADES ────────────────────────────────────────────────────────────

    @staticmethod
    def _clean_address(v: str) -> Optional[str]:
        """
        Elimina texto de puntuacion/valoracion que Booking.com pega en el
        mismo bloque DOM que la direccion fisica del hotel.

        Ejemplos de ruido capturado:
          '..., SeychellesUbicacionexcelente, puntuada con 9.1/10!'
          '..., BahamasDespues de reservar, encontraras todos los datos...'
          '..., ChurchDestacado por los clientes'
        """
        if not v:
            return None
        noise_triggers = [
            r'Ubicaci[oó]n', r'Excellent\s+location', r'Great\s+location',
            r'Location\b', r'[Vv]alorad', r'puntuada', r'basada\s+en\s*\d',
            r'comentarios', r'Ver\s+mapa', r'Show\s+on\s+map',
            r'\d+\s*/\s*10', r'[Pp]untuaci[oó]n', r'[Rr]ated\s+by',
            r'customers?', r'[Dd]estacado', r'[Dd]e\s+las\s+m[aá]s',
            r'[Vv]aloradas?', r'[Vv]alued\s+by', r'[Dd]espu[eé]s\s+de\s+reservar',
            r'encontrar[aá]s', r'n[uú]mero\s+de\s+tel[eé]fono',
        ]
        pattern = '|'.join(f'(?:{p})' for p in noise_triggers)
        m = re.search(pattern, v, re.IGNORECASE)
        if m:
            v = v[:m.start()].strip().rstrip('.,;– \n\t')
        return (v[:200] if len(v) > 200 else v).strip() or None

    @staticmethod
    def _normalize_img_url(url: str) -> str:
        """
        Normaliza URLs de imagenes de Booking.com a la resolucion maxima.
        Cubre TODOS los formatos del CDN bstatic.com:
          /max500/     -> /max1280x900/   (un solo numero)
          /max300/     -> /max1280x900/
          /max500x334/ -> /max1280x900/   (dos numeros)
          /square60/   -> /max1280x900/   (miniatura cuadrada)
        """
        url = re.sub(r'/max\d+x\d+x?\d*/', '/max1280x900/', url)
        url = re.sub(r'/max\d+/',          '/max1280x900/', url)
        url = re.sub(r'/square\d+/',       '/max1280x900/', url)
        return url

    def _xpath_text(self, xpath_expr: str) -> Optional[str]:
        if self.tree is None:
            return None
        try:
            elements = self.tree.xpath(xpath_expr)
            if elements:
                elem = elements[0]
                text = (elem if isinstance(elem, str) else elem.text_content()).strip()
                return text or None
        except Exception as e:
            logger.debug(f"XPath error ({xpath_expr[:60]}): {e}")
        return None

    def _xpath_list(self, xpath_expr: str) -> List[str]:
        if self.tree is None:
            return []
        try:
            return [
                (e if isinstance(e, str) else e.text_content()).strip()
                for e in self.tree.xpath(xpath_expr)
                if (e if isinstance(e, str) else e.text_content()).strip()
            ]
        except Exception:
            return []

    def _find_text(self, *args, **kwargs) -> Optional[str]:
        """BeautifulSoup find + get_text seguro."""
        if self.soup is None:
            return None
        try:
            elem = self.soup.find(*args, **kwargs)
            if elem:
                return elem.get_text(strip=True) or None
        except Exception:
            pass
        return None

    def _meta(self, prop: str = None, name: str = None) -> Optional[str]:
        """Extrae content de una meta tag."""
        if self.soup is None:
            return None
        try:
            if prop:
                tag = self.soup.find("meta", property=prop)
            else:
                tag = self.soup.find("meta", attrs={"name": name})
            if tag and tag.get("content"):
                return tag["content"].strip() or None
        except Exception:
            pass
        return None

    # ── DETECCIÓN DE PÁGINA REAL ──────────────────────────────────────────────

    def is_real_hotel_page(self) -> bool:
        """Devuelve True si parece una página real de hotel (no consentimiento)."""
        if self.soup is None:
            return False
        html_lower = self.html_content.lower()
        # Señales de página de hotel real
        has_hotel_signals = any(k in html_lower for k in [
            "property-description",
            "hp_facilities_box",
            "maxotelroomarea",
            "reviewscore",
            "review-score",
            "b2hotelpage",
            "hoteldetails",
        ])
        # Señales de consentimiento (página vacía)
        is_consent = any(k in html_lower for k in [
            "privacymanager",
            "optanon",
            "cookie-consent",
            "cookieconsentpopup",
        ]) and not has_hotel_signals
        return has_hotel_signals and not is_consent

    # ─────────────────────────────────────────────────────────────────────────
    # EXTRACCIÓN COMPLETA
    # ─────────────────────────────────────────────────────────────────────────

    def extract_all(self) -> Dict:
        result = {
            "name":            self.extract_name(),
            "address":         self.extract_address(),
            "description":     self.extract_description(),
            "rating":          self.extract_rating(),
            "rating_category": self.extract_rating_category(),
            "total_reviews":   self.extract_total_reviews(),
            "review_scores":   self.extract_review_scores(),
            "services":        self.extract_services(),
            "facilities":      self.extract_facilities(),
            "house_rules":     self.extract_house_rules(),
            "important_info":  self.extract_important_info(),
            "rooms":           self.extract_rooms(),
            "images_urls":     self.extract_images(),
            "language":        self.language,
        }
        # Diagnostico: mostrar que campos quedaron vacios
        empty = [k for k, v in result.items()
                 if v is None or v == [] or v == {} or v == ""]
        if empty:
            logger.debug(f"  [extractor] Campos vacios [{self.language}]: {empty}")
        imgs_count = len(result.get("images_urls") or [])
        if imgs_count:
            logger.debug(f"  [extractor] {imgs_count} imagenes extraidas [{self.language}]")
        return result

    # ─────────────────────────────────────────────────────────────────────────
    # NOMBRE (8 fallbacks progresivos)
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _clean_hotel_name(v: str) -> Optional[str]:
        """
        Limpia el nombre del hotel extraído de og:title o meta title.
        Booking.com añade:
          - Prefijo de estrellas:  "★★★★★ Hotel Name" → "Hotel Name"
          - Sufijo Booking.com:    "Hotel Name | Booking.com" → "Hotel Name"
          - Sufijo ciudad/país:    "Hotel Name, City, Country" → "Hotel Name"
            (solo si el sufijo coincide con patrón ciudad corta + país)
        """
        if not v:
            return None
        # Eliminar sufijo " | Booking.com" o " - Booking.com"
        v = re.sub(r'\s*[|\-–]\s*Booking\.com\s*$', '', v, flags=re.IGNORECASE).strip()
        # Eliminar prefijo de estrellas unicode ★☆ y espacios
        v = re.sub(r'^[★☆✦✩\s]+', '', v).strip()
        # Eliminar sufijo ", Ciudad, País" — Booking.com lo añade al og:title.
        # Patrón: ", Palabra(s), Palabra(s)" al final donde los segmentos
        # son relativamente cortos (≤30 chars cada uno) = ciudad + país.
        # NO eliminar si el nombre del hotel en sí contiene comas importantes.
        parts = v.split(',')
        if len(parts) >= 3:
            # Los últimos 2 segmentos son ciudad y país si son cortos
            last_two = parts[-2:]
            if all(len(p.strip()) <= 35 for p in last_two):
                v = ','.join(parts[:-2]).strip().rstrip(',').strip()
        elif len(parts) == 2:
            # Solo 1 sufijo: puede ser ciudad o país; eliminar si es corto
            if len(parts[-1].strip()) <= 35:
                v = parts[0].strip()
        return v if v and len(v) > 2 else None

    def extract_name(self) -> Optional[str]:
        """
        Extrae el nombre del hotel con 8 fallbacks.
        [v2.7] data-testid='title' es el MÁS FIABLE (solo nombre, sin ciudad/país).
        og:title se usa como fallback con limpieza de prefijo ★ y sufijo de ubicación.
        """
        # 1. data-testid="title" (estructura Booking.com 2024-2026 — solo el nombre)
        if self.soup:
            elem = self.soup.find(attrs={"data-testid": "title"})
            if elem:
                v = elem.get_text(strip=True)
                # Limpiar posible prefijo de estrellas
                v = re.sub(r'^[★☆✦✩\s]+', '', v).strip() if v else v
                if v and len(v) > 2:
                    logger.debug(f"  Nombre extraído vía: data-testid='title'")
                    return v

        # 2. data-testid="property-name"
        if self.soup:
            elem = self.soup.find(attrs={"data-testid": "property-name"})
            if elem:
                v = elem.get_text(strip=True)
                v = re.sub(r'^[★☆✦✩\s]+', '', v).strip() if v else v
                if v and len(v) > 2:
                    logger.debug(f"  Nombre extraído vía: data-testid='property-name'")
                    return v

        # 3. og:title (con limpieza de ★ y sufijo ciudad/país)
        v = self._meta(prop="og:title")
        if v:
            v = self._clean_hotel_name(v)
            if v:
                logger.debug(f"  Nombre extraído vía: og:title (limpiado)")
                return v

        # 4. meta name="title"
        v = self._meta(name="title")
        if v:
            v = self._clean_hotel_name(v)
            if v:
                logger.debug(f"  Nombre extraído vía: meta[name=title] (limpiado)")
                return v

        # 5. XPath clásico del proyecto
        v = self._xpath_text(
            '//div[@id="wrap-hotelpage-top"]/div[2]/div[1]/div[2]/h2[1]'
        )
        if v:
            logger.debug(f"  Nombre extraído vía: XPath wrap-hotelpage-top/h2")
            return v

        # 6. h2.pp-header__title
        if self.soup:
            h2 = self.soup.find("h2", class_="pp-header__title")
            if h2:
                v = h2.get_text(strip=True)
                if v:
                    logger.debug(f"  Nombre extraído vía: h2.pp-header__title")
                    return v

        # 7. h1 o h2 con "property" en clase o id
        if self.soup:
            for tag in self.soup.find_all(["h1", "h2"]):
                cls  = " ".join(tag.get("class", []))
                tid  = tag.get("id", "")
                if any(k in (cls + tid).lower() for k in ["property", "hotel", "title", "name"]):
                    v = tag.get_text(strip=True)
                    if v and len(v) > 3:
                        logger.debug(f"  Nombre extraído vía: h1/h2 con clase 'property/hotel/title'")
                        return v

        # 8. JSON-LD structured data
        if self.soup:
            for script in self.soup.find_all("script", type="application/ld+json"):
                try:
                    data = json.loads(script.string or "")
                    if isinstance(data, dict) and data.get("name"):
                        v = data["name"]
                        if isinstance(v, str) and len(v) > 3:
                            logger.debug(f"  Nombre extraído vía: JSON-LD")
                            return v
                except Exception:
                    continue

        logger.warning(f"  ❌ Nombre NO extraído — ningún selector funcionó")
        return None

    # ─────────────────────────────────────────────────────────────────────────
    # DIRECCIÓN
    # ─────────────────────────────────────────────────────────────────────────

    def extract_address(self) -> Optional[str]:
        """
        [v2.4] JSON-LD siempre primero: datos estructurados, nunca contienen
        texto de rating/valoracion. Los selectores DOM usan _clean_address().
        """
        if self.soup is None:
            return None

        # 1. JSON-LD — FUENTE MAS LIMPIA (sin ruido de rating)
        for script in self.soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
                if isinstance(data, dict):
                    addr = data.get("address")
                    if isinstance(addr, dict):
                        street   = addr.get("streetAddress", "").strip()
                        locality = addr.get("addressLocality", "").strip()
                        region   = addr.get("addressRegion", "").strip()
                        postal   = addr.get("postalCode", "").strip()
                        country  = addr.get("addressCountry", "").strip()

                        # streetAddress a menudo YA contiene city y region.
                        # Solo anadir sub-campos si no estan ya en streetAddress.
                        street_lower = street.lower()
                        parts = [street] if street else []
                        for val in [locality, postal, country]:
                            if val and val.lower() not in street_lower:
                                parts.append(val)

                        full = ", ".join(p for p in parts if p)
                        if full and len(full) > 5:
                            return full
                    elif isinstance(addr, str) and len(addr.strip()) > 5:
                        return self._clean_address(addr.strip())
            except Exception:
                continue

        # 2 – 6. Selectores DOM, todos pasan por _clean_address()
        # 2. data-testid="address"
        elem = self.soup.find(attrs={"data-testid": "address"})
        if elem:
            v = self._clean_address(elem.get_text(strip=True))
            if v:
                return v

        # 3. PropertyHeaderAddressDesktop (Booking.com 2026)
        elem = self.soup.find(attrs={"data-testid": re.compile(r"PropertyHeaderAddress|address-line", re.I)})
        if elem:
            v = self._clean_address(elem.get_text(strip=True))
            if v:
                return v

        # 4. XPath clasico del proyecto
        v = self._xpath_text(
            '//*[@id="wrap-hotelpage-top"]/div[2]/div/div[3]/div/div/div/div/span[1]/button/div'
        )
        if v:
            return self._clean_address(v)

        # 5. Clases clasicas
        for cls in ["hp_address_subtitle", "address", "address-text"]:
            elem = self.soup.find(class_=cls)
            if elem:
                v = self._clean_address(elem.get_text(strip=True))
                if v:
                    return v

        # 6. itemprop="address"
        elem = self.soup.find(attrs={"itemprop": "address"})
        if elem:
            v = self._clean_address(elem.get_text(strip=True))
            if v:
                return v

        return None

    # ─────────────────────────────────────────────────────────────────────────
    # DESCRIPCIÓN
    # ─────────────────────────────────────────────────────────────────────────

    def extract_description(self) -> Optional[str]:
        """
        [v3.0] Extraer descripción con validación de idioma en cada fallback.

        CAUSA RAÍZ (confirmada con datos CSV url_id=322 lang=en):
          Booking.com con IP española sirve los bloques de texto (description,
          services, house_rules) en ESPAÑOL aunque la URL tenga ?lang=en-us.
          Solo el bloque de puntuación (review-score-component) se sirve correctamente.
          Resultado: rating_category='Exceptional' (correcto) pero descripción en ES.

        SOLUCIÓN: cada fallback pasa por _validate_lang(). Si el texto está en
        idioma incorrecto → descartado. Si ningún fallback pasa → retorna None.
        Mejor campo vacío que descripción en idioma incorrecto guardada en BD.

        [v2.8] FIX: Booking.com 2025/2026 usa <div> para data-testid="property-description".
               Eliminado og:description (siempre en idioma de sesión = incorrecto).
        """
        if self.soup:
            # 1. data-testid="property-description" — cualquier tag (div, p, section...)
            elem = self.soup.find(attrs={"data-testid": "property-description"})
            if elem:
                text = elem.get_text(separator=" ", strip=True)
                if text and len(text) > 20:
                    if self._validate_lang(text):
                        return text
                    else:
                        logger.debug(
                            f"  [desc] data-testid='property-description' RECHAZADO "
                            f"(idioma incorrecto para '{self.language}'): {text[:60]}"
                        )

            # 2. Variantes de testid (PropertyDescription, property-desc, hotel-description)
            for testid in [
                re.compile(r"^PropertyDescription", re.I),
                re.compile(r"^property-desc", re.I),
                re.compile(r"^hotel-description", re.I),
            ]:
                elem = self.soup.find(attrs={"data-testid": testid})
                if elem:
                    text = elem.get_text(separator=" ", strip=True)
                    if text and len(text) > 20 and self._validate_lang(text):
                        return text

        # 3. XPath — cualquier tag
        v = self._xpath_text('//*[@data-testid="property-description"]')
        if v and len(v) > 20 and self._validate_lang(v):
            return v

        # 4. div#property_description_content (estructura legacy pre-2024)
        if self.soup:
            div = self.soup.find("div", id="property_description_content")
            if div:
                paragraphs = div.find_all("p")
                text = " ".join(p.get_text(strip=True) for p in paragraphs)
                if text and self._validate_lang(text):
                    return text

        # 5. div clase hotel_desc (legacy)
        if self.soup:
            div = self.soup.find("div", class_=re.compile(r"hotel.?desc", re.I))
            if div:
                text = div.get_text(strip=True)
                if text and len(text) > 20 and self._validate_lang(text):
                    return text

        # 6. JSON-LD description
        #    ⚠️ ADVERTENCIA: Booking.com sirve JSON-LD en idioma de SESIÓN (IP),
        #    NO en el idioma de la URL. Sólo se acepta si pasa validación de idioma.
        if self.soup:
            for script in self.soup.find_all("script", type="application/ld+json"):
                try:
                    data = json.loads(script.string or "")
                    if isinstance(data, dict):
                        desc = data.get("description")
                        if desc and isinstance(desc, str) and len(desc) > 30:
                            if self._validate_lang(desc):
                                return desc.strip()
                            else:
                                logger.debug(
                                    f"  [desc] JSON-LD description RECHAZADO "
                                    f"(idioma sesión != '{self.language}'): {desc[:60]}"
                                )
                except Exception:
                    continue

        # NOTA: og:description ELIMINADO — siempre en idioma de sesión (incorrecto).
        # Si ningún fallback pasó validación → retornar None (no guardar dato incorrecto).
        return None

    # ─────────────────────────────────────────────────────────────────────────
    # RATING NUMÉRICO
    # ─────────────────────────────────────────────────────────────────────────

    def extract_rating(self) -> Optional[float]:
        # 1. data-testid="review-score-component"
        if self.soup:
            elem = self.soup.find(attrs={"data-testid": "review-score-component"})
            if elem:
                text = elem.get_text()
                m = re.search(r'(\d+[.,]\d+)', text)
                if m:
                    try:
                        return float(m.group(1).replace(",", "."))
                    except Exception:
                        pass

        # 2. XPath review-score
        v = self._xpath_text('//div[@data-testid="review-score-component"]')
        if v:
            m = re.search(r'(\d+[.,]\d+)', v)
            if m:
                try:
                    return float(m.group(1).replace(",", "."))
                except Exception:
                    pass

        # 3. aria-label con puntuación
        if self.soup:
            for elem in self.soup.find_all(attrs={"aria-label": True}):
                label = elem.get("aria-label", "")
                m = re.search(r'(\d+[.,]\d+)\s*(?:out\s*of|\/)', label)
                if m:
                    try:
                        return float(m.group(1).replace(",", "."))
                    except Exception:
                        pass

        # 4. itemprop ratingValue
        if self.soup:
            elem = self.soup.find(attrs={"itemprop": "ratingValue"})
            if elem:
                content = elem.get("content") or elem.get_text()
                m = re.search(r'(\d+[.,]\d+)', content)
                if m:
                    try:
                        return float(m.group(1).replace(",", "."))
                    except Exception:
                        pass

        # 5. JSON-LD
        if self.soup:
            for script in self.soup.find_all("script", type="application/ld+json"):
                try:
                    data = json.loads(script.string or "")
                    if isinstance(data, dict):
                        agg = data.get("aggregateRating", {})
                        rv  = agg.get("ratingValue")
                        if rv:
                            return float(rv)
                except Exception:
                    continue

        return None

    # ─────────────────────────────────────────────────────────────────────────
    # CATEGORÍA DE RATING ("Excepcional", "Fabuloso", etc.)
    # ─────────────────────────────────────────────────────────────────────────

    def extract_rating_category(self) -> Optional[str]:
        """
        [v2.7] Categorías de rating completas para todos los idiomas soportados.
        Booking.com usa diferentes palabras según el rango de puntuación:
          9.0-10:  Exceptional / Excepcional / Hervorragend / Exceptionnel / Eccezionale
          8.0-8.9: Fabulous    / Fabuloso    / Fabelhaft    / Fabuleux     / Favoloso
                   Excellent   / Excelente   / Ausgezeichnet
          7.0-7.9: Very good   / Muy bien    / Sehr gut     / Très bien    / Molto buono
          6.0-6.9: Good        / Bien        / Gut          / Bien         / Buono
          5.0-5.9: Pleasant    / Agradable   / Angenehm     / Agréable     / Piacevole
        """
        CATEGORIES = {
            "en": [
                "Exceptional", "Superb", "Fabulous", "Excellent",
                "Very good", "Good", "Pleasant", "No rating",
            ],
            "es": [
                "Excepcional", "Fabuloso", "Espléndido", "Excelente",
                "Muy bien", "Bien", "Agradable",
            ],
            "de": [
                "Hervorragend", "Fantastisch", "Ausgezeichnet", "Fabelhaft",
                "Sehr gut", "Gut", "Angenehm",
            ],
            "fr": [
                "Exceptionnel", "Fabuleux", "Superbe", "Excellent",
                "Très bien", "Bien", "Agréable",
            ],
            "it": [
                "Eccezionale", "Favoloso", "Fantastico", "Eccellente",
                "Molto buono", "Buono", "Piacevole",
            ],
            "pt": [
                "Excepcional", "Fabuloso", "Soberbo", "Excelente",
                "Muito bom", "Bom", "Agradável",
            ],
            "nl": [
                "Uitzonderlijk", "Fantastisch", "Uitstekend",
                "Zeer goed", "Goed", "Aangenaam",
            ],
            "ru": [
                "Исключительно", "Великолепно", "Отлично",
                "Очень хорошо", "Хорошо",
            ],
        }
        # Buscar en idioma del documento + inglés como fallback universal
        search_cats = (
            CATEGORIES.get(self.language, []) +
            CATEGORIES.get("en", [])
        )
        # Eliminar duplicados manteniendo orden
        seen = set()
        search_cats = [c for c in search_cats if not (c in seen or seen.add(c))]

        if self.soup:
            # 1. Buscar en review-score component (fuente más fiable)
            elem = self.soup.find(attrs={"data-testid": "review-score-component"})
            if elem:
                text_content = elem.get_text()
                for cat in search_cats:
                    if cat.lower() in text_content.lower():
                        return cat

            # 2. aria-label en elementos del bloque de puntuación
            score_block = self.soup.find(attrs={"data-testid": re.compile(r"review-score|rating", re.I)})
            if score_block:
                for tag in score_block.find_all(attrs={"aria-label": True}):
                    label = tag.get("aria-label", "")
                    for cat in search_cats:
                        if cat.lower() in label.lower():
                            return cat

            # 3. Scan global de aria-label
            for tag in self.soup.find_all(attrs={"aria-label": True}):
                label = tag.get("aria-label", "")
                for cat in search_cats:
                    if cat.lower() in label.lower():
                        return cat

            # 4. Inferir desde rating numérico si no se encontró texto
            rating = self.extract_rating()
            if rating is not None:
                return self._infer_rating_category_from_score(rating)

        return None

    def _infer_rating_category_from_score(self, score: float) -> Optional[str]:
        """
        [v2.7] Infiere la categoría de rating a partir del score numérico
        cuando no se puede extraer el texto de categoría del DOM.
        """
        SCORE_MAP = {
            "en": [(9.0, "Exceptional"), (8.0, "Excellent"), (7.0, "Very good"), (6.0, "Good"), (0.0, "Pleasant")],
            "es": [(9.0, "Excepcional"), (8.0, "Fabuloso"),  (7.0, "Muy bien"), (6.0, "Bien"), (0.0, "Agradable")],
            "de": [(9.0, "Hervorragend"), (8.0, "Fabelhaft"), (7.0, "Sehr gut"), (6.0, "Gut"), (0.0, "Angenehm")],
            "fr": [(9.0, "Exceptionnel"), (8.0, "Fabuleux"), (7.0, "Très bien"), (6.0, "Bien"), (0.0, "Agréable")],
            "it": [(9.0, "Eccezionale"),  (8.0, "Favoloso"), (7.0, "Molto buono"), (6.0, "Buono"), (0.0, "Piacevole")],
            "pt": [(9.0, "Excepcional"),  (8.0, "Fabuloso"), (7.0, "Muito bom"), (6.0, "Bom"), (0.0, "Agradável")],
            "nl": [(9.0, "Uitzonderlijk"), (8.0, "Fantastisch"), (7.0, "Zeer goed"), (6.0, "Goed"), (0.0, "Aangenaam")],
            "ru": [(9.0, "Исключительно"), (8.0, "Великолепно"), (7.0, "Очень хорошо"), (6.0, "Хорошо"), (0.0, "Хорошо")],
        }
        scale = SCORE_MAP.get(self.language) or SCORE_MAP["en"]
        for threshold, label in scale:
            if score >= threshold:
                return label
        return None

    # ─────────────────────────────────────────────────────────────────────────
    # TOTAL REVIEWS
    # ─────────────────────────────────────────────────────────────────────────

    def extract_total_reviews(self) -> Optional[int]:
        if self.soup:
            # 1. data-testid="review-score-component"
            elem = self.soup.find(attrs={"data-testid": "review-score-component"})
            if elem:
                text = elem.get_text()
                m = re.search(r'([\d,\.]+)\s*(?:review|opinión|Bewertung|avis|recensioni|avaliações)', text, re.IGNORECASE)
                if m:
                    try:
                        return int(re.sub(r'[,\.]', '', m.group(1)))
                    except Exception:
                        pass

            # 2. itemprop reviewCount
            elem = self.soup.find(attrs={"itemprop": "reviewCount"})
            if elem:
                content = elem.get("content") or elem.get_text()
                m = re.search(r'(\d+)', content.replace(",", ""))
                if m:
                    return int(m.group(1))

        # 3. JSON-LD
        if self.soup:
            for script in self.soup.find_all("script", type="application/ld+json"):
                try:
                    data = json.loads(script.string or "")
                    if isinstance(data, dict):
                        rc = data.get("aggregateRating", {}).get("reviewCount")
                        if rc:
                            return int(rc)
                except Exception:
                    continue

        return None

    # ─────────────────────────────────────────────────────────────────────────
    # REVIEW SCORES (puntuaciones por categoría)
    # ─────────────────────────────────────────────────────────────────────────

    def extract_review_scores(self) -> Dict:
        scores = {}
        if self.soup is None:
            return scores

        # 1. ReviewSubscoresDesktop (estructura clasica)
        container = self.soup.find(attrs={"data-testid": "ReviewSubscoresDesktop"})
        if container:
            for item in container.find_all(class_=re.compile(r"subscores|score|category", re.I)):
                text = item.get_text(separator=" ").strip()
                m = re.search(r'^(.+?)\s+(\d+[.,]\d+)\s*$', text)
                if m:
                    try:
                        scores[m.group(1).strip()] = float(m.group(2).replace(",", "."))
                    except Exception:
                        pass
            if scores:
                return scores

        # 2. review-score-category items (Booking.com 2024-2026)
        for elem in self.soup.find_all(attrs={"data-testid": re.compile(r"review.?score.?category|ReviewScore", re.I)}):
            text = elem.get_text(separator=" ").strip()
            m = re.search(r'([A-Za-z\u00C0-\u024F\s]{2,40})\s+(\d+[.,]\d+)', text)
            if m:
                try:
                    score_val = float(m.group(2).replace(",", "."))
                    if 1.0 <= score_val <= 10.0:
                        scores[m.group(1).strip()] = score_val
                except Exception:
                    pass
        if scores:
            return scores

        # 3. JSON-LD aggregateRating con subratings
        for script in self.soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
                if isinstance(data, dict):
                    agg = data.get("aggregateRating", {})
                    if isinstance(agg, dict) and agg.get("ratingValue"):
                        scores["overall"] = float(str(agg["ratingValue"]).replace(",", "."))
                    # reviewAspects
                    for aspect in data.get("reviewAspects", []):
                        name = aspect.get("name") or aspect.get("@type", "")
                        val  = aspect.get("ratingValue")
                        if name and val:
                            try:
                                scores[name] = float(str(val).replace(",", "."))
                            except Exception:
                                pass
                if scores:
                    return scores
            except Exception:
                continue

        # 4. Scan general: pares de texto+puntuacion en el area de reviews
        review_section = self.soup.find(attrs={"data-testid": re.compile(r"review", re.I)})
        if review_section:
            text = review_section.get_text(separator="\n")
            for m in re.finditer(r'([A-Za-z\u00C0-\u024F][A-Za-z\u00C0-\u024F\s]{1,30})\s*\n\s*(\d+[.,]\d+)', text):
                try:
                    score_val = float(m.group(2).replace(",", "."))
                    if 1.0 <= score_val <= 10.0:
                        scores[m.group(1).strip()] = score_val
                except Exception:
                    pass

        return scores

    # ─────────────────────────────────────────────────────────────────────────
    # SERVICIOS
    # ─────────────────────────────────────────────────────────────────────────

    # [v3.0] Texto UI de Booking.com que NO son servicios reales — botones, CTAs, navegación.
    # Ampliado con términos en EN, DE, FR, IT adicionales.
    _SERVICE_NOISE_RE = re.compile(
        r'^(ver\s+disponibilidad|check\s+availability|verfügbarkeit\s+prüfen'
        r'|vérifier\s+la\s+disponibilité|verifica\s+disponibilità|beschikbaarheid\s+controleren'
        r'|ver\s+todas\s+las\s+fotos|see\s+all\s+photos|alle\s+fotos\s+anzeigen'
        r'|see\s+all\s+\d+\s+photos?|show\s+all\s+photos?|view\s+all\s+photos?'
        r'|ver\s+todas|see\s+all|alle\s+anzeigen|voir\s+tout|vedi\s+tutto|bekijk\s+alles'
        r'|reservar|book\s+now|buchen|réserver|prenota|boek\s+nu|book\s+a\s+stay'
        r'|ver\s+mapa|show\s+on\s+map|auf\s+karte|voir\s+carte|mostra\s+mappa'
        r'|mostrar\s+más|show\s+more|mehr\s+anzeigen|voir\s+plus|mostra\s+altro|load\s+more'
        r'|\d+\s+opiniones?|\d+\s+reviews?|\d+\s+bewertungen?|\d+\s+avis|\d+\s+guest\s+reviews?'
        r'|check.?in|check.?out|eincheckzeit|heure\s+d.arrivée|check-in\s+time|check-out\s+time'
        r'|cancelación\s+gratuita|free\s+cancellation|kostenlose\s+stornierung|cancellation\s+policy'
        r'|no\s+se\s+requiere\s+tarjeta|no\s+credit\s+card\s+needed|no\s+prepayment\s+needed'
        r'|breakfast\s+included|desayuno\s+incluido|frühstück\s+inklusive|petit-déjeuner\s+inclus'
        r'|sign\s+in|sign\s+up|log\s+in|iniciar\s+sesión|crear\s+cuenta|registrarse'
        r'|property\s+highlights|property\s+info|about\s+this\s+property|acerca\s+del\s+alojamiento'
        r'|sustainability|certificate\s+of\s+excellence|traveler.s\s+choice'
        r'|see\s+availability|view\s+rooms|select\s+rooms|ver\s+habitaciones'
        r'|genius\s+discount|genius\s+level|best\s+price\s+guarantee|precio\s+mínimo\s+garantizado'
        r'|de\s+pago|paid\s+service|kostenpflichtig|payant|a\s+pagamento'
        r'|solo\s+para\s+adultos|adults\s+only|nur\s+für\s+erwachsene|réservé\s+aux\s+adultes'
        r'|abierta?\s+todo\s+el\s+año|open\s+all\s+year|ganzjährig\s+geöffnet)$',
        re.IGNORECASE
    )

    def extract_services(self) -> List[str]:
        """
        [v3.0] Extrae servicios con validación de idioma.
        Si la lista completa está en idioma incorrecto, devuelve lista vacía.
        Mejor vacío que servicios en español guardados como lang='en'.
        """
        if self.soup is None:
            return []

        def _is_valid_service(text: str) -> bool:
            if not text or len(text) < 3 or len(text) > 120:
                return False
            if self._SERVICE_NOISE_RE.match(text.strip()):
                return False
            if re.match(r'^[\d\s\.,/\-\+%€$£]+$', text):
                return False
            return True

        raw_services = []

        # 1. hp_facilities_box (estructura clásica pre-2024)
        box = self.soup.find(id="hp_facilities_box")
        if box:
            for li in box.find_all(["li", "span"]):
                text = li.get_text(strip=True)
                if _is_valid_service(text) and text not in raw_services:
                    raw_services.append(text)
            if raw_services:
                # [v3.0] Validar idioma ANTES de retornar
                validated = self._filter_by_language(raw_services)
                if validated:
                    return validated[:50]
                raw_services = []  # rechazados → probar selector siguiente

        # 2. data-testid con "facilities", "amenities" o "services" (2024-2026)
        for container in self.soup.find_all(attrs={"data-testid": re.compile(r"facilities|amenities|services", re.I)}):
            for elem in container.find_all(["li", "span", "div"]):
                if elem.find(["li", "div"]):
                    continue
                text = elem.get_text(strip=True)
                if _is_valid_service(text) and text not in raw_services:
                    raw_services.append(text)

        if raw_services:
            return self._filter_by_language(raw_services)[:50]

        return []

    # ─────────────────────────────────────────────────────────────────────────
    # FACILITIES (instalaciones por categoría)
    # ─────────────────────────────────────────────────────────────────────────

    def extract_facilities(self) -> Dict:
        """
        [v3.0] Extrae instalaciones por categoría con validación de idioma.
        Categorías/ítems en idioma incorrecto se descartan individualmente.
        """
        facilities = {}
        if self.soup is None:
            return facilities

        box = self.soup.find(id="hp_facilities_box")
        if not box:
            box = self.soup.find(attrs={"data-testid": re.compile(r"facilities", re.I)})

        if box:
            for section in box.find_all(["div", "section"], recursive=False):
                header = section.find(["h3", "h4", "p"])
                if header:
                    cat   = header.get_text(strip=True)
                    items = [li.get_text(strip=True) for li in section.find_all("li") if li.get_text(strip=True)]
                    if items:
                        # [v3.0] Validar idioma de categoría + primeros ítems como proxy
                        sample = cat + " " + " ".join(items[:3])
                        if self._validate_lang(sample):
                            facilities[cat] = items
                        else:
                            logger.debug(
                                f"  [facilities] Categoría '{cat}' RECHAZADA "
                                f"(idioma incorrecto para '{self.language}')"
                            )

        return facilities

    # ─────────────────────────────────────────────────────────────────────────
    # HOUSE RULES / POLÍTICAS
    # ─────────────────────────────────────────────────────────────────────────

    def extract_house_rules(self) -> Optional[str]:
        """[v3.0] Extrae normas del hotel con validación de idioma."""
        if self.soup:
            candidates = []

            # 1. id="policies" (estructura clásica)
            sec = self.soup.find(id="policies")
            if sec:
                candidates.append(sec.get_text(separator="\n", strip=True))

            # 2. data-testid con "policies", "house-rules", "HouseRules" (2024-2026)
            sec = self.soup.find(attrs={"data-testid": re.compile(
                r"policies|house.?rules|HouseRules|normas|regeln|règles|regole", re.I)})
            if sec:
                candidates.append(sec.get_text(separator="\n", strip=True))

            # 3. id/clase con "house", "rule" o "polic"
            for attr_val in [re.compile(r"house.?rule|house.?policy|hotel.?rule", re.I)]:
                for find_kwargs in [{"id": attr_val}, {"class_": attr_val}]:
                    sec = self.soup.find(**find_kwargs)
                    if sec:
                        candidates.append(sec.get_text(separator="\n", strip=True))

            # [v3.0] Retornar primer candidato que pase validación de idioma
            for candidate in candidates:
                if candidate and self._validate_lang(candidate):
                    return candidate
                elif candidate:
                    logger.debug(
                        f"  [house_rules] Candidato RECHAZADO "
                        f"(idioma incorrecto para '{self.language}'): {candidate[:60]}"
                    )

        return None

    # ─────────────────────────────────────────────────────────────────────────
    # IMPORTANT INFO
    # ─────────────────────────────────────────────────────────────────────────

    def extract_important_info(self) -> Optional[str]:
        """[v3.0] Extrae información importante con selectores 2026 y validación de idioma."""
        if self.soup:
            candidates = []

            # 1. id="important_info" (estructura clásica)
            sec = self.soup.find(id="important_info")
            if sec:
                candidates.append(sec.get_text(separator="\n", strip=True))

            # 2. data-testid variantes (2024-2026)
            for testid in [
                "important-info",
                re.compile(r"ImportantInfo", re.I),
                re.compile(r"important.?information", re.I),
                re.compile(r"need.?to.?know", re.I),
                re.compile(r"a.?tener.?en.?cuenta", re.I),
            ]:
                sec = self.soup.find(attrs={"data-testid": testid})
                if sec:
                    candidates.append(sec.get_text(separator="\n", strip=True))

            # [v3.0] Retornar primer candidato que pase validación.
            # NOTA: important_info puede ser mixta (parte traducida, parte no),
            # se usa umbral más permisivo — se acepta si no hay señales fuertemente negativas.
            for candidate in candidates:
                if candidate and len(candidate) > 10:
                    if self._validate_lang(candidate):
                        return candidate
                    else:
                        logger.debug(
                            f"  [important_info] Candidato RECHAZADO "
                            f"(idioma incorrecto para '{self.language}'): {candidate[:60]}"
                        )

        return None

    # ─────────────────────────────────────────────────────────────────────────
    # HABITACIONES
    # ─────────────────────────────────────────────────────────────────────────

    def extract_rooms(self) -> List[Dict]:
        rooms = []
        seen_names = set()
        if self.soup is None:
            return rooms

        def _add_room(name, price=None, capacity=None, beds=None):
            name = name.strip() if name else None
            if not name or name in seen_names or len(name) < 3:
                return
            seen_names.add(name)
            room = {"name": name}
            if price:
                room["price"] = price.strip()
            if capacity:
                room["capacity"] = capacity.strip()
            if beds:
                room["beds"] = beds.strip()
            rooms.append(room)

        # 1. id="maxotelRoomArea" (estructura clasica)
        area = self.soup.find(id="maxotelRoomArea")
        if area:
            for row in area.find_all(["tr", "div"], class_=re.compile(r"room|hprt")):
                name_e  = row.find(class_=re.compile(r"room.?name|room.?title", re.I))
                price_e = row.find(class_=re.compile(r"price|rate", re.I))
                if name_e:
                    _add_room(name_e.get_text(strip=True),
                              price_e.get_text(strip=True) if price_e else None)

        # 2. data-testid con "roomType" o "room" (estructura 2024-2026)
        if not rooms:
            for container in self.soup.find_all(attrs={"data-testid": re.compile(r"roomType|room.?block|room.?row", re.I)}):
                name_e = (
                    container.find(attrs={"data-testid": re.compile(r"room.?name|room.?type.?name", re.I)})
                    or container.find(["h3", "h4", "strong"])
                )
                price_e = container.find(attrs={"data-testid": re.compile(r"price", re.I)})
                if name_e:
                    _add_room(name_e.get_text(strip=True),
                              price_e.get_text(strip=True) if price_e else None)

        # 3. Tabla HPRT (tabla de habitaciones clasica)
        if not rooms:
            for row in self.soup.find_all(class_=re.compile(r"hprt-table-room|roomtype", re.I)):
                name_e = row.find(class_=re.compile(r"room.?type|room.?name", re.I))
                if name_e:
                    _add_room(name_e.get_text(strip=True))

        # 4. JSON-LD con containsPlace o roomAmenities
        if not rooms:
            for script in self.soup.find_all("script", type="application/ld+json"):
                try:
                    data = json.loads(script.string or "")
                    if isinstance(data, dict):
                        for room_data in data.get("containsPlace", []):
                            name = room_data.get("name")
                            if name:
                                _add_room(name)
                except Exception:
                    continue

        return rooms[:20]

    # ─────────────────────────────────────────────────────────────────────────
    # IMÁGENES
    # ─────────────────────────────────────────────────────────────────────────

    def extract_images(self) -> List[str]:
        """
        [v2.6] Extrae TODAS las fotos reales del hotel y sus habitaciones, SIN LIMITE.

        Patron valido (unico aceptado):
          cf.bstatic.com/xdata/images/hotel/  — fotos de hotel Y habitaciones

        Descartado automaticamente por _is_hotel_photo():
          - t-cf.bstatic.com/design-assets/   (logos, banderas, iconos UI Booking.com)
          - xx.bstatic.com/static/img/review/  (avatares de resenadores)
          - r-xx.bstatic.com/images/user/      (fotos de perfil de usuarios)
          - bstatic.com/xdata/images/xphoto/   (fotos de destino, no del hotel)
          - tracking pixels, GIFs, 1×1

        No hay limite de cantidad: se devuelven todas las URLs encontradas.
        El filtro de dimensiones minimas (200×150 px) en ImageDownloader descarta
        cualquier residuo pequeno que pudiera escapar.
        """
        images = []
        seen   = set()

        def _is_hotel_photo(url: str) -> bool:
            """Acepta solo fotos reales del hotel del CDN de Booking.com."""
            if not url or not url.startswith("http"):
                return False
            # UNICO patron valido: CDN principal de fotos de hotel
            return "bstatic.com/xdata/images/hotel/" in url

        def _add(url: str):
            if not _is_hotel_photo(url):
                return
            # Normalizar a resolucion maxima — usa _normalize_img_url para cubrir
            # /max500/, /max300/, /max1280x900/, /square60/, etc.
            url = self._normalize_img_url(url)
            # Deduplicacion por path base (sin query params de firma)
            base = url.split("?")[0] if "?" in url else url
            if base not in seen:
                seen.add(base)
                images.append(url)

        if self.soup is None:
            return images

        # 1. GalleryGridViewModal (galeria interactiva si esta abierta)
        gallery = self.soup.find(attrs={"data-testid": "GalleryGridViewModal-wrapper"})
        if gallery:
            for img in gallery.find_all("img"):
                _add(img.get("src") or img.get("data-src") or "")

        # 2. b2hotelPage - bloque principal de la ficha del hotel
        b2page = self.soup.find(id="b2hotelPage")
        if not b2page:
            b2page = self.soup.find(attrs={"data-testid": "b2hotelPage"})
        if b2page:
            for img in b2page.find_all("img"):
                _add(img.get("src") or img.get("data-src") or img.get("data-lazy-src") or "")
            for source in b2page.find_all("source"):
                for part in (source.get("srcset", "") or "").split(","):
                    _add(part.strip().split(" ")[0])

        # 3. Scan global por fotos de hotel (captura lo que no este en b2hotelPage)
        for img in self.soup.find_all("img"):
            _add(img.get("src") or img.get("data-src") or img.get("data-lazy-src") or "")
            for part in (img.get("srcset", "") or "").split(","):
                _add(part.strip().split(" ")[0])

        # 4. og:image — fallback si no se encontro nada (debe ser foto del hotel)
        if not images:
            og_img = self._meta(prop="og:image")
            if og_img and _is_hotel_photo(og_img):
                _add(og_img)

        # 5. data-photos JSON embebido en algun elemento
        for tag in self.soup.find_all(attrs={"data-photos": True}):
            try:
                photos = json.loads(tag.get("data-photos", "[]"))
                for p in photos:
                    if isinstance(p, dict):
                        _add(p.get("url") or p.get("src") or "")
                    elif isinstance(p, str):
                        _add(p)
            except Exception:
                pass

        count = len(images)
        if count:
            logger.debug(f"  [extractor] {count} fotos de hotel extraidas [{self.language}]")
        else:
            logger.warning(f"  [extractor] Sin fotos de hotel en [{self.language}]")

        # [v2.6] Sin limite: se devuelven TODAS las fotos reales del hotel.
        # El filtro _is_hotel_photo() ya elimina logos, banderas y avatares.
        return images
