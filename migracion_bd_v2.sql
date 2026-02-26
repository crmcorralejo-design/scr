-- ============================================================
-- BookingScraper Pro - Migración de BD v1.1 → v2.0
-- Ejecutar en pgAdmin o psql ANTES de arrancar la app
-- ============================================================
-- Este script es IDEMPOTENTE: puede ejecutarse varias veces
-- sin causar errores (usa IF NOT EXISTS / DO $$ blocks)
-- ============================================================

-- ── url_queue: columnas faltantes ────────────────────────────

DO $$
BEGIN
    -- scraped_at: fecha en que se scrapeó la URL
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='url_queue' AND column_name='scraped_at'
    ) THEN
        ALTER TABLE url_queue ADD COLUMN scraped_at TIMESTAMP WITHOUT TIME ZONE;
        RAISE NOTICE 'url_queue.scraped_at: AÑADIDA';
    ELSE
        RAISE NOTICE 'url_queue.scraped_at: ya existe';
    END IF;

    -- max_retries: máximo de reintentos permitidos
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='url_queue' AND column_name='max_retries'
    ) THEN
        ALTER TABLE url_queue ADD COLUMN max_retries INTEGER DEFAULT 3;
        UPDATE url_queue SET max_retries = 3 WHERE max_retries IS NULL;
        RAISE NOTICE 'url_queue.max_retries: AÑADIDA';
    ELSE
        RAISE NOTICE 'url_queue.max_retries: ya existe';
    END IF;

    -- language: idioma base de la URL
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='url_queue' AND column_name='language'
    ) THEN
        ALTER TABLE url_queue ADD COLUMN language VARCHAR(10) DEFAULT 'en';
        UPDATE url_queue SET language = 'en' WHERE language IS NULL;
        RAISE NOTICE 'url_queue.language: AÑADIDA';
    ELSE
        RAISE NOTICE 'url_queue.language: ya existe';
    END IF;

    -- retry_count: ya debería existir, pero por si acaso
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='url_queue' AND column_name='retry_count'
    ) THEN
        ALTER TABLE url_queue ADD COLUMN retry_count INTEGER DEFAULT 0;
        RAISE NOTICE 'url_queue.retry_count: AÑADIDA';
    ELSE
        RAISE NOTICE 'url_queue.retry_count: ya existe';
    END IF;

    -- last_error: último mensaje de error
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='url_queue' AND column_name='last_error'
    ) THEN
        ALTER TABLE url_queue ADD COLUMN last_error TEXT;
        RAISE NOTICE 'url_queue.last_error: AÑADIDA';
    ELSE
        RAISE NOTICE 'url_queue.last_error: ya existe';
    END IF;

END $$;


-- ── hotels: columnas faltantes ───────────────────────────────

DO $$
BEGIN
    -- url_id (FK a url_queue)
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='hotels' AND column_name='url_id'
    ) THEN
        -- Si existía como url_queue_id, renombrar
        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='hotels' AND column_name='url_queue_id'
        ) THEN
            ALTER TABLE hotels RENAME COLUMN url_queue_id TO url_id;
            RAISE NOTICE 'hotels.url_queue_id → url_id: RENOMBRADA';
        ELSE
            ALTER TABLE hotels ADD COLUMN url_id INTEGER REFERENCES url_queue(id);
            RAISE NOTICE 'hotels.url_id: AÑADIDA';
        END IF;
    ELSE
        RAISE NOTICE 'hotels.url_id: ya existe';
    END IF;

    -- url: dirección directa del hotel
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='hotels' AND column_name='url'
    ) THEN
        ALTER TABLE hotels ADD COLUMN url VARCHAR(512);
        RAISE NOTICE 'hotels.url: AÑADIDA';
    ELSE
        RAISE NOTICE 'hotels.url: ya existe';
    END IF;

    -- language
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='hotels' AND column_name='language'
    ) THEN
        ALTER TABLE hotels ADD COLUMN language VARCHAR(10) DEFAULT 'en';
        RAISE NOTICE 'hotels.language: AÑADIDA';
    ELSE
        RAISE NOTICE 'hotels.language: ya existe';
    END IF;

    -- rating_category (Excepcional, Fabuloso, etc.)
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='hotels' AND column_name='rating_category'
    ) THEN
        ALTER TABLE hotels ADD COLUMN rating_category VARCHAR(100);
        RAISE NOTICE 'hotels.rating_category: AÑADIDA';
    ELSE
        RAISE NOTICE 'hotels.rating_category: ya existe';
    END IF;

    -- total_reviews (era review_count en versiones anteriores)
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='hotels' AND column_name='total_reviews'
    ) THEN
        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='hotels' AND column_name='review_count'
        ) THEN
            ALTER TABLE hotels RENAME COLUMN review_count TO total_reviews;
            RAISE NOTICE 'hotels.review_count → total_reviews: RENOMBRADA';
        ELSE
            ALTER TABLE hotels ADD COLUMN total_reviews INTEGER;
            RAISE NOTICE 'hotels.total_reviews: AÑADIDA';
        END IF;
    ELSE
        RAISE NOTICE 'hotels.total_reviews: ya existe';
    END IF;

    -- rating (era rating_score en versiones anteriores)
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='hotels' AND column_name='rating'
    ) THEN
        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='hotels' AND column_name='rating_score'
        ) THEN
            ALTER TABLE hotels RENAME COLUMN rating_score TO rating;
            RAISE NOTICE 'hotels.rating_score → rating: RENOMBRADA';
        ELSE
            ALTER TABLE hotels ADD COLUMN rating FLOAT;
            RAISE NOTICE 'hotels.rating: AÑADIDA';
        END IF;
    ELSE
        RAISE NOTICE 'hotels.rating: ya existe';
    END IF;

    -- review_scores (JSON con puntuaciones por categoría)
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='hotels' AND column_name='review_scores'
    ) THEN
        ALTER TABLE hotels ADD COLUMN review_scores JSONB DEFAULT '{}';
        RAISE NOTICE 'hotels.review_scores: AÑADIDA';
    ELSE
        RAISE NOTICE 'hotels.review_scores: ya existe';
    END IF;

    -- services
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='hotels' AND column_name='services'
    ) THEN
        ALTER TABLE hotels ADD COLUMN services JSONB DEFAULT '[]';
        RAISE NOTICE 'hotels.services: AÑADIDA';
    ELSE
        RAISE NOTICE 'hotels.services: ya existe';
    END IF;

    -- facilities
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='hotels' AND column_name='facilities'
    ) THEN
        ALTER TABLE hotels ADD COLUMN facilities JSONB DEFAULT '{}';
        RAISE NOTICE 'hotels.facilities: AÑADIDA';
    ELSE
        RAISE NOTICE 'hotels.facilities: ya existe';
    END IF;

    -- house_rules
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='hotels' AND column_name='house_rules'
    ) THEN
        ALTER TABLE hotels ADD COLUMN house_rules TEXT;
        RAISE NOTICE 'hotels.house_rules: AÑADIDA';
    ELSE
        RAISE NOTICE 'hotels.house_rules: ya existe';
    END IF;

    -- important_info
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='hotels' AND column_name='important_info'
    ) THEN
        ALTER TABLE hotels ADD COLUMN important_info TEXT;
        RAISE NOTICE 'hotels.important_info: AÑADIDA';
    ELSE
        RAISE NOTICE 'hotels.important_info: ya existe';
    END IF;

    -- rooms_info
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='hotels' AND column_name='rooms_info'
    ) THEN
        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='hotels' AND column_name='room_types'
        ) THEN
            ALTER TABLE hotels RENAME COLUMN room_types TO rooms_info;
            RAISE NOTICE 'hotels.room_types → rooms_info: RENOMBRADA';
        ELSE
            ALTER TABLE hotels ADD COLUMN rooms_info JSONB DEFAULT '[]';
            RAISE NOTICE 'hotels.rooms_info: AÑADIDA';
        END IF;
    ELSE
        RAISE NOTICE 'hotels.rooms_info: ya existe';
    END IF;

    -- images_urls
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='hotels' AND column_name='images_urls'
    ) THEN
        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='hotels' AND column_name='images'
        ) THEN
            ALTER TABLE hotels RENAME COLUMN images TO images_urls;
            RAISE NOTICE 'hotels.images → images_urls: RENOMBRADA';
        ELSE
            ALTER TABLE hotels ADD COLUMN images_urls JSONB DEFAULT '[]';
            RAISE NOTICE 'hotels.images_urls: AÑADIDA';
        END IF;
    ELSE
        RAISE NOTICE 'hotels.images_urls: ya existe';
    END IF;

    -- images_count
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='hotels' AND column_name='images_count'
    ) THEN
        ALTER TABLE hotels ADD COLUMN images_count INTEGER DEFAULT 0;
        RAISE NOTICE 'hotels.images_count: AÑADIDA';
    ELSE
        RAISE NOTICE 'hotels.images_count: ya existe';
    END IF;

    -- scraped_at
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='hotels' AND column_name='scraped_at'
    ) THEN
        ALTER TABLE hotels ADD COLUMN scraped_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW();
        RAISE NOTICE 'hotels.scraped_at: AÑADIDA';
    ELSE
        RAISE NOTICE 'hotels.scraped_at: ya existe';
    END IF;

END $$;


-- ── Índice único (url_id, language) para el ON CONFLICT ──────

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE tablename = 'hotels'
          AND indexname = 'ix_hotels_url_language'
    ) THEN
        CREATE UNIQUE INDEX ix_hotels_url_language ON hotels (url_id, language);
        RAISE NOTICE 'Índice ix_hotels_url_language: CREADO';
    ELSE
        RAISE NOTICE 'Índice ix_hotels_url_language: ya existe';
    END IF;
END $$;


-- ── scraping_logs: columnas faltantes ────────────────────────

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='scraping_logs' AND column_name='url_id'
    ) THEN
        ALTER TABLE scraping_logs ADD COLUMN url_id INTEGER REFERENCES url_queue(id);
        RAISE NOTICE 'scraping_logs.url_id: AÑADIDA';
    ELSE
        RAISE NOTICE 'scraping_logs.url_id: ya existe';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='scraping_logs' AND column_name='language'
    ) THEN
        ALTER TABLE scraping_logs ADD COLUMN language VARCHAR(10);
        RAISE NOTICE 'scraping_logs.language: AÑADIDA';
    ELSE
        RAISE NOTICE 'scraping_logs.language: ya existe';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='scraping_logs' AND column_name='duration_seconds'
    ) THEN
        ALTER TABLE scraping_logs ADD COLUMN duration_seconds FLOAT;
        RAISE NOTICE 'scraping_logs.duration_seconds: AÑADIDA';
    ELSE
        RAISE NOTICE 'scraping_logs.duration_seconds: ya existe';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='scraping_logs' AND column_name='items_extracted'
    ) THEN
        ALTER TABLE scraping_logs ADD COLUMN items_extracted INTEGER DEFAULT 0;
        RAISE NOTICE 'scraping_logs.items_extracted: AÑADIDA';
    ELSE
        RAISE NOTICE 'scraping_logs.items_extracted: ya existe';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='scraping_logs' AND column_name='error_message'
    ) THEN
        ALTER TABLE scraping_logs ADD COLUMN error_message TEXT;
        RAISE NOTICE 'scraping_logs.error_message: AÑADIDA';
    ELSE
        RAISE NOTICE 'scraping_logs.error_message: ya existe';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='scraping_logs' AND column_name='timestamp'
    ) THEN
        ALTER TABLE scraping_logs ADD COLUMN timestamp TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW();
        RAISE NOTICE 'scraping_logs.timestamp: AÑADIDA';
    ELSE
        RAISE NOTICE 'scraping_logs.timestamp: ya existe';
    END IF;

END $$;


-- ── Verificación final ────────────────────────────────────────

SELECT
    '✅ url_queue' AS tabla,
    COUNT(*) AS filas,
    string_agg(column_name, ', ' ORDER BY ordinal_position) AS columnas
FROM information_schema.columns
WHERE table_name = 'url_queue'

UNION ALL

SELECT
    '✅ hotels',
    COUNT(*),
    string_agg(column_name, ', ' ORDER BY ordinal_position)
FROM information_schema.columns
WHERE table_name = 'hotels'

UNION ALL

SELECT
    '✅ scraping_logs',
    COUNT(*),
    string_agg(column_name, ', ' ORDER BY ordinal_position)
FROM information_schema.columns
WHERE table_name = 'scraping_logs';
