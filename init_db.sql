-- scripts/init_db.sql
-- Script de inicialización de base de datos para Booking Scraper Pro
-- PostgreSQL 15+

-- ============================================================================
-- EXTENSIONES
-- ============================================================================
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";  -- Para búsquedas de texto

-- ============================================================================
-- TABLA: url_queue
-- Cola de URLs a procesar
-- ============================================================================
CREATE TABLE IF NOT EXISTS url_queue (
    id SERIAL PRIMARY KEY,
    url VARCHAR(500) UNIQUE NOT NULL,
    status VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'completed', 'failed', 'skipped')),
    priority INT DEFAULT 0,
    retry_count INT DEFAULT 0,
    max_retries INT DEFAULT 3,
    last_attempt TIMESTAMP,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Índices para optimización
CREATE INDEX IF NOT EXISTS idx_url_status_priority ON url_queue(status, priority DESC);
CREATE INDEX IF NOT EXISTS idx_url_created_at ON url_queue(created_at);
CREATE INDEX IF NOT EXISTS idx_url_retry ON url_queue(retry_count, status);

-- Trigger para actualizar updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_url_queue_updated_at BEFORE UPDATE ON url_queue
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- TABLA: hotels
-- Datos principales de hoteles
-- ============================================================================
CREATE TABLE IF NOT EXISTS hotels (
    id SERIAL PRIMARY KEY,
    booking_url VARCHAR(500) UNIQUE NOT NULL,
    hotel_name VARCHAR(255),
    address TEXT,
    description TEXT,
    rating DECIMAL(3,2),
    total_reviews INT DEFAULT 0,
    language VARCHAR(5) DEFAULT 'en',
    country VARCHAR(100),
    city VARCHAR(100),
    
    -- Datos estructurados en JSON
    raw_data JSONB,
    review_scores JSONB,  -- Puntuaciones por categoría
    
    -- Metadata
    images_downloaded BOOLEAN DEFAULT FALSE,
    images_count INT DEFAULT 0,
    extraction_source VARCHAR(50),
    vpn_ip VARCHAR(50),
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraint
    CONSTRAINT positive_rating CHECK (rating >= 0 AND rating <= 10),
    CONSTRAINT positive_reviews CHECK (total_reviews >= 0)
);

-- Índices
CREATE INDEX IF NOT EXISTS idx_hotel_url ON hotels(booking_url);
CREATE INDEX IF NOT EXISTS idx_hotel_name ON hotels(hotel_name);
CREATE INDEX IF NOT EXISTS idx_hotel_rating ON hotels(rating);
CREATE INDEX IF NOT EXISTS idx_hotel_language ON hotels(language);
CREATE INDEX IF NOT EXISTS idx_hotel_city ON hotels(city);
CREATE INDEX IF NOT EXISTS idx_hotel_created ON hotels(created_at);

-- Índice para búsqueda de texto completo
CREATE INDEX IF NOT EXISTS idx_hotel_name_trgm ON hotels USING gin (hotel_name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_hotel_description_trgm ON hotels USING gin (description gin_trgm_ops);

-- Trigger
CREATE TRIGGER update_hotels_updated_at BEFORE UPDATE ON hotels
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- TABLA: hotel_facilities
-- Servicios e instalaciones de los hoteles
-- ============================================================================
CREATE TABLE IF NOT EXISTS hotel_facilities (
    id SERIAL PRIMARY KEY,
    hotel_id INT NOT NULL REFERENCES hotels(id) ON DELETE CASCADE,
    facility_name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Evitar duplicados
    UNIQUE(hotel_id, facility_name)
);

-- Índices
CREATE INDEX IF NOT EXISTS idx_facility_hotel ON hotel_facilities(hotel_id);
CREATE INDEX IF NOT EXISTS idx_facility_name ON hotel_facilities(facility_name);
CREATE INDEX IF NOT EXISTS idx_facility_category ON hotel_facilities(category);

-- ============================================================================
-- TABLA: rooms
-- Tipos de habitaciones
-- ============================================================================
CREATE TABLE IF NOT EXISTS rooms (
    id SERIAL PRIMARY KEY,
    hotel_id INT NOT NULL REFERENCES hotels(id) ON DELETE CASCADE,
    room_type VARCHAR(255) NOT NULL,
    room_description TEXT,
    room_facilities TEXT[],  -- Array de servicios de la habitación
    max_occupancy INT,
    price_info JSONB,  -- Información de precios si está disponible
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Índices
CREATE INDEX IF NOT EXISTS idx_room_hotel ON rooms(hotel_id);
CREATE INDEX IF NOT EXISTS idx_room_type ON rooms(room_type);

-- ============================================================================
-- TABLA: images
-- Imágenes de hoteles
-- ============================================================================
CREATE TABLE IF NOT EXISTS images (
    id SERIAL PRIMARY KEY,
    hotel_id INT NOT NULL REFERENCES hotels(id) ON DELETE CASCADE,
    room_id INT REFERENCES rooms(id) ON DELETE CASCADE,  -- NULL si es imagen del hotel
    
    image_url VARCHAR(1000) NOT NULL,
    local_path VARCHAR(500),
    image_type VARCHAR(50) CHECK (image_type IN ('hotel', 'room', 'facility', 'other')),
    
    file_size_bytes BIGINT,
    width INT,
    height INT,
    format VARCHAR(20),
    
    downloaded BOOLEAN DEFAULT FALSE,
    download_attempts INT DEFAULT 0,
    download_error TEXT,
    download_date TIMESTAMP,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Evitar duplicados
    UNIQUE(hotel_id, image_url)
);

-- Índices
CREATE INDEX IF NOT EXISTS idx_image_hotel ON images(hotel_id);
CREATE INDEX IF NOT EXISTS idx_image_room ON images(room_id);
CREATE INDEX IF NOT EXISTS idx_image_downloaded ON images(downloaded);
CREATE INDEX IF NOT EXISTS idx_image_type ON images(image_type);

-- ============================================================================
-- TABLA: scraping_logs
-- Logs de operaciones de scraping
-- ============================================================================
CREATE TABLE IF NOT EXISTS scraping_logs (
    id SERIAL PRIMARY KEY,
    url VARCHAR(500) NOT NULL,
    hotel_id INT REFERENCES hotels(id) ON DELETE SET NULL,
    
    status VARCHAR(50) NOT NULL,  -- success, failed, timeout, error
    error_message TEXT,
    error_type VARCHAR(100),
    
    duration_seconds INT,
    vpn_ip VARCHAR(50),
    vpn_country VARCHAR(10),
    user_agent TEXT,
    
    data_extracted JSONB,  -- Resumen de datos extraídos
    
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Índices
CREATE INDEX IF NOT EXISTS idx_log_timestamp ON scraping_logs(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_log_status ON scraping_logs(status);
CREATE INDEX IF NOT EXISTS idx_log_url ON scraping_logs(url);
CREATE INDEX IF NOT EXISTS idx_log_hotel ON scraping_logs(hotel_id);

-- Particionamiento por fecha (opcional, para mejor performance con muchos logs)
-- CREATE INDEX IF NOT EXISTS idx_log_date ON scraping_logs(DATE(timestamp));

-- ============================================================================
-- TABLA: policies
-- Políticas y normas de los hoteles
-- ============================================================================
CREATE TABLE IF NOT EXISTS policies (
    id SERIAL PRIMARY KEY,
    hotel_id INT NOT NULL REFERENCES hotels(id) ON DELETE CASCADE,
    
    policy_type VARCHAR(50),  -- checkin, checkout, cancellation, payment, pets, children, etc.
    policy_text TEXT NOT NULL,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(hotel_id, policy_type)
);

-- Índice
CREATE INDEX IF NOT EXISTS idx_policy_hotel ON policies(hotel_id);
CREATE INDEX IF NOT EXISTS idx_policy_type ON policies(policy_type);

-- ============================================================================
-- TABLA: statistics
-- Estadísticas generales del sistema
-- ============================================================================
CREATE TABLE IF NOT EXISTS statistics (
    id SERIAL PRIMARY KEY,
    stat_date DATE DEFAULT CURRENT_DATE,
    
    total_urls INT DEFAULT 0,
    urls_pending INT DEFAULT 0,
    urls_processing INT DEFAULT 0,
    urls_completed INT DEFAULT 0,
    urls_failed INT DEFAULT 0,
    
    hotels_scraped INT DEFAULT 0,
    images_downloaded INT DEFAULT 0,
    
    avg_extraction_time_seconds DECIMAL(10,2),
    success_rate DECIMAL(5,2),
    
    vpn_rotations INT DEFAULT 0,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(stat_date)
);

-- ============================================================================
-- VISTAS
-- ============================================================================

-- Vista de estado de la cola
CREATE OR REPLACE VIEW v_queue_status AS
SELECT 
    status,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM url_queue
GROUP BY status;

-- Vista de hoteles con estadísticas
CREATE OR REPLACE VIEW v_hotels_summary AS
SELECT 
    h.id,
    h.hotel_name,
    h.city,
    h.country,
    h.rating,
    h.total_reviews,
    h.language,
    COUNT(DISTINCT f.id) as facilities_count,
    COUNT(DISTINCT r.id) as rooms_count,
    COUNT(DISTINCT i.id) as images_count,
    SUM(CASE WHEN i.downloaded THEN 1 ELSE 0 END) as images_downloaded_count,
    h.created_at
FROM hotels h
LEFT JOIN hotel_facilities f ON h.id = f.hotel_id
LEFT JOIN rooms r ON h.id = r.hotel_id
LEFT JOIN images i ON h.id = i.hotel_id
GROUP BY h.id;

-- Vista de estadísticas por día
CREATE OR REPLACE VIEW v_daily_stats AS
SELECT 
    DATE(created_at) as date,
    COUNT(*) as hotels_scraped,
    AVG(rating) as avg_rating,
    COUNT(CASE WHEN images_downloaded THEN 1 END) as with_images
FROM hotels
GROUP BY DATE(created_at)
ORDER BY date DESC;

-- ============================================================================
-- FUNCIONES ÚTILES
-- ============================================================================

-- Función para obtener URLs pendientes prioritarias
CREATE OR REPLACE FUNCTION get_pending_urls(limit_count INT DEFAULT 10)
RETURNS TABLE (
    id INT,
    url VARCHAR,
    priority INT,
    retry_count INT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        uq.id,
        uq.url,
        uq.priority,
        uq.retry_count
    FROM url_queue uq
    WHERE uq.status = 'pending'
        AND uq.retry_count < uq.max_retries
    ORDER BY uq.priority DESC, uq.created_at ASC
    LIMIT limit_count;
END;
$$ LANGUAGE plpgsql;

-- Función para marcar URL como procesando
CREATE OR REPLACE FUNCTION mark_url_processing(url_id INT)
RETURNS VOID AS $$
BEGIN
    UPDATE url_queue
    SET status = 'processing',
        last_attempt = CURRENT_TIMESTAMP
    WHERE id = url_id;
END;
$$ LANGUAGE plpgsql;

-- Función para marcar URL como completada
CREATE OR REPLACE FUNCTION mark_url_completed(url_id INT)
RETURNS VOID AS $$
BEGIN
    UPDATE url_queue
    SET status = 'completed',
        updated_at = CURRENT_TIMESTAMP
    WHERE id = url_id;
END;
$$ LANGUAGE plpgsql;

-- Función para marcar URL como fallida
CREATE OR REPLACE FUNCTION mark_url_failed(url_id INT, error_msg TEXT DEFAULT NULL)
RETURNS VOID AS $$
BEGIN
    UPDATE url_queue
    SET status = CASE 
            WHEN retry_count + 1 >= max_retries THEN 'failed'
            ELSE 'pending'
        END,
        retry_count = retry_count + 1,
        error_message = error_msg,
        updated_at = CURRENT_TIMESTAMP
    WHERE id = url_id;
END;
$$ LANGUAGE plpgsql;

-- Función para obtener estadísticas generales
CREATE OR REPLACE FUNCTION get_system_stats()
RETURNS TABLE (
    total_urls BIGINT,
    pending BIGINT,
    processing BIGINT,
    completed BIGINT,
    failed BIGINT,
    total_hotels BIGINT,
    total_images BIGINT,
    images_downloaded BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        COUNT(*) as total_urls,
        COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending,
        COUNT(CASE WHEN status = 'processing' THEN 1 END) as processing,
        COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed,
        COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed,
        (SELECT COUNT(*) FROM hotels) as total_hotels,
        (SELECT COUNT(*) FROM images) as total_images,
        (SELECT COUNT(*) FROM images WHERE downloaded = TRUE) as images_downloaded
    FROM url_queue;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- DATOS INICIALES
-- ============================================================================

-- Insertar categorías de servicios comunes
CREATE TABLE IF NOT EXISTS facility_categories (
    id SERIAL PRIMARY KEY,
    category_name VARCHAR(100) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO facility_categories (category_name) VALUES
    ('General'),
    ('Internet'),
    ('Parking'),
    ('Food & Drink'),
    ('Pool & Wellness'),
    ('Entertainment'),
    ('Business'),
    ('Family'),
    ('Accessibility'),
    ('Safety & Security')
ON CONFLICT (category_name) DO NOTHING;

-- ============================================================================
-- COMENTARIOS
-- ============================================================================

COMMENT ON TABLE url_queue IS 'Cola de URLs a procesar con sistema de reintentos';
COMMENT ON TABLE hotels IS 'Datos principales de hoteles extraídos de Booking.com';
COMMENT ON TABLE hotel_facilities IS 'Servicios e instalaciones de los hoteles';
COMMENT ON TABLE rooms IS 'Tipos de habitaciones disponibles en cada hotel';
COMMENT ON TABLE images IS 'Imágenes de hoteles y habitaciones';
COMMENT ON TABLE scraping_logs IS 'Logs detallados de operaciones de scraping';
COMMENT ON TABLE policies IS 'Políticas y normas de los hoteles';
COMMENT ON TABLE statistics IS 'Estadísticas agregadas del sistema';

-- ============================================================================
-- PERMISOS (ajustar según necesidades)
-- ============================================================================

-- Crear usuario para la aplicación (opcional)
-- CREATE USER booking_scraper_app WITH PASSWORD 'your_secure_password_here';
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO booking_scraper_app;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO booking_scraper_app;

-- ============================================================================
-- FINALIZACIÓN
-- ============================================================================

-- Verificar creación de tablas
SELECT 
    schemaname,
    tablename,
    tableowner
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY tablename;

-- Verificar vistas
SELECT 
    schemaname,
    viewname
FROM pg_views
WHERE schemaname = 'public'
ORDER BY viewname;

VACUUM ANALYZE;

-- FIN DEL SCRIPT
