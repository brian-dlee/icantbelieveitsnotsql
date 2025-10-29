-- PostgreSQL-specific features
--
-- NO SUPPORT
-- UNLOGGED TABLE
-- EXCLUDE USING table constraint
-- GIST indexes

-- SERIAL types
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE big_table (
    id BIGSERIAL PRIMARY KEY,
    small_id SMALLSERIAL NOT NULL,
    data TEXT
);

-- Schema qualified names
CREATE TABLE public.customers (
    customer_id SERIAL PRIMARY KEY,
    email VARCHAR(255)
);

CREATE TABLE app.orders (
    order_id BIGSERIAL PRIMARY KEY,
    customer_id INTEGER,
    FOREIGN KEY (customer_id) REFERENCES public.customers(customer_id)
);

-- JSON types
CREATE TABLE api_logs (
    log_id BIGSERIAL PRIMARY KEY,
    request_data JSON,
    response_data JSONB,
    metadata JSONB DEFAULT '{}'::jsonb
);

-- Array types
CREATE TABLE posts (
    post_id SERIAL PRIMARY KEY,
    tags TEXT[],
    ratings INTEGER[],
    metadata VARCHAR(50)[]
);

-- UUID type
CREATE TABLE sessions (
    session_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id INTEGER NOT NULL,
    data JSONB
);

-- BYTEA for binary data
CREATE TABLE files (
    file_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    filename VARCHAR(255),
    content BYTEA,
    mime_type VARCHAR(100)
);

-- Network address types
CREATE TABLE network_data (
    id SERIAL PRIMARY KEY,
    ip_address INET,
    mac_address MACADDR,
    cidr_block CIDR
);

-- Geometric types
CREATE TABLE spatial_data (
    id SERIAL PRIMARY KEY,
    location POINT,
    bounding_box BOX,
    line_segment LINE,
    circle_area CIRCLE,
    polygon_shape POLYGON
);

-- Text search types
CREATE TABLE documents (
    doc_id SERIAL PRIMARY KEY,
    title TEXT,
    body TEXT,
    search_vector TSVECTOR
);

-- XML type
CREATE TABLE xml_data (
    id SERIAL PRIMARY KEY,
    xml_content XML
);

-- Generated columns
CREATE TABLE computed_values (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    full_name TEXT GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED,
    width NUMERIC(10, 2),
    height NUMERIC(10, 2),
    area NUMERIC(10, 4) GENERATED ALWAYS AS (width * height) STORED
);

-- TIMESTAMP WITH/WITHOUT TIME ZONE
CREATE TABLE temporal_data (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITHOUT TIME ZONE,
    event_date DATE,
    event_time TIME,
    event_time_tz TIME WITH TIME ZONE
);

-- Partial indexes (with WHERE clause)
CREATE TABLE active_users (
    user_id SERIAL PRIMARY KEY,
    email VARCHAR(255),
    is_active BOOLEAN DEFAULT TRUE,
    deleted_at TIMESTAMP
);

CREATE INDEX idx_active_users ON active_users(email) WHERE is_active = TRUE;
CREATE INDEX idx_not_deleted ON active_users(user_id) WHERE deleted_at IS NULL;

-- Expression indexes
CREATE INDEX idx_lower_email ON users(LOWER(email));
CREATE INDEX idx_year_created ON api_logs(EXTRACT(YEAR FROM created_at));

-- MONEY type
CREATE TABLE financial_data (
    id SERIAL PRIMARY KEY,
    amount MONEY,
    description TEXT
);

-- Range types
CREATE TABLE reservations (
    reservation_id SERIAL PRIMARY KEY,
    room_id INTEGER,
    date_range DATERANGE,
    time_range TSRANGE
);

-- Composite types (user-defined types - showing usage)
CREATE TABLE addresses (
    address_id SERIAL PRIMARY KEY,
    street VARCHAR(255),
    city VARCHAR(100),
    state CHAR(2),
    zip VARCHAR(10)
);

-- EXCLUSION constraints
CREATE TABLE bookings (
    booking_id SERIAL PRIMARY KEY,
    resource_id INTEGER,
    during TSRANGE
);

-- Default with function calls
CREATE TABLE uuid_table (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at TIMESTAMP DEFAULT clock_timestamp(),
    random_val FLOAT DEFAULT random()
);

-- CHECK constraint with subquery (PostgreSQL allows this)
CREATE TABLE validated_data (
    id SERIAL PRIMARY KEY,
    category_id INTEGER,
    value NUMERIC(10, 2) CHECK (value >= 0)
);

-- Multiple schemas
CREATE TABLE warehouse.staging.imports (
    import_id SERIAL PRIMARY KEY,
    data JSONB,
    imported_at TIMESTAMP DEFAULT NOW()
);

-- INHERITS (table inheritance)
CREATE TABLE base_entity (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE derived_entity (
    extra_field TEXT
) INHERITS (base_entity);

-- GIN for JSONB
CREATE TABLE json_store (
    id SERIAL PRIMARY KEY,
    data JSONB
);

CREATE INDEX idx_gin_data ON json_store USING GIN (data);
