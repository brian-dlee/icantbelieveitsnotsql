-- MySQL-specific features
--
-- NO SUPPORT
-- ZEROFILL COLUMN

-- AUTO_INCREMENT and UNSIGNED
CREATE TABLE users (
    id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
    email VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- MySQL numeric types
CREATE TABLE mysql_numbers (
    id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
    tiny TINYINT,
    medium MEDIUMINT,
    big BIGINT UNSIGNED,
    unsigned_decimal DECIMAL(10, 2) UNSIGNED
);

-- MySQL text types
CREATE TABLE text_types (
    id INT PRIMARY KEY AUTO_INCREMENT,
    tiny_text TINYTEXT,
    medium_text MEDIUMTEXT,
    long_text LONGTEXT,
    regular_text TEXT
);

-- MySQL binary types
CREATE TABLE binary_types (
    id INT PRIMARY KEY AUTO_INCREMENT,
    tiny_blob TINYBLOB,
    medium_blob MEDIUMBLOB,
    long_blob LONGBLOB,
    regular_blob BLOB,
    var_binary VARBINARY(255)
);

-- ENUM and SET types
CREATE TABLE status_table (
    id INT PRIMARY KEY AUTO_INCREMENT,
    status ENUM('active', 'inactive', 'pending', 'suspended') NOT NULL DEFAULT 'pending',
    permissions SET('read', 'write', 'execute', 'admin')
);

-- Inline INDEX definitions
CREATE TABLE products (
    product_id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
    sku VARCHAR(50) NOT NULL,
    name VARCHAR(255) NOT NULL,
    price DECIMAL(10, 2) DEFAULT 0.00,
    category_id INT UNSIGNED,
    is_active BOOLEAN DEFAULT TRUE,
    INDEX idx_sku (sku),
    INDEX idx_category (category_id),
    UNIQUE INDEX idx_sku_unique (sku)
);

-- Virtual and stored generated columns
CREATE TABLE rectangles (
    id INT PRIMARY KEY AUTO_INCREMENT,
    width DECIMAL(10, 2),
    height DECIMAL(10, 2),
    area DECIMAL(10, 4) AS (width * height) STORED,
    perimeter DECIMAL(10, 2) AS (2 * (width + height)) VIRTUAL
);

CREATE TABLE orders (
    order_id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
    subtotal DECIMAL(10, 2) NOT NULL,
    tax_rate DECIMAL(4, 2) NOT NULL,
    tax_amount DECIMAL(10, 2) AS (subtotal * tax_rate) STORED,
    total DECIMAL(10, 2) AS (subtotal + (subtotal * tax_rate)) STORED
);

-- CHARACTER SET and COLLATE
CREATE TABLE i18n_table (
    id INT PRIMARY KEY AUTO_INCREMENT,
    utf8_col VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    latin1_col VARCHAR(100) CHARACTER SET latin1,
    binary_col VARCHAR(100) COLLATE utf8mb4_bin
);

-- COMMENT syntax
CREATE TABLE documented (
    id INT PRIMARY KEY AUTO_INCREMENT COMMENT 'Primary identifier',
    description TEXT COMMENT 'Item description',
    price DECIMAL(10, 2) COMMENT 'Price in USD'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='This is a documented table';

-- FULLTEXT indexes
CREATE TABLE articles (
    article_id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
    title VARCHAR(255) NOT NULL,
    body TEXT NOT NULL,
    FULLTEXT INDEX idx_fulltext (title, body)
);

-- ON UPDATE CURRENT_TIMESTAMP
CREATE TABLE audit_trail (
    id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
    record_id INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Database qualified names
CREATE TABLE mydb.users (
    id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
    username VARCHAR(50) NOT NULL
);

CREATE TABLE app.sessions (
    session_id VARCHAR(36) PRIMARY KEY,
    user_id INT UNSIGNED,
    FOREIGN KEY (user_id) REFERENCES app.users(id)
);

-- SPATIAL types and indexes
CREATE TABLE locations (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100),
    coordinates POINT NOT NULL,
    area POLYGON,
    SPATIAL INDEX idx_coordinates (coordinates)
);

-- Year type
CREATE TABLE yearly_data (
    id INT PRIMARY KEY AUTO_INCREMENT,
    data_year YEAR NOT NULL,
    value DECIMAL(15, 2)
);

-- Multiple composite indexes
CREATE TABLE events (
    event_id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
    user_id INT UNSIGNED NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_user_events (user_id, event_type),
    INDEX idx_type_time (event_type, created_at)
);
