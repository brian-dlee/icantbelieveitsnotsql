-- Standard ANSI SQL features

-- Basic table
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Composite primary key
CREATE TABLE order_items (
    order_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    PRIMARY KEY (order_id, product_id)
);

-- Foreign keys with actions
CREATE TABLE orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    total DECIMAL(15, 2) NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE sessions (
    session_id VARCHAR(36) PRIMARY KEY,
    user_id INTEGER,
    expires_at TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT ON UPDATE NO ACTION
);

-- Various numeric types
CREATE TABLE measurements (
    id INTEGER PRIMARY KEY,
    small_val SMALLINT,
    int_val INTEGER,
    big_val BIGINT,
    decimal_val DECIMAL(10, 2),
    numeric_val NUMERIC(15, 4),
    real_val REAL,
    double_val DOUBLE PRECISION,
    float_val FLOAT
);

-- String types
CREATE TABLE text_types (
    id INTEGER PRIMARY KEY,
    fixed_char CHAR(10),
    var_string VARCHAR(255),
    unlimited_text TEXT
);

-- Date and time types
CREATE TABLE temporal_data (
    id INTEGER PRIMARY KEY,
    event_date DATE,
    event_time TIME,
    event_datetime TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Boolean types
CREATE TABLE flags (
    id INTEGER PRIMARY KEY,
    is_active BOOLEAN DEFAULT TRUE,
    is_deleted BOOLEAN DEFAULT FALSE
);

-- Inline constraints
CREATE TABLE constrained_columns (
    id INTEGER PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    age INTEGER NOT NULL CHECK (age >= 18),
    balance DECIMAL(10, 2) DEFAULT 0.00 CHECK (balance >= 0)
);

-- Table-level constraints
CREATE TABLE named_constraints (
    customer_id INTEGER,
    email VARCHAR(255),
    backup_email VARCHAR(255),
    account_number VARCHAR(20),
    CONSTRAINT pk_customer PRIMARY KEY (customer_id),
    CONSTRAINT uk_email UNIQUE (email),
    CONSTRAINT uk_account UNIQUE (account_number),
    CONSTRAINT chk_different_emails CHECK (email != backup_email)
);

-- Multiple foreign keys
CREATE TABLE transactions (
    transaction_id INTEGER PRIMARY KEY,
    from_account INTEGER NOT NULL,
    to_account INTEGER NOT NULL,
    amount DECIMAL(15, 2) NOT NULL,
    FOREIGN KEY (from_account) REFERENCES accounts(id) ON DELETE RESTRICT,
    FOREIGN KEY (to_account) REFERENCES accounts(id) ON DELETE RESTRICT
);

-- Self-referencing foreign key
CREATE TABLE employees (
    employee_id INTEGER PRIMARY KEY,
    manager_id INTEGER,
    name VARCHAR(100),
    FOREIGN KEY (manager_id) REFERENCES employees(employee_id)
);

-- NULL and NOT NULL variations
CREATE TABLE nullability (
    id INTEGER PRIMARY KEY NOT NULL,
    required_field VARCHAR(50) NOT NULL,
    optional_field VARCHAR(50),
    nullable_explicit TEXT NULL,
    with_default VARCHAR(50) DEFAULT 'default_value'
);

-- Separate index statements
CREATE INDEX idx_users_email ON users(email);
CREATE UNIQUE INDEX idx_customers_account ON customers(account_number);
CREATE INDEX idx_orders_composite ON orders(customer_id, status);

-- IF NOT EXISTS
CREATE TABLE IF NOT EXISTS idempotent_table (
    id INTEGER PRIMARY KEY,
    data VARCHAR(255)
);

-- TEMPORARY tables
CREATE TEMPORARY TABLE temp_processing (
    batch_id INTEGER,
    record_data TEXT
);
