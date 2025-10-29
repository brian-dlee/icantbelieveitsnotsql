-- SQLite-specific features
--
-- NO SUPPORT
-- TEXT(50) - cannot include length

-- AUTOINCREMENT (SQLite specific - different from AUTO_INCREMENT)
CREATE TABLE users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- SQLite type affinity (flexible typing)
CREATE TABLE flexible_types (
    id INTEGER PRIMARY KEY,
    int_col INTEGER,
    text_col TEXT,
    blob_col BLOB,
    real_col REAL,
    numeric_col NUMERIC
);

-- TEXT instead of VARCHAR (SQLite treats them the same)
CREATE TABLE text_table (
    id INTEGER PRIMARY KEY,
    short_text TEXT,
    long_text TEXT
);

-- STRICT tables (SQLite 3.37+)
CREATE TABLE strict_table (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    value REAL
) STRICT;

-- WITHOUT ROWID optimization
CREATE TABLE uuid_keyed (
    uuid TEXT PRIMARY KEY,
    data TEXT
) WITHOUT ROWID;

-- Composite primary key without AUTOINCREMENT
CREATE TABLE order_items (
    order_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    PRIMARY KEY (order_id, product_id)
);

-- Foreign keys (must enable with PRAGMA)
CREATE TABLE orders (
    order_id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id INTEGER NOT NULL,
    status TEXT DEFAULT 'pending',
    FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE
);

-- CHECK constraints
CREATE TABLE accounts (
    account_id INTEGER PRIMARY KEY,
    balance REAL NOT NULL CHECK (balance >= 0),
    account_type TEXT CHECK (account_type IN ('checking', 'savings'))
);

-- Generated columns (SQLite 3.31+)
CREATE TABLE rectangles (
    id INTEGER PRIMARY KEY,
    width REAL,
    height REAL,
    area REAL GENERATED ALWAYS AS (width * height) STORED
);

CREATE TABLE users_with_computed (
    user_id INTEGER PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    full_name TEXT GENERATED ALWAYS AS (first_name || ' ' || last_name) VIRTUAL
);

-- UNIQUE constraints
CREATE TABLE unique_emails (
    user_id INTEGER PRIMARY KEY,
    email TEXT UNIQUE NOT NULL,
    username TEXT NOT NULL,
    UNIQUE (username)
);

-- Multiple constraints
CREATE TABLE constrained (
    id INTEGER PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    age INTEGER CHECK (age >= 18),
    status TEXT DEFAULT 'active' CHECK (status IN ('active', 'inactive'))
);

-- Table-level constraints
CREATE TABLE named_constraints (
    customer_id INTEGER,
    email TEXT,
    account_number TEXT,
    CONSTRAINT pk_customer PRIMARY KEY (customer_id),
    CONSTRAINT uk_email UNIQUE (email),
    CONSTRAINT chk_email CHECK (email LIKE '%@%')
);

-- Partial indexes (SQLite supports WHERE clause)
CREATE TABLE filtered_data (
    id INTEGER PRIMARY KEY,
    value TEXT,
    is_active INTEGER DEFAULT 1,
    deleted_at TEXT
);

CREATE INDEX idx_active ON filtered_data(value) WHERE is_active = 1;
CREATE INDEX idx_not_deleted ON filtered_data(id) WHERE deleted_at IS NULL;

-- Expression indexes
CREATE INDEX idx_lower_email ON users(LOWER(email));
CREATE INDEX idx_text_length ON text_table(LENGTH(short_text));

-- IF NOT EXISTS
CREATE TABLE IF NOT EXISTS idempotent (
    id INTEGER PRIMARY KEY,
    data TEXT
);

-- TEMPORARY tables
CREATE TEMPORARY TABLE temp_data (
    session_id TEXT,
    value REAL
);

-- ON CONFLICT clause (UPSERT)
CREATE TABLE settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
) WITHOUT ROWID;

-- Date/time as TEXT (SQLite's approach)
CREATE TABLE events (
    event_id INTEGER PRIMARY KEY,
    event_date TEXT,
    event_time TEXT,
    event_datetime TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

-- Boolean as INTEGER
CREATE TABLE flags (
    id INTEGER PRIMARY KEY,
    is_active INTEGER DEFAULT 1,
    is_deleted INTEGER DEFAULT 0,
    CHECK (is_active IN (0, 1)),
    CHECK (is_deleted IN (0, 1))
);

-- JSON support (as TEXT, with JSON functions)
CREATE TABLE json_data (
    id INTEGER PRIMARY KEY,
    data TEXT CHECK (json_valid(data))
);

-- Self-referencing foreign key
CREATE TABLE employees (
    employee_id INTEGER PRIMARY KEY,
    manager_id INTEGER,
    name TEXT,
    FOREIGN KEY (manager_id) REFERENCES employees(employee_id)
);

-- Complex default values
CREATE TABLE defaults (
    id INTEGER PRIMARY KEY,
    created_at TEXT DEFAULT (datetime('now', 'localtime')),
    uuid TEXT DEFAULT (lower(hex(randomblob(16)))),
    random_val REAL DEFAULT (abs(random() % 100))
);

-- Multiple foreign keys
CREATE TABLE transactions (
    transaction_id INTEGER PRIMARY KEY,
    from_account INTEGER NOT NULL,
    to_account INTEGER NOT NULL,
    amount REAL NOT NULL CHECK (amount > 0),
    FOREIGN KEY (from_account) REFERENCES accounts(account_id),
    FOREIGN KEY (to_account) REFERENCES accounts(account_id)
);

-- Composite unique constraint
CREATE TABLE user_services (
    user_id INTEGER NOT NULL,
    service_name TEXT NOT NULL,
    api_key TEXT,
    UNIQUE (user_id, service_name)
);
