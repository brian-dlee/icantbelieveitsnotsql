CREATE TABLE accounts (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    profile JSON,
    status ENUM('active', 'disabled') NOT NULL,
    balance DECIMAL(10, 2) NOT NULL,
    created_at TIMESTAMP NOT NULL
);
