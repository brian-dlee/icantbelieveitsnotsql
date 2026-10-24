-- Widgets
--
-- A bare `--` line is a comment in MySQL.
CREATE TABLE widgets (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(64) NOT NULL,
    price DOUBLE NOT NULL,
    createdAt DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
);
