CREATE TABLE widgets (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    meta JSONB
);

-- A FOR UPDATE that is not a locking clause, in an earlier statement.
CREATE POLICY widget_editors ON widgets FOR UPDATE USING (true);

-- PostgreSQL accepts the locking clause before LIMIT; sqlparser needs a retry.
CREATE VIEW next_widget AS
SELECT id, name FROM widgets
ORDER BY id
FOR UPDATE SKIP LOCKED
LIMIT 1; -- one at a time
