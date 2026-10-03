CREATE TABLE account (
    id INTEGER PRIMARY KEY,
    display_name TEXT NOT NULL
);

CREATE TABLE invoice (
    id INTEGER PRIMARY KEY,
    account_id INTEGER NOT NULL,
    amount NUMERIC NOT NULL,
    memo TEXT
);
