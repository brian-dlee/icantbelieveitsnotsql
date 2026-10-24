CREATE TABLE items (
    id BIGSERIAL PRIMARY KEY,
    owner TEXT NOT NULL,
    label TEXT,
    meta JSONB,
    seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
