CREATE TABLE public.accounts (
    id BIGSERIAL PRIMARY KEY,
    external_id UUID NOT NULL,
    email TEXT NOT NULL,
    profile JSONB,
    created_at TIMESTAMPTZ NOT NULL
);
