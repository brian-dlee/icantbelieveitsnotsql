CREATE TABLE public.accounts (
    id BIGSERIAL PRIMARY KEY,
    external_id UUID NOT NULL,
    email TEXT NOT NULL,
    profile JSONB,
    tags TEXT[],
    score_matrix INTEGER[][] NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);
