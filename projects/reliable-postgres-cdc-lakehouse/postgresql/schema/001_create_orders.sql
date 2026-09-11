CREATE TABLE orders (
    id_order UUID PRIMARY KEY,
    id_client UUID NOT NULL,
    status_order TEXT NOT NULL CHECK (status_order IN ('CREATED', 'CONFIRMED', 'SHIPPED', 'DELIVERED', 'CANCELLED')),
    amount DECIMAL(10,2) NOT NULL CHECK (amount >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL
);