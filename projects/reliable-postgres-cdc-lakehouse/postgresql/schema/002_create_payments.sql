CREATE TABLE payments (
    id_payment UUID PRIMARY KEY,
    id_order UUID NOT NULL REFERENCES orders(id_order),
    status_payment TEXT NOT NULL CHECK (status_payment IN ('PENDING', 'AUTHORIZED', 'CAPTURED', 'DECLINED', 'CANCELLED')), 
    amount DECIMAL(10,2) NOT NULL CHECK (amount > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL
);