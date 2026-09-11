CREATE TABLE refunds (
    id_refund UUID PRIMARY KEY,
    id_payment UUID NOT NULL REFERENCES payments(id_payment),
    status_refund TEXT NOT NULL CHECK (status_refund IN ('PENDING','APPROVED','COMPLETED', 'FAILED', 'CANCELLED')),
    amount DECIMAL(10,2) NOT NULL CHECK (amount > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL
);