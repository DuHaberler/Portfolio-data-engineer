BEGIN;

INSERT INTO orders (id_order, id_client, status_order, amount, created_at, updated_at)
VALUES 
('9f1c2a4e-7d8b-4f3a-a6c1-2b9e5d7f1034', '7b4a1e96-2c5f-4d83-8a71-f9e0c6b2d345', 'CONFIRMED', 10.00, DEFAULT, NOW()), 
('2a7e91c5-6b4d-4c8f-9a32-1d5e7b0f6c44', 'e5f2a7c9-1b3d-4e68-a940-6c7d8f2b1530', 'CREATED', 20.00, DEFAULT, NOW()), 
('c3d8f214-9a6b-45e1-b7c2-8f0d3a5e9146', '4c9d7e21-8a5b-4f36-b102-d6e3c7a91584', 'DELIVERED', 30.00, DEFAULT, NOW());

INSERT INTO payments(id_payment, id_order, status_payment, amount, created_at, updated_at)
VALUES 
('a1b8c5d3-7e2f-4690-9c41-5d8a6b3e7201', '9f1c2a4e-7d8b-4f3a-a6c1-2b9e5d7f1034', 'CAPTURED', 10.00, DEFAULT, NOW()),
('d6e3a9f8-4c1b-47d5-b620-3a8f9e2c7154', '2a7e91c5-6b4d-4c8f-9a32-1d5e7b0f6c44', 'DECLINED', 20.00, DEFAULT, NOW()),
('5a2f8c71-d9e4-43b6-8710-c3d5a9f2e648', 'c3d8f214-9a6b-45e1-b7c2-8f0d3a5e9146', 'CAPTURED', 30.00, DEFAULT, NOW());

INSERT INTO refunds(id_refund, id_payment, status_refund, amount, created_at, updated_at)
VALUES 
('8d7c4b2a-1e6f-4953-a8b0-f2c7d9e31465', '5a2f8c71-d9e4-43b6-8710-c3d5a9f2e648', 'COMPLETED', 30.00, DEFAULT, NOW());

COMMIT;