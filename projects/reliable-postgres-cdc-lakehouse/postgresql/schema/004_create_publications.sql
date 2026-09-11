CREATE PUBLICATION cdc_publication 
FOR TABLE orders, payments, refunds
WITH (publish = 'insert, update, delete');