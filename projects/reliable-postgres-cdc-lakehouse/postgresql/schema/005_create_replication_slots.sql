SELECT pg_create_logical_replication_slot(
    'cdc_slot',
    'pgoutput'
);