-- 1. Criar usuário dedicado ao Debezium
CREATE ROLE user_debezium
WITH
    LOGIN
    REPLICATION
    PASSWORD :'debezium_user_password';


-- 2. Permitir conexão no database
GRANT CONNECT
ON DATABASE portfolio
TO user_debezium;


-- 3. Permitir acesso ao schema
GRANT USAGE
ON SCHEMA public
TO user_debezium;


-- 4. Permitir leitura apenas das tabelas capturadas pelo CDC
GRANT SELECT
ON TABLE
    orders,
    payments,
    refunds
TO user_debezium;