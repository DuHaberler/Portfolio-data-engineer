# COMANDOS DOCKER

## docker compose up: 
- sobe o compose e mantém o terminal mostrando os logs da subida

## docker compose up -d: 
- sobe o compose em segundo plano (-d: detached mode)

## docker compose down: 
- remove o compose, sem remover volumes

## docker compose down -v: 
- remove todo o compose, inclusive volumes (-v: volumes)

## docker compose logs: 
- ver os logs de um ambiente iniciado

## docker compose logs -f:
- acompanhar os logs continuamente (-f: follow)



# COMANDOS POSTGRESQL

## docker compose exec postgres psql -U portfolio_user -d portfolio
- executa comandos SQL dentro do container que já está rodando (-U: usuario a ser utilizado | -d: database a ser conectado)

## docker compose exec postgres ls -la /docker-entrypoint-initdb.d
- "Quais arquivos o container realmente está enxergando na pasta de inicialização?"



# COMANDOS KAFKA

## docker compose logs kafka 
- consulta os logs do container gerenciados pelo Docker.

## docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server kafka:9092 
- valida o listener interno e confirma que o broker responde dentro da rede Docker.

## docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --describe --topic portfolio.test --bootstrap-server kafka:9092
- inspeciona o topic, incluindo partições, leader, replicas e ISR.

## docker compose exec kafka /opt/kafka/bin/kafka-console-producer.sh --topic portfolio.test --bootstrap-server kafka:9092
- abre um producer interativo dentro do container para publicar mensagens.

## docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --topic portfolio.test --from-beginning --bootstrap-server kafka:9092
- abre um consumer dentro do container para validar o consumo das mensagens.

### VALIDAÇÕES USER DEBEZIUM

#### valida possibilidade de login e replicação
SELECT
    rolname,
    rolcanlogin,
    rolreplication
FROM pg_roles
WHERE rolname = 'user_debezium';

#### valida grant de connect no banco, usage no database e select nas tabelas
SELECT
    has_database_privilege('user_debezium', 'portfolio', 'CONNECT') AS can_connect,
    has_schema_privilege('user_debezium', 'public', 'USAGE') AS can_use_schema,
    has_table_privilege('user_debezium', 'public.orders', 'SELECT') AS can_read_orders,
    has_table_privilege('user_debezium', 'public.payments', 'SELECT') AS can_read_payments,
    has_table_privilege('user_debezium', 'public.refunds', 'SELECT') AS can_read_refunds;