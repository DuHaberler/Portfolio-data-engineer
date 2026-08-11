# Estrutura macro:

## M0 — Fundação do projeto
## M1 — CDC funcional
## M2 — Camada histórica
## M3 — Consolidação analítica
## M4 — Orquestração e reprocessamento
## M5 — Confiabilidade e falhas
## M6 — Documentação e apresentação


# M0 — Fundação do projeto

## M0-01 — Criar repositório e estrutura de diretórios.

reliable-postgres-cdc-lakehouse/
│
├── README.md
│   → Visão geral do projeto: problema, objetivo, arquitetura,
│     stack, instruções de execução e principais decisões.
│
├── docker-compose.yml
│   → Define os serviços locais que serão executados em containers,
│     como PostgreSQL, Kafka, Kafka Connect, MinIO e Airflow.
│
├── .env.example
│   → Lista variáveis de ambiente esperadas pelo projeto,
│     sem conter senhas ou segredos reais.
│
├── .gitignore
│   → Define arquivos que não devem ser versionados:
│     .env, logs, caches, arquivos temporários, dados locais etc.
│
├── Makefile
│   → Atalhos para operações frequentes, como subir o ambiente,
│     executar testes, inicializar dados ou derrubar containers.
│
├── infra/
│   → Arquivos relacionados à infraestrutura do ambiente local.
│
│   └── docker/
│       → Configurações específicas de containers que não cabem
│         diretamente no docker-compose, como Dockerfiles customizados.
│
├── postgreSQL/
│   → Tudo relacionado ao PostgreSQL que simula o sistema transacional.
│
│   ├── init/
│   │   → Scripts executados na inicialização do banco.
│   │     Ex.: criação de database, usuários, permissões e configuração inicial.
│   │
│   ├── schema/
│   │   → Definição das tabelas transacionais e demais objetos SQL.
│   │     Ex.: orders, payments, refunds, constraints e índices.
│   │
│   └── seed/
│       → Dados iniciais fictícios utilizados para desenvolvimento e testes.
│         Ex.: pedidos, clientes e pagamentos de exemplo.
│
├── cdc/
│   → Tudo relacionado à captura de mudanças da origem.
│
│   └── debezium/
│       → Configurações do conector Debezium.
│         Ex.: publication, replication slot, tabelas monitoradas,
│         formato dos eventos e configurações do connector.
│
├── spark/
│   → Código dos processamentos executados com Apache Spark.
│
│   ├── historical/
│   │   → Job responsável por consumir eventos CDC e persistir
│   │     a camada histórica append-only.
│   │
│   └── consolidation/
│       → Job responsável por ler o histórico e produzir
│         as tabelas consolidadas com o estado atual dos dados.
│
├── airflow/
│   → Tudo relacionado à orquestração.
│
│   └── dags/
│       → Definição das DAGs do Airflow.
│         Ex.: execução da consolidação, retries, backfills
│         e dependências entre tarefas.
│
├── iceberg/
│   → Configurações relacionadas ao Apache Iceberg.
│
│   └── config/
│       → Configuração de catálogo, warehouse, MinIO,
│         propriedades de tabela e integração com Spark.
│
├── tests/
│   → Testes automatizados do projeto.
│
│   ├── unit/
│   │   → Testam funções ou transformações isoladamente,
│   │     sem depender da infraestrutura completa.
│   │
│   ├── integration/
│   │   → Testam a comunicação entre componentes.
│   │     Ex.: Spark lendo Kafka ou escrevendo Iceberg.
│   │
│   └── e2e/
│       → Testam o fluxo completo.
│         Ex.: INSERT no PostgreSQL → evento CDC →
│         histórico → tabela consolidada.
│
├── scripts/
│   → Scripts auxiliares para desenvolvimento e operação.
│     Ex.: gerar pedidos fictícios, criar connector,
│     executar cenários de falha ou limpar o ambiente.
│
└── docs/
    → Documentação técnica que complementa o README.
    
    ├── architecture/
    │   → Diagramas e explicações detalhadas da arquitetura
    │     e dos fluxos de dados.
    │
    ├── adr/
    │   → Architecture Decision Records.
    │     Cada arquivo documenta uma decisão relevante,
    │     alternativas avaliadas e trade-offs.
    │
    └── runbooks/
        → Procedimentos operacionais.
          Ex.: "conector CDC parou", "replication slot acumulou WAL",
          "como executar um replay" ou "como fazer backfill".


## M0-02 — Definir Docker Compose base.
## M0-03 — Subir PostgreSQL local.
## M0-04 — Criar schema transacional mínimo de pedidos, pagamentos e reembolsos.
## M0-05 — Criar dados seed e comandos de teste.
## M0-06 — Adicionar README inicial com objetivo, arquitetura macro e instruções locais.
## M0-07 — Configurar lint/testes básicos e CI inicial.