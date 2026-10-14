# PMO - Reliable Postgres CDC Lakehouse

## Objetivo:

Centralizar em um data lakehouse os dados transacionais de um sistema de pedidos, para produzir uma camada analítica que possa ser consumida pelo time que faz a gestão do produto.


## Escopo:

Dentro do Escopo:

Criação e configuração de um(a):

- Banco transacional apto a armazenar os dados do sistema de pedidos;
- Camada de transporte dos eventos transacionais;
- Captura de alteraçlões via CDC;
- Object Storage para persistência dos dados;
- Persistência histórica dos eventos originais na Raw Landing.
- Compactação e publicação da Raw paraconsumo downstream;
- Construção das tabelas analíticas;
- Catalogação, governança e controle de acesso;
- Orquestração, retentativas, reprocessamento e tratamento de falhas.

Fora do escopo:
- Desenvolvimento da aplicação originadora dos dados;
- Consumo analítico por dashboards, modelos de machine learning ou estudos aprofundados.


## Etapas

### M0 - Fundação

Resultado esperado:
- Ambiente de execução e banco transacional operacionais.

Critério de conclusão:
- Ambiente configurado e funcional;
- Banco e tabelas disponíveis;
- Leitura e persistência validadas;
- Configuração documentada.

Tarefas e subtarefas:

M0-T01 - Definir ambiente de execução
* M0-T01.1 - Avaliar altertnativas de hospedagem;
* M0-T01.2 - Seleciona a tecnologia;
* M0-T01.3 - Registrar a decisão.

M0-T02 - Definir banco transacional
* M0-T02.1 - Avaliar alternativas;
* M0-T02.2 - Selecionar o banco;
* M0-T02.3 - Registrar a decisão.

M0-T03 - Configurar banco
* M0-T03.1 - Provisionar o serviço;
* M0-T03.2 - Configurar conexão e credenciais;
* M0-T03.3 - Validar disponibilidade.

M0-T04 - Criar estrutura transacional
* M0-T04.1 - Definir tabelas e relacionamentos;
* M0-T04.2 - Implementar o schema;
* M0-T04.3 - Validar a estrutura.

M0-T05 - Validar funcionamento
* M0-T05.1 - Inserir dados de teste;
* M0-T05.2 - Consultar e atualizar registros;
* M0-T05.3 - Validar persistência.

M0-T06 - Documentar
* M0-T06.1 - Registrar arquitetura e configurações;
* M0-T06.2 - Documentar procedimentos de inicializaçãoe e validação.

### M1 - CDC

Resultado esperado:
- Alterações dos banco transacional capturadas e disponibilizadas na camada de transporte

Critério de conclusão:
- Transporte operacional;
- Eventos CDC capturados corretamente;
- Operações transacionais validadas;
- Configuração documentada.

Tarefas e subtarefas:

M1-T01 - Configurar transporte
* M1-T01.1 - Avaliar e selecionar a tecnologia;
* M1-T01.2 - Provisionar os componentes;
* M1-T01.3 - Validar comunicação e disponibilidade.

M1-T02 - Configurar CDC
* M1-T02.1 - Preparar o banco para captura de alterações;
* M1-T02.2 - Configurar o conector CDC;
* M1-T02.3 - Configurar publicação dos eventos no transporte.

M1-T03 - Validar fluxo
* M1-T03.1 - Gerar inserções, atualizações e exclusões;
* M1-T03.2 - Validar captura e conteúdo dos eventos;
* M1-T03.3 - Verificar integridade e ordenação aplicável.

M1-T04 - Documentar
* M1-T04.1 - Registrar configurações e decisões;
* M1-T04.2 - Documentar execução e validação do CDC.

### M2 - Camada Raw/Histórica

Resultado esperado:
- Eventos CDC persistidos historicamente e publicados em uma Raw organizada, preservando alta fidelidade aos eventos originais.

Critério de conclusão:
- Ingestão funcional;
- Persistência histórica com alterações mínimas;
- Publicação validada;
- Idempotência e recuperação de falhas;
- Raw Compacted disponível para consumo.

Tarefas e subtarefas:

M2-T01 - Definir engine de processamento
* M2-T01.1 - Avaliar alternativas;
* M2-T01.2 - Selecionar e configurar a engine.

M2-T02 - Configurar Object Storage
* M2-T02.1 - Selecionar tecnologia;
* M2-T02.2 - Configurar armazenamento e acesso;
* M2-T02.3 - Validar leitura e escrita.

M2-T03 - Organizar a Raw
* M2-T03.1 - Definir diretórios de Landing e Compacted;
* M2-T03.2 - Definir particionamento;
* M2-T03.3 - Padronizar identificação dos eventos.

M2-T04 - Implementar a ingestão
* M2-T04.1 - Consumir eventos do transporte;
* M2-T04.2 - Persistir eventos na Raw Landing;
* M2-T04.3 - Configurar checkpoint e recuperação;
* M2-T04.4 - Validar persistência.

M2-T05 - Definir publicação
* M2-T05.1 - Definir critérios de publicação;
* M2-T05.2 - Definir identificação das execuções;
* M2-T05.3 - Definir controle de dados de estado da publicação.

M2-T06 - Implementar compactação
* M2-T06.1 - Ler dados da Raw Landing;
* M2-T06.2 - Reorganizar e compactar arquivos;
* M2-T06.3 - Persistir resultado na Raw Compacted.

M2-T07 - Validar equivalência
* M2-T07.1 - Definir critérios de comparação;
* M2-T07.2 - Comparar eventos de entrada e saída;
* M2-T07.3 - Registrar resultados.

M2-T08 - Implementar controle de publicação
* M2-T08.1 - Registrar estados de execução;
* M2-T08.2 - Publicar somente resultados validados;
* M2-T08.3 - Disponibilizar referência da versão publicada.

M2-T09 - Validar recuperação e idempotência
* M2-T09.1 - Testar reexecução da ingestão;
* M2-T09.2 - Testar reexecução da compactação;
* M2-T09.3 - Validar recuperação após falhas.

M2-T10 - Documentar
* M2-T10.1 - Registrar arquitetura e contratos;
* M2-T10.2 - Documentar publicação e recuperação;
* M2-T10.3 - Registrar evidências de validação.

### M3 - Camada Analítica

Resultado esperado:
- Tabelas analíticas histórica e de estado atual disponíveis para consumo.

Critério de conclusão:
- Eventos CDC interpretados e normalizados;
- orders_history e orders_current funcionais;
- Escrita idempotente e reprocessamento;
- Catalogação e controle de acesso;
- Resultados validados.

Tarefas e subtarefas:

M3-T01 - Organizar armazenamento analítico
* M3-T01.1 - Definir estrutura de armazenamento no MinIO;
* M3-T01.2 - Configurar caminhos das tabelas Iceberg;
* M3-T01.3 - Validar acesso ao armazenamento.

M3-T02 - Definir tecnologias analíticas
* M3-T02.1 - Selecionar Apache Iceberg;
* M3-T02.2 - Selecionar Apache Spark;
* M3-T02.3 - Definir orders_history e orders_current.

M3-T03 - Interpretar e normalizar CDC
* M3-T03.1 - Implementar leitura da Raw Compacted publicada;
* M3-T03.2 - Interpretar operações CRUD;
* M3-T03.3 - Tratar tombstones;
* M3-T03.4 - Normalizar before e after;
* M3-T03.5 - Converter tipos de dados;
* M3-T03.6 - Preservar metadados técnicos;
* M3-T03.7 - Validar o parser.

M3-T04 - Definir ordenação dos eventos
* M3-T04.1 - Definir identificação por id_order;
* M3-T04.2 - Definir ordenação por source_lsn;
* M3-T04.3 - Definir consolidação utilizando max_by;
* M3-T04.4 - Definir representação de exclusões;
* M3-T04.5 - Definir tratamento de ambiguidade.

M3-T05 - Implementar tabelas analíticas
* M3-T05.1 - Criar orders_history;
* M3-T05.2 - Implementar persistência histórica;
* M3-T05.3 - Criar orders_current;
* M3-T05.4 - Implementar consolidação do estado atual;
* M3-T05.5 - Integrar escrita ao catálogo.

M3-T06 - Implementar idempotência e reprocessamento
* M3-T06.1 - Definir identidade técnica para deduplicação;
* M3-T06.2 - Implementar escrita incremental;
* M3-T06.3 - Tratar eventos fora de ordem;
* M3-T06.4 - Implementar reprocessamento por data;
* M3-T06.5 - Validar reexecuções.

M3-T07 - Validar tabelas
* M3-T07.1 - Comparar histórico analítico com eventos da Raw;
* M3-T07.2 - Validar unicidade e estado atual dos pedidos;
* M3-T07.3 - Testar criação, atualização e exclusão;
* M3-T07.4 - Validar tipos de dados;
* M3-T07.5 - Registrar evidências.

M3-T08 - Configurar catálogo e governança
* M3-T08.1 - Configurar PostgreSQL dedicado ao Polaris;
* M3-T08.2 - Execurtar bootstrap do Polaris;
* M3-T08.3 - Validar serviço principal;
* M3-T08.4 - Configurar catálogo e namespaces;
* M3-T08.5 - Integrar Spark ao REST Catalog;
* M3-T08.6 - Validar leitura e escrita Iceberg;
* M3-T08.7 - Configurar e validar permissões.

M3-T09 - Documentar
* M3-T09.1 - Registrar decisões arquiteturais;
* M3-T09.2 - Documentar interpretação e ordenação CDC;
* M3-T09.3 - Documentar escrita, idempotência e reprocessamento;
* M3-T09.4 - Documentar operação e validação.


### M4 - Orquestração

Resultado esperado:
- Pipeline executado automaticamente, respeitando dependências e periodicidade.

Critério de conclusão:
- Execução diária D-1 automatizada;
- Dependências respeitadas;
- Retentativas configuradas;
- Reprocessamento coordenado.

Tarefas e subtarefas

M4-T01 - Configurar orquestrador
* M4-T01.1 - Avaliar e selecionar tecnologia;
* M4-T01.2 - Configurar serviço e acesso;
* M4-T01.3 - Validar funcionamento.

M4-T02 - Definir fluxo
* M4-T02.1 - Identificar jobs e dependências;
* M4-T02.2 - Definir ordem de execução;
* M4-T02.3 - Implementar fluxo de orquestração.

M4-T03 - Configurar retentativas
* M4-T03.1 - Definir política de retries;
* M4-T03.2 - Configurar tratamento de falhas;
* M4-T03.3 - Validar retentativas.

M4-T04 - Configurar periodicidade
* M4-T04.1 - Definir calendário D-1;
* M4-T04.2 - Configurar agendamento;
* M4-T04.3 - Validar execução automática.

M4-T05 - Validar fluxo completo
* M4-T05.1 - Executar pipeline ponta a ponta;
* M4-T05.2 - Validar dependências e resultados;
* M4-T05.3 - Testar reprocessamento coordenado.

M4-T06 - Documentar
* M4-T06.1 - Registrar arquitetura e configurações;
* M4-T06.2 - Documentar operação, retentativas e reprocessamento.


### M5 - Confiabilidade

Resultado esperado:
- Pipeline validado quanto à integridade, recuperação de falhas e consistências.

Critério de conclusão:
- Recuperação sem perda indevida de eventos;
- Ausência de duplicidades indevidas;
- Consistência entre camadas;
- Execuções recuperáveis ou reprocessáveis;
- Falhas identificáveis;
- Cenários críticos testados.

Tarefas e subtarefas:

M5-T01 - Definir cenários críticos
* M5-T01.1 - Identificar pontos de falha;
* M5-T01.2 - Definir resultados esperados;
* M5-T01.3 - Preparar dados e procedimento de teste.

M5-T02 - Testar interrupção da ingestão
* M5-T02.1 - Interromper Spark durante o consumo;
* M5-T02.2 - Retomar execução;
* M5-T02.3 - Validar integridade dos eventos.

M5-T03 - Testar falha na publicação
* M5-T03.1 - Interromper compactação antes da publicação;
* M5-T03.2 - Recuperar ou reexecutar;
* M5-T03.3 - Validar consistência da Raw publicada.

M5-T04 - Testar consolidação repetida
* M5-T04.1 - Executar consolidação analítica;
* M5-T04.2 - Reexecutar com os mesmos dados;
* M5-T04.3 - Validar ausência de duplicidades.

M5-T05 - Testar reprocessamento histórico
* M5-T05.1 - Selecionar partição histórica;
* M5-T05.2 - Reprocessar a partição;
* M5-T05.3 - Comparar resultados.

M5-T06 - Testar indisponibilidade
* M5-T06.1 - Simular indisponibilidade temporária de componente;
* M5-T06.2 - Restabelecer o serviço;
* M5-T06.3 - Validar recuperação do fluxo.

M5-T07 - Validar integridade
* M5-T07.1 - Comparar eventos entre camadas;
* M5-T07.2 - Verificar perdas e duplicidades;
* M5-T07.3 - Validar consistência final.

M5-T08 - Avaliar observabilidade
* M5-T08.1 - Verificar logs de execução;
* M5-T08.2 - Validar identificação de falhas;
* M5-T08.3 - Registrar limitações.

M5-T09 - Documentar
* M5-T09.1 - Registrar cenários e resultados;
* M5-T09.2 - Consolidar evidências;
* M5-T09.3 - Documentar limitações conhecidas.

### M6 - Documentação

Resultado esperado:
- Documentação suficiente para compreender, executar, validar e avaliar o projeto.

Critério de conclusão:
- ADRs completos;
- Repositório organizado;
- Código documentado quando necessário;
- README com arquitetura e procedimentos;
- Evidências de testes disponíveis;
- Instruções reproduzíveis em ambiente limpo.

Tarefas e subtarefas:

M6-T01 - Consolidar ADRs
* M6-T01.1 - Identificar decisões arquiteturais;
* M6-T01.2 - Registrar contexto, alternativas e trade-offs;
* M6-T01.3 - Revisar garantias e reconsiderações.

M6-T02 - Organizar repositório
* M6-T02.1 - Revisar estrutura de diretórios;
* M6-T02.2 - Organizar códigos, configurações e documentação;
* M6-T02.3 - Validar consistências da organização.

M6-T03 - Padronizar documentação de código
* M6-T03.1 - Definir padrão de comentários;
* M6-T03.2 - Documentar contratos e comportamentos não intuitivos;
* M6-T03.3 - Revisar aplicação do padrão.

M6-T04 - Criar README
* M6-T04.1 - Descrever objetivo e arquitetura;
* M6-T04.2 - Documentar componentes e integrações;
* M6-T04.3 - Referenciar ADRs;
* M6-T04.4 - Documentar configuração, execução e validação.

M6-T05 - Validar reprodutibilidade
* M6-T05.1 - Preparar ambiente limpo;
* M6-T05.2 - Executar instruções documentadas;
* M6-T05.3 - Corrigir lacunas identificadas.

M6-T06 - Consolidar evidências
* M6-T06.1 - Reunir testes de integração;
* M6-T06.2 - Reunir testes de recuperação e reprocessamento;
* M6-T06.3 - Organizar referências às evidências.


## Plano de execução

O plano referencia as tarefas e subtarefas da seção 3. O status representa a situação conhecida, enquanto a ordem indica a sequência preferencial de trabalho.

### Situação geral:

-------------------------------------------------------
Etapa                       | Status
-------------------------------------------------------
M0 - Fundação               | Concluído provisoriamente
M1 - CDC                    | Concluído provisoriamente
M2 - Camada Raw/Histórica   | Concluído provisoriamente
M3 - Camada Analítica       | Em andamento
M4 - Orquestração           | Não iniciado
M5 - Confiabilidade         | Não iniciado
M6 - Documentação           | Em andamento
-------------------------------------------------------

M3 - Acompanhamento

-----------------------------------------
Referência  | Status
-----------------------------------------
M3-T01      | Em andamento
M3-T02      | Concluído
M3-T03      | Em andamento
M3-T04      | Definição técnica concluída
M3-T05      | Não iniciado
M3-T06      | Não iniciado
M3-T07      | Não iniciado
M3-T08      | Em andamento
M3-T09      | Em andamento
-----------------------------------------

Próxima atividade: M3-T08.3 - Validar serviço principal do Apache Polaris.

Demais etapas

* M0-M2: Implementadas; revisão retrospectiva dos critérios pendentes;
* M4-M5: execução não iniciada;
* M6: documentação parcialmente realizada.

O acompanhamento individual das subtarefas será registrado conforme sua execução for confirmada.

## Observações

* OBS-001 - Validação retrospectiva:
    M0-M2 foram implementadas antes da formalização completa dos critérios de conclusão. A validação retrospectiva permanece pendente.

* OBS-002 - Ordenação CDC:
    A consolidação do estado atual utiliza source_lsn como referêcnai principal de ordenação por id_order. Eventos com source_lsn idêntico para o mesmo pedido exigem tratamento explícito.
        Referência: M3-T04

* OBS-003 - Apache Polaris
    O PostgreSQL dedicado e o bootstrap inicial foram configurados. Permanecem pendentes a validação do serviço principal e a integração com Spark, Iceberg e MinIO.
        Referência: M3-T08

* OBS-004 - Riscos
------------------------------------------------------------------------------
ID  | Risco                                               | Impacto preliminar
------------------------------------------------------------------------------
R01 | Perda ou inconsistência de eventos                  | Alto
R02 | Conflitos de concorrência                           | Alto
R03 | Falhas que comprometam a continuidade               | Alto
R04 | Casos excepcionais não previstos                    | Alto
R05 | Problemas de integração Polaris/Spark/Iceberg/MinIO | Alto
------------------------------------------------------------------------------

Impedimentos confirmados: nenhum registrado.

* OBS-005 - Histórico de execução

O histórico deverá registrar alterações relevantes, decisões, falhas significativas e evidências de conclusão, com data e referência à tarefa correspondente.

Registros anteriores permanecem parcialmente distribuídos entre o repositório e a documentação técnica.