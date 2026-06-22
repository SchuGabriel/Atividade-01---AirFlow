# Pipeline de Orquestração de Workflow com Apache Airflow

## Descrição

Este projeto implementa um pipeline de processamento de dados de e-commerce utilizando Apache Airflow.

O pipeline:

- Consome dados da FakeStoreAPI
- Extrai categorias de produtos
- Processa métricas por categoria utilizando Dynamic Task Mapping
- Consolida os resultados
- Persiste dados em PostgreSQL
- Mantém snapshot idempotente
- Mantém histórico de execuções

---

## Tecnologias Utilizadas

- Python 3
- Apache Airflow 3.2
- PostgreSQL 16
- Docker
- Docker Compose

---

## Funcionalidades Implementadas

- TaskFlow API
- XCom automático
- Fan-out
- Fan-in
- Dynamic Task Mapping
- Task Groups
- Pool de concorrência
- Retry com Exponential Backoff
- Callbacks de sucesso, retry e falha
- Persistência PostgreSQL
- Snapshot Idempotente
- Histórico de Execuções

---

## Estrutura do Projeto

```text
.
├── dags/
│   └── ecommerce_pipeline.py
├── logs/
├── plugins/
├── docker-compose.yaml
├── requirements.txt
└── README.md
```

---

## Pré-requisitos

Instalar:

- Docker Desktop
- Docker Compose

Verificar instalação:

```bash
docker --version
docker compose version
```

---

## Executando o Projeto

### 1. Clonar o repositório

```bash
git clone https://github.com/SchuGabriel/Atividade-01---AirFlow.git
```

### 2. Subir os containers

```bash
docker compose up -d
```

Verificar se os containers estão em execução:

```bash
docker ps
```

---

## Acessando o Airflow

Abrir no navegador:

```text
http://localhost:8080
```

Credenciais padrão:

```text
Usuário: airflow
Senha: airflow
```

---

## Configuração Obrigatória

### Criar Connection PostgreSQL

No Airflow:

```text
Admin → Connections
```

Criar uma nova conexão com os seguintes dados:

| Campo     | Valor            |
| --------- | ---------------- |
| Conn Id   | postgres_default |
| Conn Type | Postgres         |
| Host      | postgres         |
| Schema    | airflow          |
| Login     | airflow          |
| Password  | airflow          |
| Port      | 5432             |

Salvar a conexão.

---

### Criar Pool

No Airflow:

```text
Admin → Pools
```

Criar um novo pool:

| Campo       | Valor                                                    |
| ----------- | -------------------------------------------------------- |
| Pool Name   | ecommerce_pool                                           |
| Slots       | 2                                                        |
| Description | Limite de concorrência para processamento das categorias |

Salvar.

---

## Executando a DAG

Na interface do Airflow:

```text
DAGs
↓
ecommerce_pipeline
↓
Trigger DAG
```

---

## Fluxo da DAG

```text
ingestao
├── fetch_products
└── extract_categories

analise
├── calculate_metrics (Dynamic Task Mapping)
├── consolidate_metrics
├── save_to_postgres
└── save_history
```

---

## Banco de Dados

O projeto cria automaticamente as tabelas necessárias caso elas não existam.

### Snapshot Idempotente

Tabela:

```sql
category_metrics
```

Armazena apenas um registro por categoria e data de execução.

### Histórico de Execuções

Tabela:

```sql
category_metrics_history
```

Armazena todas as execuções realizadas.

---

## Consultando os Dados

Entrar no PostgreSQL:

```bash
docker exec -it atividade01-airflow-postgres-1 psql -U airflow
```

Consultar snapshot:

```sql
SELECT * FROM category_metrics;
```

Consultar histórico:

```sql
SELECT * FROM category_metrics_history;
```

---

## Idempotência

A tabela `category_metrics` utiliza chave primária composta:

```sql
(category, execution_date)
```

e a instrução:

```sql
ON CONFLICT DO NOTHING
```

garantindo que reprocessamentos não gerem registros duplicados.

---

## Requisitos Atendidos

- ✅ TaskFlow API
- ✅ XCom automático
- ✅ Fan-out
- ✅ Fan-in
- ✅ Dynamic Task Mapping
- ✅ Task Groups
- ✅ Pool de concorrência
- ✅ Retry
- ✅ Exponential Backoff
- ✅ Callbacks
- ✅ PostgreSQL Hook
- ✅ Persistência PostgreSQL
- ✅ Snapshot Idempotente
- ✅ Histórico de Execuções

---

## Autor

Gabriel Schu

Pós-Graduação em Engenharia de Software
Disciplina: Orquestração de Workflows
