# Brazilian Retail Lakehouse

> Plataforma de dados end-to-end simulando o ecossistema de uma rede de varejo brasileira — eventos Kafka em tempo real, arquitetura Medallion com Delta Live Tables, contratos de dados, camada semântica dbt e modelos de ML.

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)
![Delta Live Tables](https://img.shields.io/badge/Delta_Live_Tables-003366?style=for-the-badge&logo=delta&logoColor=white)
![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-0194E2?style=for-the-badge&logo=databricks&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

---

## 📌 Sobre o projeto

Plataforma lakehouse que simula o ecossistema de dados de uma rede de varejo brasileira com múltiplos domínios: **vendas**, **estoque**, **clientes** e **pagamentos**. Os eventos são gerados pelo **ShadowTraffic** (simulador declarativo), publicados no **Confluent Cloud (Kafka)** e processados no **Databricks** via **Delta Live Tables** com validações automáticas de qualidade em cada camada.

O projeto é **intencionalmente evolutivo** — cada etapa adiciona uma camada de maturidade arquitetural sem quebrar o que foi construído antes, contando a história de como uma plataforma de dados cresce de um pipeline simples para um sistema de produção real.

---

## Arquitetura

```
┌─────────────────────────────────────────────────────────────┐
│                    SIMULADOR DE EVENTOS                      │
│         ShadowTraffic (Docker local / GitHub Actions)        │
│   retail.vendas │ retail.estoque │ retail.clientes           │
│                    retail.pagamentos                         │
└──────────────────────────┬──────────────────────────────────┘
                           │  Confluent Cloud (Kafka)
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                   BRONZE — Delta Lake                        │
│        DLT Pipelines com Expectations (schema enforcement)   │
│   raw_vendas │ raw_estoque │ raw_clientes │ raw_pagamentos   │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                   SILVER — Delta Lake                        │
│         DLT com validações de qualidade (expect_or_drop)     │
│        vendas │ estoque │ clientes │ pagamentos              │
└──────────────────────────┬──────────────────────────────────┘
                           │  dbt (Etapa 3)
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                    GOLD — dbt models                         │
│   fct_vendas │ dim_produto │ dim_cliente │ dim_loja          │
│   mrt_performance_loja │ mrt_risco_fraude │ mrt_estoque      │
└──────────────────────────┬──────────────────────────────────┘
                           │
              ─────────────┴─────────────
              ▼                         ▼
┌──────────────────────┐   ┌────────────────────────────┐
│    ML PLATFORM       │   │      OBSERVABILITY          │
│  MLflow Registry     │   │  Data Contracts Monitor     │
│  demand_forecast     │   │  Quality Trends             │
│  fraud_detector      │   │  SLA Compliance             │
└──────────────────────┘   └────────────────────────────┘
```

---

## Domínios de Dados

| Domínio | Tópico Kafka | Frequência | Descrição |
|---------|-------------|------------|-----------|
| Vendas | `retail.vendas` | 10–50 eventos/min | Transações de venda (loja, produto, valor) |
| Estoque | `retail.estoque` | 5–20 eventos/min | Movimentações de estoque |
| Clientes | `retail.clientes` | 1–5 eventos/min | Cadastro e atualizações de clientes |
| Pagamentos | `retail.pagamentos` | 10–50 eventos/min | Eventos de pagamento (pix, crédito, débito) |

---

## Roadmap de Etapas

| Etapa | Descrição | Status |
|-------|-----------|--------|
| **Etapa 1** | Fundação — Kafka + DLT + Bronze/Silver/Gold básico | ✅ Concluída |
| **Etapa 2** | Contratos de Dados — Schema registry + validação automática | ✅ Concluída |
| **Etapa 3** | Camada dbt — Modelo dimensional + semantic layer | ⏳ Planejada |
| **Etapa 4** | Plataforma de ML — Previsão de demanda + detecção de fraude | ⏳ Planejada |
| **Etapa 5** | Observabilidade — Data health + SLA + drift monitoring | ⏳ Planejada |
| **Etapa 6** | Avançado — Feature Store online + Model Serving + A/B test | ⏳ Planejada |

---

## Etapa 1 — Fundação

Pipeline completo Kafka → Bronze → Silver → Gold com 1,2 milhão de eventos históricos processados.

**ShadowTraffic (simulador de eventos)**
- `bootstrap.json` — gera histórico de eventos em massa (execução única)
- `realtime.json` — publica em loop contínuo simulando horários de pico

**DLT Bronze** — ingestão dos 4 tópicos Kafka
```python
@dlt.table(name="raw_vendas")
@dlt.expect("payload_nao_nulo", "payload IS NOT NULL")
def raw_vendas():
    return spark.readStream.format("kafka")...
```

**DLT Silver** — parse, tipagem e validação de qualidade
```python
@dlt.expect_or_drop("preco_positivo", "preco_unitario > 0")
@dlt.expect_or_drop("quantidade_positiva", "quantidade > 0")
def vendas():
    return dlt.read_stream("raw_vendas").select(from_json(...))
```

**Gold Básico** — 4 tabelas analíticas com `MERGE INTO`

| Tabela | Descrição |
|--------|-----------|
| `gold.vendas_por_loja_dia` | KPIs de vendas agregados por loja e dia |
| `gold.top_produtos` | Ranking de produtos por receita |
| `gold.status_pagamentos` | Distribuição de métodos e status de pagamento |
| `gold.estoque_atual` | Posição atual de estoque com classificação de criticidade |

---

## Etapa 2 — Contratos de Dados

Camada de governança que formaliza o acordo entre produtores e consumidores de dados. Cada domínio possui um contrato YAML versionado com schema, SLAs e regras de qualidade.

**Contratos disponíveis**

| Contrato | Freshness SLA | Volume mínimo/dia | Severidades |
|----------|--------------|-------------------|-------------|
| `retail.vendas` | 30 min | 5.000 eventos | CRITICAL + WARNING |
| `retail.estoque` | 60 min | 500 eventos | CRITICAL + WARNING |
| `retail.clientes` | 120 min | 100 eventos | CRITICAL + WARNING |
| `retail.pagamentos` | 30 min | 1.000 eventos | CRITICAL + WARNING |

**Fluxo de contratos**

```
contracts/*.yaml  →  04_contract_registry  →  contracts.registry
                                                      │
                                         05_contract_validator
                                                      │
                                         contracts.violations
                                                      │
                                         06_contract_monitor (dashboard)
```

- Violações **CRITICAL** → registradas e o job falha (bloqueia dependentes no Workflow)
- Violações **WARNING** → registradas, execução continua
- `06_contract_monitor` — dashboard com freshness check, tendência de 30 dias e resumo executivo

---

## Estrutura do Repositório

```
brazilian-retail-lakehouse/
├── databricks.yml                    # Databricks Asset Bundle
├── contracts/
│   ├── vendas_v1.yaml                # Contrato do domínio de vendas
│   ├── estoque_v1.yaml               # Contrato do domínio de estoque
│   ├── clientes_v1.yaml              # Contrato do domínio de clientes
│   └── pagamentos_v1.yaml            # Contrato do domínio de pagamentos
├── shadowtraffic/
│   ├── connections.json              # Conexão Confluent Cloud (via env vars)
│   ├── bootstrap.json                # Geração de histórico em massa
│   └── realtime.json                 # Publicação contínua com horários de pico
└── src/
    └── notebooks/
        ├── 00_setup.py               # Catalog, schemas, volumes, secrets
        ├── bronze/
        │   └── 01_dlt_bronze.py      # DLT: Kafka → Bronze (4 tabelas)
        ├── silver/
        │   └── 02_dlt_silver.py      # DLT: Bronze → Silver (parse + validação)
        ├── gold/
        │   └── 03_gold_basic.py      # Agregações analíticas básicas
        └── contracts/
            ├── 04_contract_registry.py   # YAML → contracts.registry
            ├── 05_contract_validator.py  # Silver → contracts.violations
            └── 06_contract_monitor.py    # Dashboard de conformidade
```

---

## Como executar

### Pré-requisitos
- Docker Desktop + WSL2 (Ubuntu)
- Conta Confluent Cloud com cluster e tópicos criados
- Conta ShadowTraffic (free trial — shadowtraffic.io)
- Databricks CLI configurado

### Configurar credenciais

Crie o arquivo `shadowtraffic/.env`:
```bash
CONFLUENT_BOOTSTRAP=<bootstrap-server>:9092
CONFLUENT_KEY=<api-key>
CONFLUENT_SECRET=<api-secret>
CONFLUENT_JAAS=org.apache.kafka.common.security.plain.PlainLoginModule required username="<api-key>" password="<api-secret>";
```

Configure os Databricks Secrets:
```bash
databricks secrets create-scope retail-lakehouse
databricks secrets put-secret retail-lakehouse confluent_bootstrap_servers --string-value "<valor>"
databricks secrets put-secret retail-lakehouse confluent_api_key --string-value "<valor>"
databricks secrets put-secret retail-lakehouse confluent_api_secret --string-value "<valor>"
```

### Gerar carga histórica (executar uma vez)

> Execute os comandos abaixo no terminal **WSL Ubuntu**, a partir da raiz do projeto.

**1. Validar configuração antes de publicar (dry run — não envia ao Kafka)**
```bash
docker run --rm \
  --env-file shadowtraffic/license.env \
  --env-file shadowtraffic/.env \
  -v "$(pwd)/shadowtraffic":/shadowtraffic \
  shadowtraffic/shadowtraffic:latest \
  --config /shadowtraffic/bootstrap.json \
  --stdout --sample 5
```

**2. Gerar histórico (~1,2 milhão de eventos)**
```bash
docker run --rm \
  --env-file shadowtraffic/license.env \
  --env-file shadowtraffic/.env \
  -v "$(pwd)/shadowtraffic":/shadowtraffic \
  shadowtraffic/shadowtraffic:latest \
  --config /shadowtraffic/bootstrap.json
```

### Publicar eventos em tempo real

```bash
docker run --rm \
  --env-file shadowtraffic/license.env \
  --env-file shadowtraffic/.env \
  -v "$(pwd)/shadowtraffic":/shadowtraffic \
  shadowtraffic/shadowtraffic:latest \
  --config /shadowtraffic/realtime.json
```

### Deploy e execução no Databricks
```bash
databricks bundle deploy

# Etapa 1 — Fundação
databricks bundle run setup           # executar uma única vez
databricks bundle run gold_basic      # executar após o DLT pipeline

# Etapa 2 — Contratos de Dados
databricks bundle run contract_registry   # executar ao criar/atualizar contratos
databricks bundle run contract_validator  # executar após cada execução do DLT
databricks bundle run contract_monitor    # dashboard de conformidade
```

---

## Unity Catalog

```
Catalog : retail_lakehouse
Schemas : bronze | silver | gold | ml_features | contracts | observability
```

---

## Ambiente

- **Databricks Free Edition** (Serverless AWS)
- **Unity Catalog** habilitado
- **Confluent Cloud** free tier (10 GB/mês) como broker Kafka
- **GitHub Actions** para CI/CD (Etapa 3+)
