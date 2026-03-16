# Knowledge Base — Infraspeak ETL (Carmel Hotéis)

> Documento de referência completo para agentes de IA que precisam entender, consultar ou modificar este projeto.

---

## 1. VISÃO GERAL DO PROJETO

Sistema ETL em Python que extrai dados da **API Infraspeak v3** (ERP de manutenção hoteleira) e persiste em um banco **PostgreSQL** em produção. Os dados são usados para dashboards de gestão de manutenção dos hotéis da rede Carmel.

- **Ambiente de produção**: PostgreSQL em `10.197.3.2`, schema `carmel`
- **API fonte**: `https://api.infraspeak.com/v3` (REST + Bearer Token)
- **Autenticação**: variáveis de ambiente em `auth/prod/.env`
- **Rate limit da API**: 60 requisições/minuto (header `Retry-After` no 429)

---

## 2. ENTIDADES DO NEGÓCIO

| Entidade | Descrição | Endpoint API |
|----------|-----------|--------------|
| **Failure** | Chamado de manutenção corretiva | `failures/` e `failures/{id}` |
| **Work** | Plano mestre de manutenção preventiva | `works/` e `works/{id}` |
| **Scheduled Work** | Ocorrência gerada por um Work (execução real) | `works/scheduled/` e `works/scheduled/{id}` |
| **Operator** | Técnico/operador que executa os serviços | `operators/` |

### Hotéis mapeados (via `local_full_name`)
| Padrão no nome | Alias |
|----------------|-------|
| `CARMEL CUMBUCO%` | CUMBUCO |
| `CARMEL TAÍBA%` | TAÍBA |
| `CARMEL CHARME%` | CHARME |
| `MAGNA PRAIA HOTEL%` | MAGNA |

---

## 3. BANCO DE DADOS — TABELAS RAW (BRONZE)

Todas as tabelas ficam no schema `carmel`. Os dados são armazenados como JSONB completo (resposta da API).

### 3.1 Tabelas e suas chaves

| Tabela | PK | Coluna de dado | Tipo |
|--------|----|----------------|------|
| `infraspeak_raw_failures` | `failure_id` | `data JSONB` | Bulk da API (lista) |
| `infraspeak_raw_failure_details` | `failure_id` | `data JSONB` | Detalhe individual + events |
| `infraspeak_raw_works` | `work_id` | `data JSONB` | Bulk da API (lista) |
| `infraspeak_raw_work_details` | `work_id` | `data JSONB` | Detalhe individual |
| `infraspeak_raw_scheduled_works` | `scheduled_work_id` | `data JSONB` | Bulk da API (lista) |
| `infraspeak_raw_scheduled_work_details` | `scheduled_work_id` | `data JSONB` | Detalhe + events |
| `infraspeak_raw_operators` | `operator_id` | `data JSONB` | Dump completo |

Todas possuem também: `extracted_at TIMESTAMP WITH TIME ZONE` (atualizado a cada upsert).

### 3.2 Diferença de estrutura JSONB entre bulk e detail

**Tabelas bulk** (ex: `infraspeak_raw_failures`):
```
data -> 'id'                             → ID do registro
data -> 'attributes' ->> 'state'         → estado do chamado (ex: PAUSED, OPEN, CLOSED)
data -> 'attributes' ->> 'status'        → status de workflow
data -> 'attributes' ->> 'date'          → data de abertura
```

**Tabelas de detalhe** (ex: `infraspeak_raw_failure_details`):
```
data -> 'data' -> 'id'                         → ID do registro
data -> 'data' -> 'attributes' ->> 'state'     → estado textual livre
data -> 'data' -> 'attributes' ->> 'status'    → status do workflow (PAUSED, OPEN, CLOSED, EXCLUIDO)
data -> 'included'                             → array com event, event_registry, location, operator, etc.
```

> **ATENÇÃO**: A tabela de detalhe tem um nível extra `data -> 'data'` porque armazena o envelope completo da API (`{ data: {...}, included: [...] }`).

---

## 4. CAMPOS DE STATUS / STATE

### Failures (`infraspeak_raw_failures`)
- Campo relevante: `data -> 'attributes' ->> 'state'`
- Valores conhecidos da API: `OPEN`, `PAUSED`, `IN_PROGRESS`, `ON_HOLD`, `CLOSED`, `ASSIGNED`
- Valor customizado do ETL: `EXCLUIDO` (marcado quando a API retorna 404)

### Failures detalhe (`infraspeak_raw_failure_details`)
- Campo relevante para status de workflow: `data -> 'data' -> 'attributes' ->> 'status'`
- Mesmo conjunto de valores + `EXCLUIDO`

### Scheduled Works (`infraspeak_raw_scheduled_works`)
- Campo: `data -> 'attributes' ->> 'state'`
- Valores: `PENDING`, `RUNNING`, `COMPLETED`, `SKIPPED`

---

## 5. VIEWS ANALÍTICAS (CAMADA PRATA/OURO)

### `carmel.v_operadores`
Dimensão de operadores/técnicos.
- Fonte: `infraspeak_raw_operators`
- Campos principais: `operator_id`, `nome_operador`, `email`, `custo_por_hora`, `entity_id`

### `carmel.v_detalhe_planos_manutencao`
Dimensão de planos mestres de manutenção preventiva.
- Fonte: `infraspeak_raw_work_details` UNION `infraspeak_raw_works`
- Lógica: prefere o detalhe quando disponível, cai no bulk como fallback
- Campos: `work_id`, `nome_plano`, `tipo_plano`, `periodicidade`, `hotel`, `local_full_name`

### `carmel.v_detalhe_ocorrencias`
Fato de ocorrências preventivas (scheduled works).
- Fonte: `infraspeak_raw_scheduled_work_details` UNION `infraspeak_raw_scheduled_works`
- JOIN com `v_detalhe_planos_manutencao` para trazer nome do plano e localização
- Campos: `scheduled_work_id`, `work_id`, `status`, `data_agendada`, `data_conclusao`, `hotel`, `nome_plano`, `percentual_conclusao`

### `carmel.v_trabalho_analitico_operador_chamados`
Apontamento de horas em chamados (failures).
- Fonte: `infraspeak_raw_failure_details` → array `included` (type = `event` e `event_registry`)
- Lógica: pares STARTED→PAUSED e RESUMED→COMPLETED definem janelas de trabalho
- Campos: `failure_id`, `operator_id`, `data_inicio`, `data_fim`, `duracao`, `horas_decimais`

### `carmel.v_trabalho_analitico_operador_ocorrencias`
Apontamento de horas em ocorrências preventivas.
- Fonte: `infraspeak_raw_scheduled_work_details` → array `included`
- Mesma lógica de pares de eventos que a view de chamados
- Campos: `scheduled_work_id`, `operator_id`, `data_inicio`, `data_fim`, `duracao`, `horas_decimais`

---

## 6. ARQUIVOS PYTHON — RESPONSABILIDADES

| Arquivo | Papel |
|---------|-------|
| `api.py` | Cliente HTTP: autenticação, throttling (429), paginação automática, `RouteManager` com queries prontas |
| `utils.py` | Conexão com banco, `upsert_raw_data()`, `get_failure_ids_by_state()`, `mark_failure_as_deleted()` |
| `extractor.py` | Loop de extração detalhada com retry (3x, backoff 2^n), log de 404 em `ids_perdidos_detalhe.log`, chama `mark_failure_as_deleted` em 404 |
| `sync.py` | Sync incremental (últimos 3 dias) + varredura de failures PAUSED |
| `history_sync.py` | Backfill histórico dia a dia: `python history_sync.py 2024-01-01 2024-01-31 [true/false]` |
| `repescagem.py` | Reprocessa IDs de CSVs (`repescagem_failures.csv`, `repescagem_works.csv`, `repescagem_scheduled.csv`) |

---

## 7. FUNÇÕES-CHAVE EM `utils.py`

### `upsert_raw_data(table_name, id_column, data_list, type)`
Insere ou atualiza registros no banco. Em conflito de PK, sobrescreve `data` e `extracted_at`.
```python
utils.upsert_raw_data('infraspeak_raw_failures', 'failure_id', lista_de_dicts, 'failure')
```

### `get_failure_ids_by_state(state)`
Retorna lista de `failure_id` com determinado `state` na tabela bulk.
```python
ids = utils.get_failure_ids_by_state('PAUSED')
```

### `mark_failure_as_deleted(failure_id)`
Marca um failure como `EXCLUIDO` em **ambas** as tabelas:
- `infraspeak_raw_failures`: atualiza `data -> 'attributes' -> 'state'`
- `infraspeak_raw_failure_details`: atualiza `data -> 'data' -> 'attributes' -> 'status'`

---

## 8. FLUXO DO SYNC INCREMENTAL (`sync.py`)

```
run_incremental_sync(days_back=3)
│
├── 1. FAILURES bulk (date_min_last_status_change_date)
│      └── upsert infraspeak_raw_failures
│          └── sync_details → upsert infraspeak_raw_failure_details
│
├── 2. WORKS bulk (date_min_updated_at)
│      └── upsert infraspeak_raw_works
│          └── sync_details (sem records) → upsert infraspeak_raw_work_details
│
├── 3. SCHEDULED WORKS bulk (date_min_updated_at)
│      └── upsert infraspeak_raw_scheduled_works
│          └── sync_details → upsert infraspeak_raw_scheduled_work_details
│
├── 4. OPERATORS (dump completo)
│      └── upsert infraspeak_raw_operators
│
└── 5. VALIDAÇÃO DE FAILURES PAUSED
       └── get_failure_ids_by_state('PAUSED')
           └── sync_details → se 404: mark_failure_as_deleted(id)
```

---

## 9. TRATAMENTO DE 404 — FLUXO COMPLETO

Quando `sync_details` recebe 404 após 3 tentativas para um `failure`:

1. Loga em `ids_perdidos_detalhe.log`:
   ```
   YYYY-MM-DD HH:MM:SS | failure | ID: XXXXXXX | Erro: 404 Client Error: Not Found for url: ...
   ```
2. Chama `mark_failure_as_deleted(r_id)` que:
   - `UPDATE infraspeak_raw_failures SET data = jsonb_set(data, '{attributes,state}', '"EXCLUIDO"') WHERE failure_id = X`
   - `UPDATE infraspeak_raw_failure_details SET data = jsonb_set(data, '{data,attributes,status}', '"EXCLUIDO"') WHERE failure_id = X`

---

## 10. PARÂMETROS DA API INFRASPEAK

### Filtros de data validados por endpoint

| Endpoint | Filtro de data | Descrição |
|----------|---------------|-----------|
| `failures` | `date_min_last_status_change_date` / `date_max_last_status_change_date` | Por mudança de status |
| `works` | `date_min_updated_at` / `date_max_updated_at` | Por atualização |
| `works/scheduled` | `date_min_updated_at` / `date_max_updated_at` | Por atualização |
| `works/scheduled` | `date_min_start_date` / `date_max_start_date` | Por data agendada |

Formato de data: `YYYY-MM-DDTHH:MM:SS`

### Expansões usadas (`expanded=`)

| Recurso | Expansão no bulk | Expansão no detalhe |
|---------|-----------------|---------------------|
| failures | `events,operator,location` | `operator,location,client,problem,events.registry` |
| works | `workPeriodicity,workSlaRules,workType,client,locations` | idem + `operators` |
| scheduled_works | `work.client,work.locations,work.operators,work.work_type` | idem + `events.registry` |

---

## 11. QUERIES ÚTEIS DE AUDITORIA

```sql
-- Failures marcadas como EXCLUIDO
SELECT failure_id, data -> 'attributes' ->> 'state' AS state
FROM carmel.infraspeak_raw_failures
WHERE data -> 'attributes' ->> 'state' = 'EXCLUIDO';

-- Failures PAUSED (candidatas a verificação)
SELECT failure_id FROM carmel.infraspeak_raw_failures
WHERE data -> 'attributes' ->> 'state' = 'PAUSED';

-- Failures sem detalhe extraído
SELECT failure_id FROM carmel.infraspeak_raw_failures
WHERE failure_id NOT IN (SELECT failure_id FROM carmel.infraspeak_raw_failure_details)
  AND (data -> 'attributes' ->> 'date')::date >= '2026-01-01';

-- Horas por operador em chamados (failures)
SELECT operator_id, SUM(horas_decimais) AS total_horas
FROM carmel.v_trabalho_analitico_operador_chamados
GROUP BY operator_id ORDER BY total_horas DESC;

-- Ocorrências concluídas por hotel
SELECT hotel, COUNT(*) FROM carmel.v_detalhe_ocorrencias
WHERE status = 'COMPLETED' GROUP BY hotel;
```

---

## 12. CONVENÇÕES E REGRAS DO PROJETO

- **Upsert sempre**: nunca INSERT puro — usar `ON CONFLICT DO UPDATE`
- **JSONB intacto**: nunca transformar dados nas tabelas raw; toda lógica fica nas views
- **Detail > Bulk**: as views preferem o dado do detalhe (mais completo) e usam o bulk como fallback via `UNION ALL ... WHERE NOT EXISTS`
- **State vs Status em failures**: na tabela bulk o campo é `state`; na tabela de detalhe o campo de workflow é `status` (ambos dentro de `attributes`)
- **EXCLUIDO**: valor customizado do ETL (não existe na API), aplicado quando o registro retorna 404
- **Retry 3x com backoff**: 2s → 4s → erro fatal + log
- **Throttling**: 60 req/min, respeitado via `Retry-After` no 429 + sleep de 0.4s entre chamadas bem-sucedidas
