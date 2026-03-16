# Skill: Infraspeak — Banco de Dados (PostgreSQL)

> Referência completa do schema, tabelas, estrutura JSONB e views analíticas.
> Use este documento para escrever queries, entender paths de campos ou modificar o banco.

---

## Conexão e Schema

- **Host de produção**: `10.197.3.2`
- **Banco**: `carmel` (variável `DB_NAME`)
- **Schema**: `carmel`
- **Credenciais**: variáveis de ambiente em `auth/prod/.env`
  ```
  DB_HOST, DB_NAME, DB_USER, DB_PASS, DB_PORT
  ```
- **Conexão Python**: `shared/db.py → get_db_connection()`

---

## Tabelas Raw (Camada Bronze)

Todas no schema `carmel`. Dados armazenados como JSONB completo (resposta bruta da API).

| Tabela | PK | Conteúdo |
|--------|----|----------|
| `infraspeak_raw_failures` | `failure_id VARCHAR` | Bulk de chamados (lista paginada) |
| `infraspeak_raw_failure_details` | `failure_id VARCHAR` | Detalhe individual + events/registries |
| `infraspeak_raw_works` | `work_id VARCHAR` | Bulk de planos mestres |
| `infraspeak_raw_work_details` | `work_id VARCHAR` | Detalhe de plano mestre |
| `infraspeak_raw_scheduled_works` | `scheduled_work_id VARCHAR` | Bulk de ocorrências preventivas |
| `infraspeak_raw_scheduled_work_details` | `scheduled_work_id VARCHAR` | Detalhe de ocorrência + events |
| `infraspeak_raw_operators` | `operator_id VARCHAR` | Dump completo de operadores |

Todas possuem também: `extracted_at TIMESTAMP WITH TIME ZONE`

---

## Estrutura JSONB — Diferença Crítica: Bulk vs Detalhe

### Tabelas BULK (ex: `infraspeak_raw_failures`)
A API retorna cada item diretamente:
```
data ->> 'id'                              → ID
data -> 'attributes' ->> 'state'           → estado (OPEN, PAUSED, CLOSED, EXCLUIDO...)
data -> 'attributes' ->> 'status'          → status de workflow
data -> 'attributes' ->> 'date'            → data de abertura
data -> 'attributes' ->> 'updated_at'      → última atualização
```

### Tabelas DETALHE (ex: `infraspeak_raw_failure_details`)
A API retorna um envelope `{ data: {...}, included: [...] }` — há um nível extra `data`:
```
data -> 'data' ->> 'id'                            → ID
data -> 'data' -> 'attributes' ->> 'state'         → estado textual/livre
data -> 'data' -> 'attributes' ->> 'status'        → status do workflow  ← campo correto para falhas
data -> 'data' -> 'relationships'                  → relacionamentos
data -> 'included'                                 → array com location, operator, events, event_registry...
```

> **REGRA**: Em failures, o campo de status de workflow na tabela de detalhes é `status`, não `state`.

---

## Campos state / status em Failures

| Tabela | Campo | Path JSONB | Valores da API |
|--------|-------|------------|----------------|
| `infraspeak_raw_failures` | `state` | `data -> 'attributes' ->> 'state'` | OPEN, PAUSED, IN_PROGRESS, ON_HOLD, CLOSED, ASSIGNED |
| `infraspeak_raw_failure_details` | `status` | `data -> 'data' -> 'attributes' ->> 'status'` | idem |

**Valor customizado do ETL** (não existe na API):
- `EXCLUIDO` — aplicado quando o registro retorna 404 (foi deletado no ERP)

### Como marcar como EXCLUIDO
```python
# Python
from shared import db
db.mark_failure_as_deleted(failure_id)

# SQL equivalente
UPDATE carmel.infraspeak_raw_failures
SET data = jsonb_set(data, '{attributes,state}', '"EXCLUIDO"'),
    extracted_at = CURRENT_TIMESTAMP
WHERE failure_id = %s;

UPDATE carmel.infraspeak_raw_failure_details
SET data = jsonb_set(data, '{data,attributes,status}', '"EXCLUIDO"'),
    extracted_at = CURRENT_TIMESTAMP
WHERE failure_id = %s;
```

---

## Views Analíticas (Camada Prata/Ouro)

### `carmel.v_operadores`
Dimensão de operadores/técnicos.
- Fonte: `infraspeak_raw_operators`
- Campos: `operator_id`, `nome_operador`, `email`, `custo_por_hora`, `entity_id`

### `carmel.v_detalhe_planos_manutencao`
Dimensão de planos mestres de manutenção preventiva.
- Fonte: `infraspeak_raw_work_details` UNION `infraspeak_raw_works`
- Lógica: prefere o detalhe quando disponível (UNION ALL + WHERE NOT EXISTS)
- Campos: `work_id`, `nome_plano`, `tipo_plano`, `periodicidade`, `hotel`, `local_full_name`

### `carmel.v_detalhe_ocorrencias`
Fato de ocorrências preventivas.
- Fonte: `infraspeak_raw_scheduled_work_details` UNION `infraspeak_raw_scheduled_works`
- JOIN com `v_detalhe_planos_manutencao` para nome do plano e localização
- Campos: `scheduled_work_id`, `work_id`, `status`, `data_agendada`, `data_conclusao`, `hotel`, `nome_plano`, `percentual_conclusao`

### `carmel.v_trabalho_analitico_operador_chamados`
Apontamento de horas em chamados (failures).
- Fonte: `infraspeak_raw_failure_details` → array `included` (type = `event` + `event_registry`)
- Lógica: pares STARTED→PAUSED e RESUMED→COMPLETED definem janelas de trabalho
- Campos: `failure_id`, `operator_id`, `data_inicio`, `data_fim`, `duracao`, `horas_decimais`

### `carmel.v_trabalho_analitico_operador_ocorrencias`
Apontamento de horas em ocorrências preventivas.
- Fonte: `infraspeak_raw_scheduled_work_details` → array `included`
- Mesma lógica de pares de eventos
- Campos: `scheduled_work_id`, `operator_id`, `data_inicio`, `data_fim`, `duracao`, `horas_decimais`

---

## Mapeamento de Hotéis

As views calculam o campo `hotel` com base em `local_full_name`:

| Padrão | Hotel |
|--------|-------|
| `CARMEL CUMBUCO%` | CUMBUCO |
| `CARMEL TAÍBA%` | TAÍBA |
| `CARMEL CHARME%` | CHARME |
| `MAGNA PRAIA HOTEL%` | MAGNA |
| Outros | Primeiro segmento antes do `-` |

---

## Funções Python em `shared/db.py`

```python
upsert_raw_data(table_name, id_column, data_list, type)
# Insere ou atualiza registros. Em conflito de PK, sobrescreve data e extracted_at.

get_failure_ids_by_state(state)
# Retorna lista de failure_ids com determinado state na tabela bulk.
# Ex: get_failure_ids_by_state('PAUSED')

mark_failure_as_deleted(failure_id)
# Atualiza state/status para EXCLUIDO em ambas as tabelas (raw e details).
```

---

## Queries de Auditoria

```sql
-- Failures marcadas como EXCLUIDO
SELECT failure_id, data -> 'attributes' ->> 'state' AS state
FROM carmel.infraspeak_raw_failures
WHERE data -> 'attributes' ->> 'state' = 'EXCLUIDO';

-- Failures PAUSED (candidatas a verificação de exclusão)
SELECT failure_id FROM carmel.infraspeak_raw_failures
WHERE data -> 'attributes' ->> 'state' = 'PAUSED';

-- Failures sem detalhe extraído (criadas >= 2026)
SELECT failure_id FROM carmel.infraspeak_raw_failures
WHERE failure_id NOT IN (SELECT failure_id FROM carmel.infraspeak_raw_failure_details)
  AND (data -> 'attributes' ->> 'date')::date >= '2026-01-01';

-- Ocorrências concluídas sem detalhe (>= 2026)
SELECT scheduled_work_id FROM carmel.infraspeak_raw_scheduled_works
WHERE scheduled_work_id NOT IN (SELECT scheduled_work_id FROM carmel.infraspeak_raw_scheduled_work_details)
  AND UPPER(data -> 'attributes' ->> 'state') = 'COMPLETED'
  AND (data -> 'attributes' ->> 'completed_date')::date >= '2026-01-01';

-- Horas por operador em chamados
SELECT operator_id, SUM(horas_decimais) AS total_horas
FROM carmel.v_trabalho_analitico_operador_chamados
GROUP BY operator_id ORDER BY total_horas DESC;

-- Ocorrências concluídas por hotel
SELECT hotel, COUNT(*) FROM carmel.v_detalhe_ocorrencias
WHERE status = 'COMPLETED' GROUP BY hotel;
```
