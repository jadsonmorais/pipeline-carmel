# Skill: Infraspeak — Pipeline ETL

> Referência do pipeline ETL: estrutura de módulos, fluxos de execução, comandos e tratamento de erros.
> Use este documento para entender como os dados são extraídos, transformados e carregados.

---

## Estrutura de Módulos

```
infraspeak/
├── shared/
│   └── db.py               ← funções de banco compartilhadas por todos os ETLs
│
└── etls/
    └── infraspeak/
        ├── api.py           ← cliente HTTP + RouteManager (queries prontas)
        ├── extractor.py     ← loop de extração detalhada com retry
        ├── sync.py          ← sync incremental (entry point diário)
        ├── history_sync.py  ← backfill histórico por intervalo de datas
        ├── repescagem.py    ← reprocessa IDs de arquivos CSV
        └── validador_api.py ← testa validade de filtros JQL
```

**Dependência entre módulos**:
```
shared/db.py  ←  etls/infraspeak/api.py
                 etls/infraspeak/extractor.py  ←  sync.py
                                                   history_sync.py
                                                   repescagem.py
```

---

## Comandos de Execução

Sempre executar como módulo Python a partir da **raiz do projeto**:

```bash
# Sync incremental (últimos 3 dias)
python -m etls.infraspeak.sync

# Backfill histórico
python -m etls.infraspeak.history_sync 2024-01-01 2024-01-31
python -m etls.infraspeak.history_sync 2024-01-01 2024-01-31 true   # com event registries

# Repescagem de IDs de CSVs
python -m etls.infraspeak.repescagem

# Validar filtros JQL da API
python -m etls.infraspeak.validador_api
```

**Agendamento automático**: `auto.bat` via Agendador de Tarefas do Windows.

---

## Fluxo: Sync Incremental (`sync.py`)

Executado diariamente. Janela de 3 dias para capturar edições retroativas.

```
run_incremental_sync(days_back=3)
│
├── 1. FAILURES bulk
│      filter: date_min_last_status_change_date (3 dias)
│      → upsert infraspeak_raw_failures
│      → sync_details → upsert infraspeak_raw_failure_details
│
├── 2. WORKS bulk
│      filter: date_min_updated_at (3 dias)
│      → upsert infraspeak_raw_works
│      → sync_details (sem events) → upsert infraspeak_raw_work_details
│
├── 3. SCHEDULED WORKS bulk
│      filter: date_min_updated_at (3 dias)
│      → upsert infraspeak_raw_scheduled_works
│      → sync_details → upsert infraspeak_raw_scheduled_work_details
│
├── 4. OPERATORS (dump completo, sem filtro de data)
│      → upsert infraspeak_raw_operators
│
└── 5. VALIDAÇÃO DE FAILURES PAUSED
       → get_failure_ids_by_state('PAUSED')
       → sync_details para cada ID
       → se 404: mark_failure_as_deleted(id)
```

---

## Fluxo: Backfill Histórico (`history_sync.py`)

Para carregar períodos grandes. Processa dia a dia para respeitar limites da API.

```
run_historical_sync(date_start, date_end, include_records=False)
│
└── Para cada dia no intervalo:
    ├── FAILURES bulk (por date_min/max_last_status_change_date)
    │     → upsert infraspeak_raw_failures
    │     → se include_records=True: sync_details
    │
    ├── WORKS bulk (por date_min/max_updated_at)
    │     → upsert infraspeak_raw_works
    │     → se include_records=True: sync_details (sem events)
    │
    └── SCHEDULED WORKS bulk (por date_min/max_start_date)
          → upsert infraspeak_raw_scheduled_works
          → se include_records=True: sync_details
```

> `include_records=False` é mais rápido — pula a extração individual de detalhes.
> Usar `true` quando precisar dos event registries para cálculo de horas.

---

## Fluxo: Extração Detalhada (`extractor.py → sync_details`)

Para cada ID da lista, faz uma requisição individual com retry:

```
sync_details(ids, resource_type, include_records)
│
└── Para cada ID:
    ├── Tentativa 1: request(endpoint/{id}, expanded=...)
    │     → sucesso: upsert_raw_data + sleep(0.4s) → próximo ID
    │     → falha: sleep(2s)
    │
    ├── Tentativa 2: retry
    │     → sucesso: upsert_raw_data
    │     → falha: sleep(4s)
    │
    └── Tentativa 3 (final):
          → sucesso: upsert_raw_data
          → falha DEFINITIVA:
              - loga em ids_perdidos_detalhe.log
              - se resource_type=='failure' e '404' in erro:
                  mark_failure_as_deleted(id)  ← marca EXCLUIDO no banco
```

---

## Tratamento de 404 — Failure Excluída

Quando uma failure não existe mais na API (foi deletada no ERP):

1. `sync_details` recebe 404 após 3 tentativas
2. Loga em `etls/infraspeak/ids_perdidos_detalhe.log`:
   ```
   2026-03-16 16:30:41 | failure | ID: 16667285 | Erro: 404 Client Error: Not Found...
   ```
3. Chama `shared.db.mark_failure_as_deleted(id)`:
   - `infraspeak_raw_failures`: `jsonb_set(data, '{attributes,state}', '"EXCLUIDO"')`
   - `infraspeak_raw_failure_details`: `jsonb_set(data, '{data,attributes,status}', '"EXCLUIDO"')`

---

## Fluxo: Repescagem (`repescagem.py`)

Para reprocessar IDs que falharam anteriormente. Lê arquivos CSV na raiz:

| Arquivo CSV | Tipo | include_records |
|-------------|------|-----------------|
| `repescagem_failures.csv` | failure | True |
| `repescagem_works.csv` | work | False |
| `repescagem_scheduled.csv` | scheduled_work | True |

- IDs são limpos de caracteres não numéricos (regex)
- Duplicatas são removidas
- Cada arquivo é opcional — se não existir, é ignorado silenciosamente

---

## Padrão de Upsert

Nunca INSERT direto — sempre `upsert_raw_data` de `shared/db.py`:

```python
utils.upsert_raw_data(
    table_name='infraspeak_raw_failures',
    id_column='failure_id',
    data_list=[dict_com_campo_id],
    type='failure'
)
```

O campo `id` deve estar na raiz do dicionário (o ETL injeta via `response['id'] = response['data']['id']`).

---

## Adicionando uma Nova ETL

1. Criar pasta `etls/<fonte>/` com `__init__.py`
2. Criar `etls/<fonte>/api.py` com o cliente HTTP específico
3. Criar `etls/<fonte>/sync.py` importando `shared.db`:
   ```python
   from shared import db as utils
   # usar utils.upsert_raw_data(), utils.get_db_connection(), etc.
   ```
4. Criar as tabelas no banco (schema `carmel`) via `db/build.sql`
5. Executar com `python -m etls.<fonte>.sync`

---

## Variáveis de Ambiente (`.env`)

```env
# API Infraspeak
API_DATA_USER=usuario@empresa.com
API_DATA_TOKEN=seu_personal_access_token

# Banco de Dados PostgreSQL
DB_HOST=10.197.3.2
DB_NAME=carmel
DB_USER=usuario_db
DB_PASS=senha_db
DB_PORT=5432
```

Arquivo localizado em `auth/prod/.env`. Carregado automaticamente ao importar `shared.db`.
