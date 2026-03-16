# Plataforma de Dados — Carmel Hotéis

Pipeline ETL unificado para extração, armazenamento e análise de dados operacionais da rede **Carmel Hotéis**. O sistema conecta múltiplas fontes de dados em um único banco PostgreSQL, permitindo cruzar informações de manutenção, reservas, marketing e outras áreas para tomada de decisão integrada.

---

## Arquitetura

```
APIs Externas                ETL (Python)              Banco de Dados           Consumo
─────────────────            ─────────────────         ──────────────────       ──────────
                             shared/db.py
Infraspeak API v3  ──────►  etls/infraspeak/  ──────►  PostgreSQL               Power BI
                             sync.py                    schema: carmel           Flask Intranet
Futuras fontes     ──────►  etls/<fonte>/     ──────►  host: 10.197.3.2         Agentes IA
                             sync.py                    Views Analíticas
```

**Princípio**: dados brutos armazenados integralmente como JSONB (sem transformação nas tabelas raw). Toda lógica de negócio fica em views SQL reutilizáveis por qualquer ferramenta de consumo.

---

## Estrutura de Pastas

```
infraspeak/
│
├── shared/
│   └── db.py                    ← conexão PostgreSQL + funções de upsert (compartilhadas)
│
├── etls/
│   └── infraspeak/              ← ETL da API Infraspeak v3
│       ├── api.py               ← cliente HTTP com throttling e paginação
│       ├── extractor.py         ← extração detalhada com retry (3x backoff)
│       ├── sync.py              ← sync incremental diário (entry point)
│       ├── history_sync.py      ← backfill histórico por intervalo de datas
│       ├── repescagem.py        ← reprocessa IDs falhos de arquivos CSV
│       └── validador_api.py     ← testa validade de filtros JQL
│
├── skills/
│   ├── infraspeak_db.md         ← referência do banco: tabelas, views, JSONB, queries
│   ├── infraspeak_api.md        ← referência da API: endpoints, filtros, expansões
│   └── infraspeak_etl.md        ← referência do pipeline: fluxos, comandos, erros
│
├── db/
│   └── build.sql                ← DDL completo: tabelas, views, schema carmel
│
├── auth/
│   ├── prod/.env                ← credenciais de produção (não commitar)
│   └── test/.env                ← credenciais de teste
│
├── apidocs/                     ← snapshots da documentação oficial da API
├── examples/                    ← exemplos reais de payloads JSON da API
├── auto.bat                     ← agendamento via Agendador de Tarefas Windows
├── requirements.txt
└── CLAUDE.md                    ← guia para agentes IA trabalharem neste projeto
```

---

## Setup

### Pré-requisitos
- Python 3.10+
- PostgreSQL 12+ com schema `carmel` criado via `db/build.sql`
- Acesso à rede interna (banco em `10.197.3.2`)

### Instalação

```bash
# 1. Criar e ativar ambiente virtual
python -m venv venv
venv\Scripts\activate        # Windows

# 2. Instalar dependências
pip install -r requirements.txt

# 3. Configurar credenciais
# Editar auth/prod/.env com os valores reais:
```

```env
API_DATA_USER=usuario@empresa.com
API_DATA_TOKEN=seu_personal_access_token
DB_HOST=10.197.3.2
DB_NAME=carmel
DB_USER=usuario_db
DB_PASS=senha_db
DB_PORT=5432
```

```bash
# 4. Criar o schema no banco (primeira vez)
psql -h 10.197.3.2 -U usuario_db -d carmel -f db/build.sql
```

---

## Executando

Sempre a partir da **raiz do projeto** (`infraspeak/`):

```bash
# Sync incremental (últimos 3 dias) — rodar diariamente
python -m etls.infraspeak.sync

# Backfill histórico
python -m etls.infraspeak.history_sync 2024-01-01 2024-12-31
python -m etls.infraspeak.history_sync 2024-01-01 2024-12-31 true   # inclui event registries

# Reprocessar IDs que falharam (colocar IDs em repescagem_failures.csv)
python -m etls.infraspeak.repescagem

# Validar filtros JQL da API
python -m etls.infraspeak.validador_api
```

### Agendamento Automático
`auto.bat` está configurado para uso com o **Agendador de Tarefas do Windows**. Requer execução como Administrador.

---

## Banco de Dados

### Tabelas Raw (Bronze)

| Tabela | Descrição |
|--------|-----------|
| `carmel.infraspeak_raw_failures` | Chamados de manutenção corretiva |
| `carmel.infraspeak_raw_failure_details` | Detalhe com eventos e log de ações |
| `carmel.infraspeak_raw_works` | Planos mestres de manutenção preventiva |
| `carmel.infraspeak_raw_work_details` | Detalhe de plano mestre |
| `carmel.infraspeak_raw_scheduled_works` | Ocorrências preventivas agendadas |
| `carmel.infraspeak_raw_scheduled_work_details` | Detalhe de ocorrência com eventos |
| `carmel.infraspeak_raw_operators` | Técnicos/operadores |

### Views Analíticas (Prata/Ouro)

| View | Descrição |
|------|-----------|
| `carmel.v_operadores` | Dimensão de técnicos |
| `carmel.v_detalhe_planos_manutencao` | Dimensão de planos preventivos |
| `carmel.v_detalhe_ocorrencias` | Fato de ocorrências com hotel e plano |
| `carmel.v_trabalho_analitico_operador_chamados` | Horas trabalhadas por operador em chamados |
| `carmel.v_trabalho_analitico_operador_ocorrencias` | Horas trabalhadas por operador em preventivas |

---

## Tratamento de Erros

| Erro | Tratamento |
|------|------------|
| **429 Rate Limit** | Aguarda `Retry-After` segundos e retenta automaticamente |
| **5xx Instabilidade** | Retry 3x com backoff exponencial (2s → 4s) |
| **404 Registro deletado** | Loga em `ids_perdidos_detalhe.log` e marca `state = EXCLUIDO` no banco |
| **Failures PAUSED** | Verificação automática a cada sync — se 404, marca como EXCLUIDO |

---

## Adicionando uma Nova ETL

```bash
# 1. Criar pasta do novo ETL
mkdir etls/nome_da_fonte
touch etls/nome_da_fonte/__init__.py
```

```python
# 2. etls/nome_da_fonte/api.py — cliente específico desta fonte

# 3. etls/nome_da_fonte/sync.py — importar shared.db
from shared import db as utils

utils.upsert_raw_data('nome_da_fonte_raw_tabela', 'id_column', data, 'tipo')
```

```sql
-- 4. db/build.sql — adicionar tabela no schema carmel
CREATE TABLE IF NOT EXISTS carmel.nome_da_fonte_raw_tabela (
    id VARCHAR(255) PRIMARY KEY,
    data JSONB NOT NULL,
    extracted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
```

---

## Referência Completa

Para detalhes aprofundados, consulte os documentos em `skills/`:

- [skills/infraspeak_db.md](skills/infraspeak_db.md) — banco de dados, JSONB, queries
- [skills/infraspeak_api.md](skills/infraspeak_api.md) — API, endpoints, rate limit
- [skills/infraspeak_etl.md](skills/infraspeak_etl.md) — pipeline, fluxos, comandos

---

*Projeto mantido como infraestrutura de dados da Carmel Hotéis para gestão operacional integrada.*
