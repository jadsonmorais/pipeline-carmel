# Plataforma de Dados — Carmel Hotéis

Pipeline ETL unificado para extração, armazenamento e análise de dados operacionais da rede **Carmel Hotéis**. O sistema conecta múltiplas fontes de dados em um único banco PostgreSQL, permitindo cruzar informações de manutenção, fiscal, reservas e outras áreas para tomada de decisão integrada.

---

## Arquitetura

```
Fontes                       ETL (Python)              Banco de Dados           Consumo
─────────────────            ─────────────────         ──────────────────       ──────────
                             shared/db.py
Infraspeak API v3  ──────►  etls/infraspeak/  ──────►  PostgreSQL               Power BI
                             sync.py                    schema: carmel           Flask Intranet
PDV Simphony       ──────►  etls/pdv/         ──────►  host: 10.197.3.2         Agentes IA
(SFTP JSON diário)           sync.py                    Views Analíticas
                                                         v_pdv_notas
NF-e XMLs          ──────►  etls/nfe/         ──────►  nfe_raw_xmls
(SMB \\10.197.0.51)          sync.py                    (PK = nota_id, join c/ PDV)

FISCAL API         ──────►  etls/fiscal/      ──────►  (em implementação)
(em implementação)

SEFAZ NF-e         ──────►  etls/sefaz/       ──────►  (em implementação)
(share rede XML)
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
│   ├── infraspeak/              ← ETL da API Infraspeak v3
│   │   ├── api.py               ← cliente HTTP com throttling e paginação
│   │   ├── extractor.py         ← extração detalhada com retry (3x backoff)
│   │   ├── sync.py              ← sync incremental diário (entry point)
│   │   ├── history_sync.py      ← backfill histórico por intervalo de datas
│   │   ├── repescagem.py        ← reprocessa IDs falhos de arquivos CSV
│   │   └── validador_api.py     ← testa validade de filtros JQL
│   │
│   ├── pdv/                     ← ETL PDV Simphony (arquivos JSON via SFTP)
│   │   ├── sftp.py              ← cliente SFTP (context manager, paramiko)
│   │   ├── parser.py            ← parse dos JSONs diários, extrai registros FISID
│   │   ├── sync.py              ← sync diário (entry point), default = ontem
│   │   └── history_sync.py      ← backfill por intervalo de datas
│   │
│   ├── nfe/                     ← ETL NF-e XMLs (shares SMB \\10.197.0.51\{Hotel})
│   │   ├── smb_client.py        ← cliente SMB (lista e lê XMLs das pastas compartilhadas)
│   │   ├── parser.py            ← parser XML NF-e/NFC-e, extrai campos para JSONB
│   │   └── sync.py              ← sync completo dos 4 shares (entry point)
│   │
│   ├── fiscal/                  ← ETL API Fiscal (em implementação)
│   │   ├── api.py               ← cliente HTTP (stub)
│   │   └── sync.py              ← entry point (stub)
│   │
│   └── sefaz/                   ← ETL NF-e SEFAZ (em implementação)
│       ├── parser.py            ← parser XML NF-e/NFC-e
│       └── sync.py              ← entry point
│
├── skills/
│   ├── infraspeak_db.md         ← referência do banco Infraspeak: tabelas, views, JSONB, queries
│   ├── infraspeak_api.md        ← referência da API Infraspeak: endpoints, filtros, expansões
│   ├── infraspeak_etl.md        ← referência do pipeline Infraspeak: fluxos, comandos, erros
│   ├── pdv_db.md                ← referência do banco PDV: tabela, JSONB paths, views, queries
│   ├── pdv_etl.md               ← referência do pipeline PDV: SFTP, estrutura JSON, comandos
│   ├── nfe_db.md                ← referência da tabela nfe_raw_xmls, queries de conciliação
│   └── nfe_etl.md               ← referência do ETL NF-e: shares SMB, parser XML, comandos
│
├── db/
│   └── build.sql                ← DDL completo: tabelas, views, schema carmel
│
├── auth/
│   ├── prod/.env                ← credenciais de produção (não commitar)
│   └── test/.env                ← credenciais de teste
│
├── examples/                    ← exemplos reais de payloads (JSON PDV, API Infraspeak)
├── apidocs/                     ← snapshots da documentação oficial da API
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
- Acesso SFTP ao servidor PDV (`PDV_SFTP_HOST`)
- Acesso SMB ao servidor de XMLs (`NFE_SMB_HOST=10.197.0.51`)

### Instalação

```bash
# 1. Criar e ativar ambiente virtual
python -m venv venv
venv\Scripts\activate        # Windows

# 2. Instalar dependências
pip install -r requirements.txt

# 3. Configurar credenciais
# Editar auth/prod/.env com os valores reais
```

```env
# Banco de dados
DB_HOST=10.197.3.2
DB_NAME=carmel
DB_USER=usuario_db
DB_PASS=senha_db
DB_PORT=5432

# Infraspeak API
API_DATA_USER=usuario@empresa.com
API_DATA_TOKEN=seu_personal_access_token

# PDV Simphony (SFTP)
PDV_SFTP_HOST=<ip_do_servidor_pdv>
PDV_SFTP_USER=<usuario_sftp>
PDV_SFTP_PASS=<senha_sftp>
PDV_SFTP_PORT=22
PDV_SFTP_PATH=/d01/carmel_sftp/arquivos

# NF-e XMLs (SMB)
NFE_SMB_HOST=10.197.0.51
NFE_SMB_USER=<usuario_smb>
NFE_SMB_PASS=<senha_smb>
NFE_SMB_DOMAIN=
```

```bash
# 4. Criar o schema no banco (primeira vez)
psql -h 10.197.3.2 -U usuario_db -d carmel -f db/build.sql
```

---

## Executando

Sempre a partir da **raiz do projeto** (`infraspeak/`):

```bash
# Infraspeak — sync incremental (últimos 3 dias)
python -m etls.infraspeak.sync

# Infraspeak — backfill histórico
python -m etls.infraspeak.history_sync 2024-01-01 2024-12-31
python -m etls.infraspeak.history_sync 2024-01-01 2024-12-31 true   # inclui event registries

# Infraspeak — reprocessar IDs que falharam
python -m etls.infraspeak.repescagem

# PDV — sync diário (default = ontem)
python -m etls.pdv.sync

# PDV — data específica
python -m etls.pdv.sync 2026-02-19

# PDV — backfill de intervalo
python -m etls.pdv.history_sync 2026-02-01 2026-02-28

# NF-e XMLs — varredura completa dos 4 shares SMB
python -m etls.nfe.sync
```

### Agendamento Automático
`auto.bat` está configurado para uso com o **Agendador de Tarefas do Windows**. Requer execução como Administrador.

---

## Banco de Dados

### Tabelas Raw (Bronze)

| Tabela | Fonte | Descrição |
|--------|-------|-----------|
| `carmel.infraspeak_raw_failures` | Infraspeak | Chamados de manutenção corretiva |
| `carmel.infraspeak_raw_failure_details` | Infraspeak | Detalhe com eventos e log de ações |
| `carmel.infraspeak_raw_works` | Infraspeak | Planos mestres de manutenção preventiva |
| `carmel.infraspeak_raw_work_details` | Infraspeak | Detalhe de plano mestre |
| `carmel.infraspeak_raw_scheduled_works` | Infraspeak | Ocorrências preventivas agendadas |
| `carmel.infraspeak_raw_scheduled_work_details` | Infraspeak | Detalhe de ocorrência com eventos |
| `carmel.infraspeak_raw_operators` | Infraspeak | Técnicos/operadores |
| `carmel.pdv_raw_notas` | PDV Simphony | Notas fiscais por ponto de venda (PK = chave NF-e 44 dígitos) |
| `carmel.nfe_raw_xmls` | NF-e XMLs (SMB) | XMLs enviados ao fiscal por hotel (PK = chave NF-e 44 dígitos, join com PDV) |

### Views Analíticas (Prata/Ouro)

| View | Fonte | Descrição |
|------|-------|-----------|
| `carmel.v_operadores` | Infraspeak | Dimensão de técnicos |
| `carmel.v_detalhe_planos_manutencao` | Infraspeak | Dimensão de planos preventivos |
| `carmel.v_detalhe_ocorrencias` | Infraspeak | Fato de ocorrências com hotel e plano |
| `carmel.v_trabalho_analitico_operador_chamados` | Infraspeak | Horas trabalhadas por operador em chamados |
| `carmel.v_trabalho_analitico_operador_ocorrencias` | Infraspeak | Horas trabalhadas por operador em preventivas |
| `carmel.v_pdv_notas` | PDV | Notas fiscais com campos extraídos: hotel, data, valor, garçom, quarto, ponto de venda |

### Chave de Conciliação PDV ↔ NF-e ↔ SEFAZ

O campo `nota_id` é a chave NF-e de 44 dígitos, compartilhada por todas as fontes fiscais:

```sql
pdv_raw_notas.nota_id = nfe_raw_xmls.nota_id  -- JOIN já disponível
-- pdv_raw_notas.nota_id = sefaz_raw_notas.nota_id  -- quando SEFAZ for implementado
```

---

## Tratamento de Erros

| Erro | ETL | Tratamento |
|------|-----|------------|
| **429 Rate Limit** | Infraspeak | Aguarda `Retry-After` segundos e retenta automaticamente |
| **5xx Instabilidade** | Infraspeak | Retry 3x com backoff exponencial (2s → 4s) |
| **404 Registro deletado** | Infraspeak | Loga em `ids_perdidos_detalhe.log` e marca `state = EXCLUIDO` no banco |
| **Failures PAUSED** | Infraspeak | Verificação automática a cada sync — se 404, marca como EXCLUIDO |
| **Arquivo não encontrado** | PDV | Log de aviso, continua para os demais hotéis |

---

## Adicionando uma Nova ETL

```bash
# 1. Criar pasta do novo ETL
mkdir etls/nome_da_fonte
touch etls/nome_da_fonte/__init__.py
```

```python
# 2. etls/nome_da_fonte/sync.py
from shared import db as utils

def run():
    # ... lógica de extração ...
    utils.upsert_raw_data('nome_da_fonte_raw_tabela', 'tipo_id', data, 'tipo')

if __name__ == '__main__':
    run()
```

```sql
-- 3. db/build.sql — adicionar tabela no schema carmel
CREATE TABLE IF NOT EXISTS carmel.nome_da_fonte_raw_tabela (
    tipo_id VARCHAR(255) PRIMARY KEY,
    data JSONB NOT NULL,
    extracted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
```

```
-- 4. Atualizar CLAUDE.md e README.md com a nova fonte
```

---

## Referência Completa

Para detalhes aprofundados, consulte os documentos em `skills/`:

**Infraspeak**
- [skills/infraspeak_db.md](skills/infraspeak_db.md) — banco de dados, JSONB, queries
- [skills/infraspeak_api.md](skills/infraspeak_api.md) — API, endpoints, rate limit
- [skills/infraspeak_etl.md](skills/infraspeak_etl.md) — pipeline, fluxos, comandos

**PDV Simphony**
- [skills/pdv_db.md](skills/pdv_db.md) — tabela raw, JSONB paths, views, queries de conciliação
- [skills/pdv_etl.md](skills/pdv_etl.md) — SFTP, estrutura JSON Simphony, comandos sync e histórico

**NF-e XMLs (SMB)**
- [skills/nfe_db.md](skills/nfe_db.md) — tabela nfe_raw_xmls, JSONB paths, queries de conciliação com PDV
- [skills/nfe_etl.md](skills/nfe_etl.md) — shares SMB, parser XML, variáveis de ambiente, comandos

---

*Projeto mantido como infraestrutura de dados da Carmel Hotéis para gestão operacional integrada.*
