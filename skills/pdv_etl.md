# Skill: PDV Simphony — Pipeline ETL

> Referência do pipeline ETL do PDV: estrutura de módulos, fluxos de execução, comandos e padrões.
> Use este documento para entender como os arquivos JSON diários do Simphony são extraídos e carregados.

---

## Estrutura de Módulos

```
infraspeak/
└── etls/
    └── pdv/
        ├── __init__.py         ← vazio (módulo Python)
        ├── sftp.py             ← cliente SFTP (context manager, paramiko)
        ├── parser.py           ← parse do JSON diário, extrai registros FISID
        ├── sync.py             ← sync de um único dia (entry point diário)
        └── history_sync.py     ← backfill por intervalo de datas
```

**Dependência entre módulos**:
```
shared/db.py  ←  etls/pdv/sftp.py
                 etls/pdv/parser.py  ←  sync.py
                                        history_sync.py
```

---

## Comandos de Execução

Sempre executar como módulo Python a partir da **raiz do projeto**:

```bash
# Sync diário (default = ontem)
python -m etls.pdv.sync

# Sync de uma data específica
python -m etls.pdv.sync 2026-02-19

# Backfill histórico (intervalo)
python -m etls.pdv.history_sync 2026-02-01 2026-02-28
```

---

## Fluxo: Sync Diário (`sync.py`)

Executado diariamente. Por padrão sincroniza o dia anterior (arquivos são gerados na madrugada seguinte ao dia de negócio).

```
run(date_str=None)
│
├── date_str = ontem (default) ou argumento CLI
│
└── SFTPClient (context manager — uma conexão para tudo)
    ├── list_files_for_date(date_str)
    │     → lista arquivos no PATH que contêm a data e terminam em .json
    │     → ex: ["CumbucoCFB_2026-02-19.json", "TaibaCFB_2026-02-19.json", ...]
    │
    └── Para cada arquivo:
          download_content(filename)
          → parser.parse_file(content, filename)
                → retorna lista de dicts, um por registro FISID
                → cada dict tem id = Invoice Data Info 8 (chave NF-e 44 dígitos)
          → upsert_raw_data('pdv_raw_notas', 'nota_id', records, 'nota')
```

---

## Fluxo: Backfill Histórico (`history_sync.py`)

Para carregar períodos grandes. Abre **uma única conexão SFTP** para todo o intervalo.

```
run_historical_sync(date_start, date_end)
│
└── SFTPClient (uma conexão para todo o intervalo)
    └── Para cada dia no intervalo:
          list_files_for_date(dia)
          → se nenhum arquivo: registra em dias_sem_arquivo, continua
          → para cada arquivo:
                download_content → parse_file → upsert
          → erros não param o loop (try/except com continue)
│
└── Resumo final: total de notas + lista de dias sem arquivo
```

> Diferença do Infraspeak: aqui a conexão SFTP é aberta **uma única vez** para todo o intervalo — mais eficiente e evita reconexões por dia.

---

## Estrutura do Arquivo JSON do PDV

Nome do arquivo: `{Hotel}CFB_{YYYY-MM-DD}.json`

Exemplos:
- `CumbucoCFB_2026-02-19.json`
- `TaibaCFB_2026-02-19.json`
- `CharmeCFB_2026-02-19.json`
- `MagnaCFB_2026-02-19.json`

Estrutura interna (JSON array de 4 seções):

```
[
  [ { Record Type: "FIS", Store Number, Store Name, First Business Date, ... } ],  ← seção 0: header
  [ {} ],                                                                            ← seção 1: vazia
  [ { Record Type: "FISID", ... }, { ... }, ... ],                                 ← seção 2: notas
  [ {} ]                                                                            ← seção 3: vazia
]
```

O parser usa `data[0][0]` para o header e `data[2]` para as notas.

---

## Mapeamento Store Number → Hotel

| Store Number (no arquivo) | Hotel canônico (no banco) |
|--------------------------|--------------------------|
| `CUMBUCO` | CUMBUCO |
| `TAIBA` | TAIBA |
| `CARM` | CHARME |
| `MAGN` | MAGNA |

Definido em `parser.py → STORE_TO_HOTEL`.

---

## Campos Injetados pelo Parser

O parser adiciona campos extras ao topo do JSONB antes de persistir:

| Campo adicionado | Valor | Origem |
|-----------------|-------|--------|
| `id` | chave NF-e 44 dígitos | `Invoice Data Info 8` |
| `hotel` | nome canônico | `Store Number` via `STORE_TO_HOTEL` |
| `source_file` | nome do arquivo | argumento `filename` |

Todos os campos originais do FISID são preservados integralmente.

---

## Cliente SFTP (`sftp.py`)

Usa `paramiko`. Implementado como context manager para garantir fechamento da conexão.

```python
with sftp_module.SFTPClient() as client:
    files = client.list_files_for_date('2026-02-19')
    content = client.download_content('CumbucoCFB_2026-02-19.json')
```

Métodos disponíveis:
- `list_files_for_date(date_str)` → lista arquivos na pasta remota que contêm a data
- `download_content(filename)` → baixa e decodifica o arquivo como string UTF-8

---

## Variáveis de Ambiente (`.env`)

```env
PDV_SFTP_HOST=<ip_do_servidor_pdv>
PDV_SFTP_USER=<usuario_sftp>
PDV_SFTP_PASS=<senha_sftp>
PDV_SFTP_PORT=22                          # opcional, default 22
PDV_SFTP_PATH=/d01/carmel_sftp/arquivos   # opcional, já é o default
```

Arquivo: `auth/prod/.env`. Carregado automaticamente ao importar `etls/pdv/sftp.py`.

---

## Tratamento de Erros

| Situação | Comportamento |
|----------|--------------|
| Dia sem arquivo no SFTP | Log de aviso, registra em `dias_sem_arquivo`, continua |
| Registro sem `Invoice Data Info 8` | Ignorado pelo parser (sem chave NF-e não há PK) |
| Erro durante download de um dia | Log do erro, `continue` para o próximo dia |
| Falha na conexão SFTP | Exceção propagada (não silenciada) |

---

## Padrão de Upsert

```python
utils.upsert_raw_data(
    table_name='pdv_raw_notas',
    id_column='nota_id',
    data_list=records,   # lista de dicts com campo 'id' = chave NF-e
    type='nota'          # → coluna nota_id na tabela
)
```
