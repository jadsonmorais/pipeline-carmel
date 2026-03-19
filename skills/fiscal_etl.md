# Skill: Fiscal ETL (CMERP API)

## Objetivo

Extrair lançamentos fiscais do sistema CMERP via API HTTP e persistir em `fiscal_raw_lancamentos`. O sync diário usa janela deslizante de 7 dias para cobrir retificações e notas tardias.

---

## Estrutura do Módulo

```
etls/fiscal/
  __init__.py       — vazio
  api.py            — cliente HTTP para a API CMERP
  sync.py           — entry point (últimos 7 dias por padrão)
  history_sync.py   — backfill por intervalo de datas (chunks de 30 dias)
```

---

## API

**Endpoint**: `GET https://carmel-api.cmerp.com.br/Global.API/ExportarDados/Executar`

**Query params fixos**:
```
idExportacao=80
compactarDados=false
retornarComoAnexo=false
parametros=[...]    ← array JSON com DataIni, DataFim, IdEmpresa
```

**Headers de autenticação**:
```
Accept: application/json
x-cmflex-client-id:     <CLIENT-ID>
x-cmflex-client-secret: <CLIENT-SECRET>
x-cmflex-empresaId:     1       ← fixo (contexto da requisição)
x-cmflex-usuarioId:     <USUARIO-ID>
```

**Parâmetros (array JSON no query param `parametros`)**:
```json
[
  {"Nome": "DataIni",    "TipoDado": "DateTime", "ValorDateTime": "2026-03-01T00:00:00"},
  {"Nome": "DataFim",    "TipoDado": "DateTime", "ValorDateTime": "2026-03-07T23:59:59"},
  {"Nome": "IdEmpresa",  "TipoDado": "String",   "ValorString":   "1,2,3,4,8"}
]
```

**Resposta**: array JSON plano, um objeto por lançamento.

---

## Variáveis de Ambiente

```
CLIENT-ID=<valor>
CLIENT-SECRET=<valor>
EMPRESA-IDS=1,2,3,4,8
USUARIO-ID=1
```

---

## Execução

```bash
# Sync padrão (últimos 7 dias)
python -m etls.fiscal.sync

# Período específico
python -m etls.fiscal.sync 2026-03-10 2026-03-16

# Backfill histórico (chunks de 30 dias automáticos)
python -m etls.fiscal.history_sync 2025-01-01 2026-03-16
```

---

## Fluxo

1. `api.fetch_lancamentos(date_ini, date_fim)` → requisição GET com `parametros` no query string
2. Resposta: lista de dicts — cada item é um lançamento com `IDLANCAMENTOICMSBASE` como PK
3. `_to_records()` adiciona campo `id = str(IDLANCAMENTOICMSBASE)` (necessário para o upsert)
4. `utils.upsert_raw_data('fiscal_raw_lancamentos', 'lancamento_id', records, 'lancamento')`
5. `utils.refresh_mv_fiscal()` → `REFRESH MATERIALIZED VIEW carmel.mv_fiscal_lancamentos`

---

## Tratamento de Erros

- `resp.raise_for_status()` — lança exceção em 4xx/5xx
- `history_sync`: erros por chunk são logados e o processo continua para o próximo chunk

---

## Dependência

`requests` — já em `requirements.txt`
