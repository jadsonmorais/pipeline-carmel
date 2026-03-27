# GCM ETL — Oracle Simphony Guest Check Management

## O que é

Extrai o relatório GCM (Guest Check Management) exportado diariamente pelo Oracle Simphony POS para cada hotel. Cada arquivo contém todos os itens de linha (line items) vendidos na data, incluindo hóspedes, consumo interno, eventos, etc.

---

## Estrutura de módulos

```
etls/gcm/
  __init__.py        — vazio
  sftp.py            — cliente SFTP (mesmas credenciais PDV_SFTP_*)
  parser.py          — parse do JSON GCM → lista de line items
  sync.py            — sincronização diária
  history_sync.py    — backfill por intervalo de datas
  cmflex_export.py   — gerador de XML CMFlex para Consumo Interno
```

---

## Estrutura do arquivo JSON GCM

```
Filename: {Hotel}GCM_{YYYY-MM-DD}.json
          Ex: CharmeGCM_2026-03-26.json

JSON:     [ [ item1, item2, item3, ... ] ]
          Os registros estão em data[0] (lista de dicts)
```

**Campos chave por registro:**

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `guestCheckLineItemID` | bigint | PK — identificador único do item |
| `guestCheckID` | bigint | Agrupa itens de uma mesma comanda |
| `checkNum` | int | Número da comanda no PDV |
| `locationRef` | string | Código do hotel: CARM, CUMBUCO, TAIBA, MAGN |
| `orderTypeName` | string | Tipo de venda: Hospede, Consumo Interno, Eventos |
| `menuItemNum` | int | Código do produto |
| `menuItemName1` | string | Descrição do produto |
| `numerator` | int | Quantidade vendida |
| `lineTotal` | float | Valor total do item |
| `businessDate` | string | Data de negócio (YYYY-MM-DD) |
| `transactionDateTime` | string | Timestamp da transação |
| `isVoidFlag` | int | 1 se cancelado |

**Mapeamento locationRef → hotel canônico:**

| locationRef | Hotel |
|-------------|-------|
| `CARM` | CHARME |
| `CUMBUCO` | CUMBUCO |
| `TAIBA` | TAIBA |
| `MAGN` | MAGNA |

**Order types encontrados:**

| Hotel | Order types |
|-------|-------------|
| Cumbuco | Hospede, Consumo Interno |
| Magna | Hospede, Consumo Interno |
| Taiba | Hospede, Eventos |
| Charme | Hospede, Consumo Interno |

---

## Comandos

```bash
# Sincronização diária (ontem por padrão)
python -m etls.gcm.sync

# Data específica
python -m etls.gcm.sync 2026-03-26

# Backfill de intervalo
python -m etls.gcm.history_sync 2026-01-01 2026-03-26

# Gerar XML CMFlex para todos os hotéis
python -m etls.gcm.cmflex_export 2026-03-26

# Gerar XML CMFlex só para Charme
python -m etls.gcm.cmflex_export 2026-03-26 CARM
```

---

## Export CMFlex (`cmflex_export.py`)

Filtra itens de `orderTypeName = 'Consumo Interno'`, agrupa por comanda (`guestCheckID`) e gera um `<PDVVenda>` por comanda no formato exigido pelo CMFlex ERP.

**Saída:** `output/{hotel}_cmflex_{data}.xml`

**Variáveis de ambiente necessárias (`auth/prod/.env`):**

```
GCM_ECF_SERIAL_CARM=<serial do ECF para o Charme>
GCM_ECF_SERIAL_CUMBUCO=<serial do ECF para o Cumbuco>
GCM_ECF_SERIAL_TAIBA=<serial do ECF para o Taiba>
GCM_ECF_SERIAL_MAGN=<serial do ECF para o Magna>
```

Se não configurados, o fallback é `ECF_{locationRef}`.

**Mapeamento GCM → CMFlex XML:**

| Campo CMFlex | Origem GCM |
|---|---|
| `NumeroDeSerieECF` | Env var `GCM_ECF_SERIAL_{locationRef}` |
| `NumeroCOO` | `checkNum` (zero-padded 6 dígitos) |
| `DataEmissao` | `transactionDateTime` + `-03:00` |
| `ValorTotal` | Soma de `lineTotal` da comanda |
| `CodigoProduto` | `menuItemNum` |
| `DescricaoProduto` | `menuItemName1` |
| `Quantidade` | `numerator` |
| `ValorUnitario` | `lineTotal / numerator` |
| `ValorTotalLiquido` | `lineTotal` |
| `TotalizadorParcial` | `majorGroupNum` (zero-padded 3 dígitos) |
| `Cancelado` | `isVoidFlag == 1` |
| `MeioDePagamento/Descricao` | `"Consumo Interno"` (fixo) |
| `ValorPago` | Soma de `lineTotal` da comanda |

---

## Variáveis de ambiente SFTP

Reutiliza as credenciais do PDV (mesmo servidor):

```
PDV_SFTP_HOST
PDV_SFTP_PORT   (padrão: 22)
PDV_SFTP_USER
PDV_SFTP_PASS
PDV_SFTP_PATH   (padrão: /d01/carmel_sftp/arquivos)
```
