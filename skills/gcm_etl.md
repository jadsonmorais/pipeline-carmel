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
  cmflex_export.py   — gerador de JSON CMFlex para Consumo Interno
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

# Gerar JSON CMFlex para todos os hotéis
python -m etls.gcm.cmflex_export 2026-03-26

# Gerar JSON CMFlex só para Charme
python -m etls.gcm.cmflex_export 2026-03-26 "CARMEL CHARME RESORT"
```

---

## Export CMFlex (`cmflex_export.py`)

Filtra itens de `orderTypeName = 'Consumo Interno'` com `lineTotal <> 0` e gera uma lista flat de vendas por hotel no formato JSON exigido pelo CMFlex ERP (`TipoDeProcessamento = ProcVendaParaBaixaEstoque`).

**Saída:** `output/{hotel}_cmflex_{data}.json`

**Variáveis de ambiente necessárias (`auth/prod/.env`):**

Chave = `locationName.upper().replace(' ', '_')`, ex: `CARMEL_CHARME_RESORT`

```
GCM_ECF_SERIAL_CARMEL_CHARME_RESORT=CHARME.SERVIDOR-CAPS
GCM_ECF_SERIAL_CARMEL_CUMBUCO_WIND_RESORT=WIND.SRV-CAPS
GCM_ECF_SERIAL_CARMEL_TAIBA_EXCLUSIVE_RESORT=TAIBA.TAIBA-CIPO
GCM_ECF_SERIAL_MAGNA_PRAIA_HOTEL=MAGNA.SRV-CAPS

GCM_EMPRESA_ID_CARMEL_CHARME_RESORT=<int>
GCM_EMPRESA_ID_CARMEL_CUMBUCO_WIND_RESORT=<int>
GCM_EMPRESA_ID_CARMEL_TAIBA_EXCLUSIVE_RESORT=<int>
GCM_EMPRESA_ID_MAGNA_PRAIA_HOTEL=<int>

GCM_CODIGO_EMPRESA_CARMEL_CHARME_RESORT=<string, ex: POS003>
GCM_CODIGO_EMPRESA_CARMEL_CUMBUCO_WIND_RESORT=<string>
GCM_CODIGO_EMPRESA_CARMEL_TAIBA_EXCLUSIVE_RESORT=<string>
GCM_CODIGO_EMPRESA_MAGNA_PRAIA_HOTEL=<string>

GCM_CHAVE_ACESSO_CARMEL_CHARME_RESORT=<UUID>
GCM_CHAVE_ACESSO_CARMEL_CUMBUCO_WIND_RESORT=<UUID>
GCM_CHAVE_ACESSO_CARMEL_TAIBA_EXCLUSIVE_RESORT=<UUID>
GCM_CHAVE_ACESSO_MAGNA_PRAIA_HOTEL=<UUID>
```

**Mapeamento GCM → CMFlex JSON:**

| Campo CMFlex JSON | Origem |
|---|---|
| `EmpresaId` | Env `GCM_EMPRESA_ID_*` (inteiro) |
| `CodigoDaEmpresa` | Env `GCM_CODIGO_EMPRESA_*` |
| `ChaveDeAcesso` | Env `GCM_CHAVE_ACESSO_*` |
| `DataDoMovimento` | Argumento `date_str` |
| `CodigoPDV` | `revenueCenterNum` (string) |
| `EmissordoCupom` | Env `GCM_ECF_SERIAL_*` |
| `CodigoExterno` | `menuItemNum` (string) |
| `UnidadeMedida` | `"UN"` (fixo) |
| `Quantidade` | `lineCount` (float) |
| `ValorUnitario` | `lineTotal / lineCount` (float) |

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
