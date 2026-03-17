# Skill: PDV Simphony — Banco de Dados

> Referência do schema, tabelas, estrutura JSONB e views analíticas do PDV.
> Use este documento para escrever queries, entender paths de campos ou modificar o banco.

---

## Tabela Raw

### `carmel.pdv_raw_notas`

Um registro por nota fiscal emitida no PDV Simphony.

```sql
CREATE TABLE IF NOT EXISTS carmel.pdv_raw_notas (
    nota_id     VARCHAR(44) PRIMARY KEY,   -- chave NF-e 44 dígitos (Invoice Data Info 8)
    data        JSONB NOT NULL,
    extracted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
```

**`nota_id`** é a chave NF-e de 44 dígitos — é também a chave de conciliação com SEFAZ.

---

## Estrutura JSONB

O JSONB armazena todos os campos originais do registro `FISID` do Simphony, mais 3 campos injetados pelo ETL:

### Campos injetados pelo ETL

| Path JSONB | Tipo | Exemplo |
|-----------|------|---------|
| `data->>'id'` | text | `'23260219253187000163650010000115191192531879'` |
| `data->>'hotel'` | text | `'CUMBUCO'` |
| `data->>'source_file'` | text | `'CumbucoCFB_2026-02-19.json'` |

### Campos originais do Simphony (FISID)

| Path JSONB | Tipo | Descrição |
|-----------|------|-----------|
| `data->>'Record Type'` | text | Sempre `'FISID'` |
| `data->>'Business Date'` | text | Data de negócio (`'2026-02-19'`) |
| `data->>'Check Number'` | numeric | Número da comanda/mesa |
| `data->>'FCR Invoice Number'` | text | Número sequencial da nota no PDV |
| `data->>'Sub Total 1'` | numeric | Valor bruto total da nota |
| `data->>'Sub Total 2'` | numeric | Subtotal tributável (ISS) |
| `data->>'Sub Total 3'` | numeric | Subtotal não tributável |
| `data->>'Sub Total 6'` | numeric | Consumação / gorjeta (depende do centro de receita) |
| `data->>'Tax Total 1'` | numeric | ISS calculado |
| `data->>'Invoice Data Info 1'` | text | Identificador NF-e com sufixo (`NFe...-nfe`) |
| `data->>'Invoice Data Info 4'` | text | Nome do estabelecimento |
| `data->>'Invoice Data Info 5'` | text | Número do quarto do hóspede |
| `data->>'Invoice Data Info 6'` | text | Nome do garçom/operador |
| `data->>'Invoice Data Info 8'` | text | Chave NF-e 44 dígitos (= `nota_id`) |
| `data->>'Invoice Status'` | numeric | Status da nota (1 = ativa) |
| `data->>'Local Revenue Center Name'` | text | Ponto de venda local (ex: `'Bar do Barco'`) |
| `data->>'Revenue Center Master Name'` | text | Ponto de venda master |
| `data->>'uwsName'` | text | Nome do terminal PDV (ex: `'WIND-24'`) |
| `data->>'uwsID'` | numeric | ID do terminal PDV |
| `data->>'opnBusDt'` | text | Data de abertura do turno |

---

## View Analítica

### `carmel.v_pdv_notas`

View principal para consumo — expõe os campos mais relevantes com tipos corretos.

```sql
SELECT * FROM carmel.v_pdv_notas;
```

| Coluna | Tipo | Origem JSONB |
|--------|------|-------------|
| `chave_nfe` | text | `nota_id` (= `Invoice Data Info 8`) |
| `hotel` | text | `data->>'hotel'` |
| `data_venda` | date | `data->>'Business Date'` |
| `numero_nota` | text | `data->>'FCR Invoice Number'` |
| `numero_comanda` | bigint | `data->>'Check Number'` |
| `subtotal_1` | numeric | `data->>'Sub Total 1'` — valor bruto |
| `subtotal_2` | numeric | `data->>'Sub Total 2'` — subtotal tributável |
| `subtotal_3` | numeric | `data->>'Sub Total 3'` — subtotal não tributável |
| `subtotal_6` | numeric | `data->>'Sub Total 6'` — consumação/gorjeta |
| `imposto_1` | numeric | `data->>'Tax Total 1'` — ISS |
| `ponto_venda` | text | `data->>'Local Revenue Center Name'` |
| `ponto_venda_master` | text | `data->>'Revenue Center Master Name'` |
| `quarto` | text | `data->>'Invoice Data Info 5'` |
| `garcom` | text | `data->>'Invoice Data Info 6'` |
| `status_nota` | smallint | `data->>'Invoice Status'` |
| `terminal_pdv` | text | `data->>'uwsName'` |
| `arquivo_origem` | text | `data->>'source_file'` |
| `extracted_at` | timestamptz | coluna da tabela raw |

---

## Queries Úteis

### Totais por hotel e dia
```sql
SELECT hotel, data_venda, COUNT(*) AS qtd_notas, SUM(subtotal_1) AS total_bruto
FROM carmel.v_pdv_notas
GROUP BY hotel, data_venda
ORDER BY data_venda DESC, hotel;
```

### Vendas por ponto de venda
```sql
SELECT hotel, ponto_venda, SUM(subtotal_1) AS total
FROM carmel.v_pdv_notas
WHERE data_venda = '2026-02-19'
GROUP BY hotel, ponto_venda
ORDER BY total DESC;
```

### Notas de um hóspede (por quarto)
```sql
SELECT data_venda, hotel, quarto, numero_nota, subtotal_1, garcom, ponto_venda
FROM carmel.v_pdv_notas
WHERE quarto = '310'
ORDER BY data_venda DESC;
```

### Buscar nota pela chave NF-e (conciliação com SEFAZ)
```sql
SELECT * FROM carmel.v_pdv_notas
WHERE chave_nfe = '23260219253187000163650010000115191192531879';
```

### Verificar cobertura de datas no banco
```sql
SELECT data_venda, hotel, COUNT(*) AS qtd_notas
FROM carmel.v_pdv_notas
GROUP BY data_venda, hotel
ORDER BY data_venda DESC;
```

---

## Chave de Conciliação com SEFAZ

O campo `nota_id` / `chave_nfe` é a chave NF-e de 44 dígitos emitida pelo Simphony.
É a mesma chave presente nos XMLs da SEFAZ. O join futuro será:

```sql
SELECT p.*, s.*
FROM carmel.pdv_raw_notas p
JOIN carmel.sefaz_raw_notas s ON p.nota_id = s.nota_id;
```

---

## Mapeamento Hotel (Store Number → Canônico)

| Store Number no arquivo | Hotel no banco |
|------------------------|----------------|
| `CUMBUCO` | `CUMBUCO` |
| `TAIBA` | `TAIBA` |
| `CARM` | `CHARME` |
| `MAGN` | `MAGNA` |
