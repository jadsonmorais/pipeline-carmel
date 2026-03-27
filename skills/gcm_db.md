# GCM DB — Schema e Queries

## Tabela: `carmel.gcm_raw_line_items`

```sql
CREATE TABLE carmel.gcm_raw_line_items (
    line_item_id BIGINT PRIMARY KEY,   -- guestCheckLineItemID
    data         JSONB NOT NULL,
    extracted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
```

**Campos JSONB importantes:**

| Path | Tipo | Descrição |
|------|------|-----------|
| `data->>'businessDate'` | date string | Data de negócio |
| `data->>'locationRef'` | string | Hotel: CARM, CUMBUCO, TAIBA, MAGN |
| `data->>'hotel'` | string | Hotel canônico: CHARME, CUMBUCO, TAIBA, MAGNA |
| `data->>'orderTypeName'` | string | Tipo de venda |
| `data->>'guestCheckID'` | bigint string | Agrupa itens de uma comanda |
| `data->>'checkNum'` | int string | Número da comanda |
| `data->>'menuItemNum'` | int string | Código do produto |
| `data->>'menuItemName1'` | string | Descrição do produto |
| `data->>'lineTotal'` | numeric string | Valor do item |
| `data->>'numerator'` | int string | Quantidade |
| `data->>'isVoidFlag'` | int string | 1 = cancelado |
| `data->>'transactionDateTime'` | timestamp string | Timestamp da transação |
| `data->>'majorGroupName'` | string | Grupo maior (Alimentos, Bebidas...) |
| `data->>'familyGroupName'` | string | Subgrupo |
| `data->>'revenueCenterName'` | string | Centro de receita (Café da Manhã, etc.) |

---

## Queries úteis

### Contagem por hotel e data
```sql
SELECT
    data->>'hotel' AS hotel,
    data->>'businessDate' AS data,
    count(*) AS total_items
FROM carmel.gcm_raw_line_items
GROUP BY 1, 2
ORDER BY 2 DESC, 1;
```

### Consumo Interno por data
```sql
SELECT
    data->>'hotel' AS hotel,
    data->>'businessDate' AS data,
    count(DISTINCT (data->>'guestCheckID')::bigint) AS comandas,
    count(*) AS itens,
    sum((data->>'lineTotal')::numeric) AS valor_total
FROM carmel.gcm_raw_line_items
WHERE data->>'orderTypeName' = 'Consumo Interno'
GROUP BY 1, 2
ORDER BY 2 DESC, 1;
```

### Itens de uma comanda específica
```sql
SELECT
    data->>'menuItemName1' AS produto,
    (data->>'numerator')::int AS quantidade,
    (data->>'lineTotal')::numeric AS valor
FROM carmel.gcm_raw_line_items
WHERE (data->>'guestCheckID')::bigint = 2390134934
ORDER BY (data->>'lineNum')::int;
```

### Volume por tipo de pedido
```sql
SELECT
    data->>'orderTypeName' AS tipo,
    data->>'hotel' AS hotel,
    count(*) AS itens,
    count(DISTINCT (data->>'guestCheckID')::bigint) AS comandas,
    sum((data->>'lineTotal')::numeric) AS valor_total
FROM carmel.gcm_raw_line_items
WHERE data->>'businessDate' = '2026-03-26'
GROUP BY 1, 2
ORDER BY 1, 2;
```

### Top produtos vendidos
```sql
SELECT
    data->>'menuItemName1' AS produto,
    data->>'majorGroupName' AS grupo,
    count(*) AS ocorrencias,
    sum((data->>'numerator')::int) AS qtd_total,
    sum((data->>'lineTotal')::numeric) AS valor_total
FROM carmel.gcm_raw_line_items
WHERE data->>'businessDate' = '2026-03-26'
  AND data->>'hotel' = 'CHARME'
  AND data->>'isVoidFlag' = '0'
GROUP BY 1, 2
ORDER BY valor_total DESC
LIMIT 20;
```

---

## Chave de conciliação

`gcm_raw_line_items` não tem chave direta com `pdv_raw_notas` ou `nfe_raw_xmls`.
O GCM é uma fonte complementar com granularidade de item de linha, enquanto o PDV trabalha com notas fiscais completas.

Para reconciliar: `businessDate` + `hotel` + `checkNum` pode cruzar com dados operacionais.
