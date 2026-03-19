# Skill: vendas_db — Views Consolidadas de Vendas

## Views disponíveis

### `carmel.v_vendas_notas`
Uma linha por NF-e. Consolida `v_nfe_notas` (base) + `v_pdv_notas` (LEFT JOIN por `chave_nfe = nota_id`) + agregado de `v_fiscal_lancamentos`.

**Colunas principais:**

| Coluna | Tipo | Origem |
|--------|------|--------|
| `nota_id` | text | NF-e (chave 44 dígitos) |
| `hotel` | text | NF-e |
| `data_emissao` | timestamp | NF-e |
| `numero_nota` | text | NF-e |
| `serie` | text | NF-e |
| `modelo` | text | NF-e |
| `valor_total` | numeric | NF-e |
| `status_sefaz` | text | NF-e (`cStat` do XML) |
| `protocolo_autorizacao` | text | NF-e (`nProt`) |
| `data_recebimento_sefaz` | timestamp | NF-e (`dhRecbto`) |
| `cancelada` | boolean | NF-e |
| `data_cancelamento` | timestamp | NF-e |
| `justificativa_cancelamento` | text | NF-e |
| `tem_pdv` | boolean | NF-e (flag de join) |
| `data_venda_pdv` | timestamp | NF-e |
| `valor_pdv` | numeric | NF-e |
| `ponto_venda` | text | NF-e |
| `subtotal_1` | numeric | PDV |
| `subtotal_2` | numeric | PDV |
| `subtotal_3` | numeric | PDV |
| `subtotal_6` | numeric | PDV |
| `imposto_1` | numeric | PDV |
| `quarto` | text | PDV (`Invoice Data Info 5`) |
| `garcom` | text | PDV (`Invoice Data Info 6`) |
| `tem_fiscal` | boolean | Fiscal (agregado) |
| `qtd_itens` | bigint | Fiscal (COUNT) |
| `cfop_principal` | text | Fiscal (MODE) |
| `status` | text | Calculado (ver abaixo) |

**Lógica de status:**
```
cancelada = TRUE        → 'Cancelada'
status_sefaz = '100'   → 'Autorizada'
else                    → 'Pendente SEFAZ'
```

### `carmel.v_vendas_diario`
Uma linha por (hotel, data). Agrega `v_vendas_notas`.

**Colunas:**

| Coluna | Descrição |
|--------|-----------|
| `hotel` | Nome canônico do hotel |
| `data` | Data de emissão (`date`) |
| `total_notas` | Todas as notas do dia |
| `notas_autorizadas` | Status = 'Autorizada' |
| `notas_canceladas` | Status = 'Cancelada' |
| `notas_pendentes` | Status = 'Pendente SEFAZ' |
| `valor_total_dia` | Soma excluindo canceladas |
| `notas_com_pdv` | tem_pdv = TRUE |
| `notas_com_fiscal` | tem_fiscal = TRUE |

---

## Queries úteis

### Vendas dos últimos 30 dias por hotel
```sql
SELECT hotel, data, total_notas, notas_autorizadas, valor_total_dia
FROM carmel.v_vendas_diario
WHERE data >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY hotel, data DESC;
```

### Notas autorizadas com discrepância PDV × NF-e
```sql
SELECT nota_id, hotel, data_emissao, valor_total, valor_pdv,
       valor_total - valor_pdv AS diferenca
FROM carmel.v_vendas_notas
WHERE status = 'Autorizada'
  AND tem_pdv
  AND ABS(valor_total - valor_pdv) > 0.01
ORDER BY ABS(valor_total - valor_pdv) DESC;
```

### Pendentes SEFAZ por hotel
```sql
SELECT hotel, COUNT(*) AS pendentes, SUM(valor_total) AS valor_total
FROM carmel.v_vendas_notas
WHERE status = 'Pendente SEFAZ'
GROUP BY hotel
ORDER BY pendentes DESC;
```

### Notas sem correspondência PDV ou Fiscal
```sql
SELECT nota_id, hotel, data_emissao, valor_total, status,
       tem_pdv, tem_fiscal
FROM carmel.v_vendas_notas
WHERE status = 'Autorizada'
  AND (NOT tem_pdv OR NOT tem_fiscal)
ORDER BY data_emissao DESC;
```

### Receita mensal por hotel (apenas autorizadas)
```sql
SELECT hotel,
       DATE_TRUNC('month', data) AS mes,
       SUM(valor_total_dia)      AS receita
FROM carmel.v_vendas_diario
GROUP BY hotel, DATE_TRUNC('month', data)
ORDER BY hotel, mes DESC;
```

---

## Filtros disponíveis (intranet)

- `hotel`: CUMBUCO / TAIBA / CHARME / MAGNA
- `status`: Autorizada / Cancelada / Pendente SEFAZ
- `data_inicio` / `data_fim`: intervalo de datas (padrão: últimos 30 dias)

---

## Notas de design

- `v_vendas_notas` é baseada em `v_nfe_notas`, então só existem notas que passaram pelo fluxo NF-e.
- `status_sefaz = '100'` é o código SEFAZ para autorização (campo `cStat` do XML).
- O join com PDV usa `chave_nfe` (44 dígitos) no PDV = `nota_id` na NF-e.
- O fiscal agrega por `chave_nfe` com `MODE()` para eleger o CFOP principal da nota.
