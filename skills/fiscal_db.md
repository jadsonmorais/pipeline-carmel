# Skill: Fiscal — Schema e Queries

## Tabela: `carmel.fiscal_raw_lancamentos`

```sql
CREATE TABLE IF NOT EXISTS carmel.fiscal_raw_lancamentos (
    lancamento_id VARCHAR(255) PRIMARY KEY,  -- IDLANCAMENTOICMSBASE (string)
    data          JSONB NOT NULL,
    extracted_at  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
```

---

## Estrutura do JSONB `data`

Campos retornados pelo export `idExportacao=80` da API CMERP:

| Campo                      | Tipo     | Descrição                                          |
|----------------------------|----------|----------------------------------------------------|
| `IDLANCAMENTOICMSBASE`     | string   | PK — ID do lançamento fiscal                       |
| `TIPODEDOCUMENTO`          | string   | Tipo do documento fiscal                           |
| `ANCFOP`                   | string   | CFOP da operação                                   |
| `FKEMPRESA`                | string   | ID da empresa (FK para mapeamento de hotel)        |
| `EMPRESA`                  | string   | Nome da empresa/hotel                              |
| `VLVALORTTDOCUMENTO`       | number   | Valor total do documento                           |
| `DTDATAEMISSAO`            | datetime | Data de emissão                                    |
| `ANNUMERODOCUMENTO`        | string   | Número do documento                                |
| `ANSERIE`                  | string   | Série do documento                                 |
| `QTQUANTIDADECOMERCIAL`    | number   | Quantidade comercial do item                       |
| `VLVALORUNITARIOCOMERCIAL` | number   | Valor unitário comercial                           |
| `VLVALORTOTAL`             | number   | Valor total do item                                |
| `VLVALORTOTALDOSPRODUTOS`  | number   | Valor total dos produtos                           |
| `VALORDESCONTO`            | number   | Valor de desconto                                  |
| `VALORUNITARIOCOMDESCONTO` | number   | Valor unitário com desconto                        |
| `ANCODIGO`                 | string   | Código do produto/serviço                          |
| `ANDESCRICAO`              | string   | Descrição do produto/serviço                       |
| `IDARTIGO`                 | string   | ID do artigo no CMERP                              |
| `ANINFORMACAOCONTRIB`      | string   | Informação adicional do contribuinte               |
| `INICIOATENDENTE`          | number   | Offset na string de info complementar (atendente)  |
| `NUMEROCARACTERESATENDENDE`| number   | Número de caracteres (atendente)                   |
| `INICIOWS`                 | number   | Offset na string de info complementar (ws)         |
| `NUMEROCARACTERESWS`       | number   | Número de caracteres (ws)                          |
| `INICIOCONTA`              | number   | Offset (conta)                                     |
| `NUMEROCARACTERESCONTA`    | number   | Número de caracteres (conta)                       |
| `INICIOMESA`               | number   | Offset (mesa)                                      |
| `NUMEROCARACTERESMESA`     | number   | Número de caracteres (mesa)                        |

---

## Mapeamento FKEMPRESA → Hotel

| FKEMPRESA | EMPRESA             | Hotel canônico |
|-----------|---------------------|----------------|
| `1`       | CARMEL TAÍBA        | TAIBA          |
| `2`       | CHARME HOSPEDAGEM   | CHARME         |
| `3`       | CARMEL CUMBUCO      | CUMBUCO        |
| `4`       | MAGNA PRAIA         | MAGNA          |

---

## View: `carmel.v_fiscal_lancamentos`

View principal do fiscal. Uma linha por item/lançamento, com hotel canônico e join com NF-e para trazer chave de 44 dígitos e status SEFAZ.

**Join com NF-e**: `nNF + serie + hotel` → `nfe_raw_xmls` (traz `nota_id`, `cStat`, `nProt`, `dhRecbto`)
**Join com cancelamentos**: via `nota_id` → indica se a nota foi cancelada.

Colunas principais:

| Coluna                  | Descrição                                      |
|-------------------------|------------------------------------------------|
| `lancamento_id`         | PK (IDLANCAMENTOICMSBASE)                      |
| `hotel`                 | Hotel canônico (TAIBA/CHARME/CUMBUCO/MAGNA)    |
| `empresa`               | Nome da empresa no CMERP                       |
| `data_emissao`          | Data de emissão do documento                   |
| `tipo_documento`        | Tipo do documento fiscal                       |
| `numero_documento`      | Número da NF                                   |
| `serie`                 | Série da NF                                    |
| `cfop`                  | CFOP da operação                               |
| `codigo_produto`        | Código do produto/serviço                      |
| `descricao_produto`     | Descrição do produto/serviço                   |
| `quantidade`            | Quantidade comercial                           |
| `valor_unitario`        | Valor unitário                                 |
| `valor_total_item`      | Valor total do item                            |
| `desconto`              | Desconto aplicado                              |
| `valor_total_documento` | Valor total da NF (repete em todos os itens)   |
| `chave_nfe`             | Chave 44 dígitos (via join NF-e XMLs)          |
| `status_sefaz`          | 100=autorizada (via join NF-e XMLs)            |
| `protocolo_sefaz`       | Protocolo de autorização                       |
| `recebimento_sefaz`     | Data/hora de recebimento na SEFAZ              |
| `cancelada`             | true/false                                     |

---

## Queries Úteis

### Contagem por empresa/hotel

```sql
SELECT data->>'EMPRESA' AS empresa, COUNT(*) AS total
FROM carmel.fiscal_raw_lancamentos
GROUP BY 1
ORDER BY 1;
```

### Lançamentos por data de emissão

```sql
SELECT
    (data->>'DTDATAEMISSAO')::date AS data_emissao,
    data->>'EMPRESA'               AS empresa,
    COUNT(*)                       AS total,
    SUM((data->>'VLVALORTTDOCUMENTO')::numeric) AS valor_total
FROM carmel.fiscal_raw_lancamentos
GROUP BY 1, 2
ORDER BY 1 DESC, 2;
```

### Lançamentos de um período específico

```sql
SELECT
    lancamento_id,
    data->>'EMPRESA'          AS empresa,
    data->>'DTDATAEMISSAO'    AS emissao,
    data->>'ANNUMERODOCUMENTO' AS numero,
    data->>'ANSERIE'           AS serie,
    data->>'TIPODEDOCUMENTO'   AS tipo,
    data->>'ANCFOP'            AS cfop,
    (data->>'VLVALORTTDOCUMENTO')::numeric AS valor_total
FROM carmel.fiscal_raw_lancamentos
WHERE (data->>'DTDATAEMISSAO')::date BETWEEN '2026-03-01' AND '2026-03-07'
ORDER BY 3 DESC;
```

### Produtos/serviços mais faturados

```sql
SELECT
    data->>'ANCODIGO'    AS codigo,
    data->>'ANDESCRICAO' AS descricao,
    COUNT(*)             AS ocorrencias,
    SUM((data->>'VLVALORTOTAL')::numeric) AS valor_total
FROM carmel.fiscal_raw_lancamentos
GROUP BY 1, 2
ORDER BY 4 DESC;
```
