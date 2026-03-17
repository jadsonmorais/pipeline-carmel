# Skill: NF-e — Schema e Queries

## Tabela: `carmel.nfe_raw_xmls`

```sql
CREATE TABLE IF NOT EXISTS carmel.nfe_raw_xmls (
    nota_id      VARCHAR(44) PRIMARY KEY,  -- chave NF-e 44 dígitos
    data         JSONB NOT NULL,
    extracted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
```

### Estrutura do JSONB `data`

| Campo         | Tipo    | Origem no XML                        | Descrição                              |
|---------------|---------|--------------------------------------|----------------------------------------|
| `id`          | string  | `infNFe/@Id` strip "NFe"             | chave 44 dígitos (igual a nota_id)     |
| `nota_id`     | string  | idem                                 | redundante, mantido por convenção      |
| `hotel`       | string  | derivado do share SMB                | CUMBUCO/CHARME/MAGNA/TAIBA             |
| `source_file` | string  | nome do arquivo                      | ex: `NFe...872-nfe.xml`                |
| `dhEmi`       | string  | `ide/dhEmi`                          | data/hora emissão (ISO 8601)           |
| `nNF`         | string  | `ide/nNF`                            | número da nota                         |
| `serie`       | string  | `ide/serie`                          | série                                  |
| `mod`         | string  | `ide/mod`                            | 55=NF-e, 65=NFC-e                      |
| `tpAmb`       | string  | `ide/tpAmb`                          | 1=produção, 2=homologação              |
| `cnpj_emit`   | string  | `emit/CNPJ`                          | CNPJ do emitente                       |
| `nome_emit`   | string  | `emit/xNome`                         | razão social do emitente               |
| `vNF`         | string  | `total/ICMSTot/vNF`                  | valor total da nota                    |
| `nProt`       | string  | `protNFe/infProt/nProt`              | protocolo de autorização SEFAZ         |
| `cStat`       | string  | `protNFe/infProt/cStat`              | 100=autorizada                         |
| `dhRecbto`    | string  | `protNFe/infProt/dhRecbto`           | data/hora recebimento na SEFAZ         |
| `xml_content` | string  | arquivo completo                     | XML original (para auditoria/reprocessamento) |

---

## Tabela: `carmel.nfe_raw_cancelamentos`

```sql
CREATE TABLE IF NOT EXISTS carmel.nfe_raw_cancelamentos (
    cancelamento_id VARCHAR(255) PRIMARY KEY,  -- Id do infEvento
    data            JSONB NOT NULL,
    extracted_at    TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
```

### Estrutura do JSONB `data`

| Campo            | Tipo   | Origem no XML            | Descrição                                    |
|------------------|--------|--------------------------|----------------------------------------------|
| `id`             | string | `infEvento/@Id`          | ID do evento (igual a cancelamento_id)       |
| `cancelamento_id`| string | idem                     | redundante, mantido por convenção             |
| `chNFe`          | string | `infEvento/chNFe`        | chave 44 dígitos da nota cancelada (FK → nfe_raw_xmls.nota_id) |
| `hotel`          | string | derivado do share SMB    | CUMBUCO/CHARME/MAGNA/TAIBA                   |
| `source_file`    | string | nome do arquivo          | ex: `NFe...-can.xml`                         |
| `dhEvento`       | string | `infEvento/dhEvento`     | data/hora do cancelamento (ISO 8601)          |
| `tpEvento`       | string | `infEvento/tpEvento`     | 110111 = cancelamento NF-e                   |
| `nSeqEvento`     | string | `infEvento/nSeqEvento`   | número de sequência do evento                |
| `cnpj`           | string | `infEvento/CNPJ`         | CNPJ do emitente                             |
| `nProt`          | string | `detEvento/nProt`        | protocolo do cancelamento                    |
| `xJust`          | string | `detEvento/xJust`        | justificativa do cancelamento                |
| `xml_content`    | string | arquivo completo         | XML original                                 |

---

## Chaves de Conciliação

```
nfe_raw_xmls.nota_id        =  pdv_raw_notas.nota_id        (PDV ↔ XML fiscal)
nfe_raw_cancelamentos.data->>'chNFe'  =  nfe_raw_xmls.nota_id  (XML fiscal ↔ cancelamento)
```

---

## View: `carmel.v_nfe_notas`

View consolidada: uma linha por NF-e, com status de cancelamento e conciliação com PDV.

```sql
SELECT
    nota_id, hotel, data_emissao, numero_nota, serie, modelo,
    ambiente,          -- 1=produção, 2=homologação
    cnpj_emit, emitente,
    valor_total,       -- vNF
    protocolo_autorizacao, status_sefaz,  -- 100=autorizada
    data_recebimento_sefaz,
    -- cancelamento
    cancelada,         -- true/false
    data_cancelamento, protocolo_cancelamento, justificativa_cancelamento,
    -- PDV
    tem_pdv,           -- true/false
    data_venda_pdv, valor_pdv, ponto_venda,
    extracted_at
FROM carmel.v_nfe_notas;
```

---

## Queries de Validação

### Contagem por hotel

```sql
SELECT hotel, COUNT(*) AS total
FROM carmel.nfe_raw_xmls
GROUP BY 1
ORDER BY 1;
```

### Cancelamentos por hotel

```sql
SELECT data->>'hotel' AS hotel, COUNT(*) AS total
FROM carmel.nfe_raw_cancelamentos
GROUP BY 1
ORDER BY 1;
```

### Visão geral — notas, canceladas e com PDV

```sql
SELECT
    hotel,
    COUNT(*)                       AS total_notas,
    COUNT(*) FILTER (WHERE cancelada)  AS canceladas,
    COUNT(*) FILTER (WHERE tem_pdv)    AS com_pdv,
    COUNT(*) FILTER (WHERE NOT tem_pdv AND NOT cancelada) AS sem_pdv_e_ativas
FROM carmel.v_nfe_notas
GROUP BY 1
ORDER BY 1;
```

### Notas do PDV que têm XML no fiscal

```sql
SELECT COUNT(*)
FROM carmel.nfe_raw_xmls n
JOIN carmel.pdv_raw_notas p USING (nota_id);
```

### Notas do PDV sem XML no fiscal (gap)

```sql
SELECT p.nota_id, p.data->>'hotel' AS hotel, p.data->>'dhEmi' AS dhEmi
FROM carmel.pdv_raw_notas p
LEFT JOIN carmel.nfe_raw_xmls n USING (nota_id)
WHERE n.nota_id IS NULL
ORDER BY p.data->>'dhEmi' DESC;
```

### XMLs enviados ao fiscal sem registro no PDV

```sql
SELECT n.nota_id, n.data->>'hotel' AS hotel, n.data->>'dhEmi' AS dhEmi
FROM carmel.nfe_raw_xmls n
LEFT JOIN carmel.pdv_raw_notas p USING (nota_id)
WHERE p.nota_id IS NULL
ORDER BY n.data->>'dhEmi' DESC;
```

### Notas autorizadas (cStat=100)

```sql
SELECT nota_id, data->>'hotel', data->>'dhEmi', data->>'vNF', data->>'nProt'
FROM carmel.nfe_raw_xmls
WHERE data->>'cStat' = '100'
ORDER BY data->>'dhEmi' DESC;
```

### Cancelamentos que não têm XML correspondente (inconsistência)

```sql
SELECT c.cancelamento_id, c.data->>'chNFe' AS chave_nota, c.data->>'hotel' AS hotel
FROM carmel.nfe_raw_cancelamentos c
LEFT JOIN carmel.nfe_raw_xmls n ON n.nota_id = c.data->>'chNFe'
WHERE n.nota_id IS NULL;
```

### Recuperar XML completo de uma nota

```sql
SELECT data->>'xml_content'
FROM carmel.nfe_raw_xmls
WHERE nota_id = '23260319253187000163650010000170031192531872';
```
