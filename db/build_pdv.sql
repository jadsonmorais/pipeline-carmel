-- ==============================================================================
-- PROJETO PIPELINE-CARMEL — PDV / FISCAL / NF-e / SEFAZ
-- ARQUIVO: build_pdv.sql
-- DESCRIÇÃO: DDL completo para o domínio fiscal/PDV (tabelas, views, índices)
-- DEPENDE DE: build.sql (schema carmel já criado)
-- ==============================================================================


-- ------------------------------------------------------------------------------
-- 1. TABELAS BRUTAS (CAMADA BRONZE / RAW)
-- ------------------------------------------------------------------------------

-- PDV Simphony: um registro por nota fiscal emitida no PDV
-- PK = chave NF-e 44 dígitos (Invoice Data Info 8) — chave de conciliação com SEFAZ
CREATE TABLE IF NOT EXISTS carmel.pdv_raw_notas (
    nota_id VARCHAR(44) PRIMARY KEY,
    data JSONB NOT NULL,
    extracted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Fiscal CMERP: lançamentos fiscais por empresa/hotel (idExportacao=80)
-- PK = IDLANCAMENTOICMSBASE (string)
CREATE TABLE IF NOT EXISTS carmel.fiscal_raw_lancamentos (
    lancamento_id VARCHAR(255) PRIMARY KEY,
    data          JSONB NOT NULL,
    extracted_at  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- NF-e XMLs: arquivos enviados para o fiscal (shares SMB \\10.197.0.51\{Hotel})
-- PK = chave NF-e 44 dígitos — mesma chave de pdv_raw_notas.nota_id
-- data->>'xml_content' contém o XML original completo para auditoria e reprocessamento
CREATE TABLE IF NOT EXISTS carmel.nfe_raw_xmls (
    nota_id VARCHAR(44) PRIMARY KEY,
    data    JSONB NOT NULL,
    extracted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Cancelamentos NF-e: eventos de cancelamento enviados pela SEFAZ (arquivos *-can.xml)
-- PK = Id do infEvento
-- data->>'chNFe' = chave 44 dígitos da nota cancelada (FK para nfe_raw_xmls.nota_id)
CREATE TABLE IF NOT EXISTS carmel.nfe_raw_cancelamentos (
    cancelamento_id VARCHAR(255) PRIMARY KEY,
    data            JSONB NOT NULL,
    extracted_at    TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);


-- ------------------------------------------------------------------------------
-- 2. VIEWS PDV / FISCAL / NF-e
-- ------------------------------------------------------------------------------

-- Fiscal: join pesado materializado para performance (~198k lançamentos × ~41k XMLs)
-- Criada com WITH NO DATA; o ETL faz o primeiro REFRESH após carga inicial
CREATE MATERIALIZED VIEW IF NOT EXISTS carmel.mv_fiscal_lancamentos AS
SELECT
    f.lancamento_id,
    CASE f.data->>'FKEMPRESA'
        WHEN '1' THEN 'TAIBA'
        WHEN '2' THEN 'CHARME'
        WHEN '3' THEN 'CUMBUCO'
        WHEN '4' THEN 'MAGNA'
    END                                               AS hotel,
    f.data->>'EMPRESA'                                AS empresa,
    (f.data->>'DTDATAEMISSAO')::date                  AS data_emissao,
    f.data->>'TIPODEDOCUMENTO'                        AS tipo_documento,
    f.data->>'ANNUMERODOCUMENTO'                      AS numero_documento,
    f.data->>'ANSERIE'                                AS serie,
    f.data->>'ANCFOP'                                 AS cfop,
    f.data->>'ANCODIGO'                               AS codigo_produto,
    f.data->>'ANDESCRICAO'                            AS descricao_produto,
    (f.data->>'QTQUANTIDADECOMERCIAL')::NUMERIC(12,4) AS quantidade,
    (f.data->>'VLVALORUNITARIOCOMERCIAL')::NUMERIC(12,2) AS valor_unitario,
    (f.data->>'VLVALORTOTAL')::NUMERIC(12,2)          AS valor_total_item,
    (f.data->>'VALORDESCONTO')::NUMERIC(12,2)         AS desconto,
    (f.data->>'VLVALORTTDOCUMENTO')::NUMERIC(12,2)    AS valor_total_documento,
    -- NF-e (join por número + série + hotel → traz chave 44 dígitos e status SEFAZ)
    n.nota_id                                         AS chave_nfe,
    n.data->>'cStat'                                  AS status_sefaz,   -- 100=autorizada
    n.data->>'nProt'                                  AS protocolo_sefaz,
    (n.data->>'dhRecbto')::timestamptz                AS recebimento_sefaz,
    -- EXISTS evita fan-out quando há múltiplos cancelamentos para o mesmo nota_id
    EXISTS (
        SELECT 1 FROM carmel.nfe_raw_cancelamentos can
        WHERE can.data->>'chNFe' = n.nota_id
    )                                                 AS cancelada,
    f.extracted_at
FROM carmel.fiscal_raw_lancamentos f
LEFT JOIN LATERAL (
    SELECT nota_id, data
    FROM carmel.nfe_raw_xmls
    WHERE data->>'nNF'   = f.data->>'ANNUMERODOCUMENTO'
      AND data->>'serie' = f.data->>'ANSERIE'
      AND data->>'hotel' = CASE f.data->>'FKEMPRESA'
          WHEN '1' THEN 'TAIBA'
          WHEN '2' THEN 'CHARME'
          WHEN '3' THEN 'CUMBUCO'
          WHEN '4' THEN 'MAGNA'
      END
    LIMIT 1
) n ON true
WITH NO DATA;

-- Índice único necessário para REFRESH MATERIALIZED VIEW CONCURRENTLY (uso futuro)
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_fiscal_lancamentos_pk
    ON carmel.mv_fiscal_lancamentos (lancamento_id);

-- Fiscal: wrapper fino sobre a MV — mantém compatibilidade com todo código existente
-- Uma linha por item/lançamento; join com nfe_raw_xmls via nNF + serie + hotel
CREATE OR REPLACE VIEW carmel.v_fiscal_lancamentos AS
SELECT * FROM carmel.mv_fiscal_lancamentos;


-- NF-e: visão consolidada — junta XML fiscal + cancelamento + PDV
-- Uma linha por nota; indica se foi cancelada e se há registro no PDV
CREATE OR REPLACE VIEW carmel.v_nfe_notas AS
SELECT
    n.nota_id,
    n.data->>'hotel'                              AS hotel,
    (n.data->>'dhEmi')::timestamptz               AS data_emissao,
    n.data->>'nNF'                                AS numero_nota,
    n.data->>'serie'                              AS serie,
    n.data->>'mod'                                AS modelo,           -- 55=NF-e, 65=NFC-e
    n.data->>'tpAmb'                              AS ambiente,         -- 1=produção, 2=homologação
    n.data->>'cnpj_emit'                          AS cnpj_emit,
    n.data->>'nome_emit'                          AS emitente,
    (n.data->>'vNF')::NUMERIC(12,2)               AS valor_total,
    n.data->>'nProt'                              AS protocolo_autorizacao,
    n.data->>'cStat'                              AS status_sefaz,     -- 100=autorizada
    (n.data->>'dhRecbto')::timestamptz            AS data_recebimento_sefaz,
    -- Cancelamento
    (c.cancelamento_id IS NOT NULL)               AS cancelada,
    (c.data->>'dhEvento')::timestamptz            AS data_cancelamento,
    c.data->>'nProt'                              AS protocolo_cancelamento,
    c.data->>'xJust'                              AS justificativa_cancelamento,
    -- Conciliação com PDV
    (p.nota_id IS NOT NULL)                       AS tem_pdv,
    (p.data->>'Business Date')::DATE              AS data_venda_pdv,
    (p.data->>'Sub Total 1')::NUMERIC(12,2)       AS valor_pdv,
    p.data->>'Local Revenue Center Name'          AS ponto_venda,
    n.extracted_at
FROM carmel.nfe_raw_xmls n
LEFT JOIN carmel.nfe_raw_cancelamentos c ON c.data->>'chNFe' = n.nota_id
LEFT JOIN carmel.pdv_raw_notas p USING (nota_id);


-- PDV: notas fiscais emitidas pelo Simphony (uma linha por nota/comanda)
CREATE OR REPLACE VIEW carmel.v_pdv_notas AS
SELECT
    nota_id                                                        AS chave_nfe,
    data->>'hotel'                                                 AS hotel,
    (data->>'Business Date')::DATE                                 AS data_venda,
    data->>'FCR Invoice Number'                                    AS numero_nota,
    (data->>'Check Number')::BIGINT                                AS numero_comanda,
    (data->>'Sub Total 1')::NUMERIC(12,2)                         AS subtotal_1,
    (data->>'Sub Total 2')::NUMERIC(12,2)                         AS subtotal_2,
    (data->>'Sub Total 3')::NUMERIC(12,2)                         AS subtotal_3,
    (data->>'Sub Total 6')::NUMERIC(12,2)                         AS subtotal_6,
    (data->>'Tax Total 1')::NUMERIC(12,2)                         AS imposto_1,
    data->>'Local Revenue Center Name'                             AS ponto_venda,
    data->>'Revenue Center Master Name'                            AS ponto_venda_master,
    data->>'Invoice Data Info 5'                                   AS quarto,
    data->>'Invoice Data Info 6'                                   AS garcom,
    (data->>'Invoice Status')::SMALLINT                            AS status_nota,
    data->>'uwsName'                                               AS terminal_pdv,
    data->>'source_file'                                           AS arquivo_origem,
    extracted_at
FROM carmel.pdv_raw_notas;


-- ------------------------------------------------------------------------------
-- 3. VIEWS CONSOLIDADAS DE VENDAS
-- ------------------------------------------------------------------------------

-- Vendas: consolidação por nota (NF-e base + PDV + Fiscal agregado)
CREATE OR REPLACE VIEW carmel.v_vendas_notas AS
WITH fiscal_agg AS (
    SELECT
        chave_nfe,
        TRUE                                        AS tem_fiscal,
        COUNT(*)                                    AS qtd_itens,
        MODE() WITHIN GROUP (ORDER BY cfop)         AS cfop_principal
    FROM carmel.v_fiscal_lancamentos
    WHERE chave_nfe IS NOT NULL
    GROUP BY chave_nfe
)
SELECT
    n.nota_id,
    n.hotel,
    n.data_emissao,
    n.numero_nota,
    n.serie,
    n.modelo,
    n.valor_total,
    n.status_sefaz,
    n.protocolo_autorizacao,
    n.data_recebimento_sefaz,
    n.cancelada,
    n.data_cancelamento,
    n.justificativa_cancelamento,
    n.tem_pdv,
    n.data_venda_pdv,
    n.valor_pdv,
    n.ponto_venda,
    p.subtotal_1,
    p.subtotal_2,
    p.subtotal_3,
    p.subtotal_6,
    p.imposto_1,
    p.quarto,
    p.garcom,
    COALESCE(f.tem_fiscal, FALSE)                   AS tem_fiscal,
    COALESCE(f.qtd_itens, 0)                        AS qtd_itens,
    f.cfop_principal,
    CASE
        WHEN n.cancelada            THEN 'Cancelada'
        WHEN n.status_sefaz = '100' THEN 'Autorizada'
        ELSE                             'Pendente SEFAZ'
    END                                             AS status
FROM carmel.v_nfe_notas n
LEFT JOIN carmel.v_pdv_notas p ON p.chave_nfe = n.nota_id
LEFT JOIN fiscal_agg f          ON f.chave_nfe = n.nota_id;


-- Vendas: agregação diária por hotel
CREATE OR REPLACE VIEW carmel.v_vendas_diario AS
SELECT
    hotel,
    data_emissao::date                                      AS data,
    COUNT(*)                                                AS total_notas,
    COUNT(*) FILTER (WHERE status = 'Autorizada')           AS notas_autorizadas,
    COUNT(*) FILTER (WHERE status = 'Cancelada')            AS notas_canceladas,
    COUNT(*) FILTER (WHERE status = 'Pendente SEFAZ')       AS notas_pendentes,
    COALESCE(SUM(valor_total) FILTER (WHERE status != 'Cancelada'), 0) AS valor_total_dia,
    COUNT(*) FILTER (WHERE tem_pdv)                         AS notas_com_pdv,
    COUNT(*) FILTER (WHERE tem_fiscal)                      AS notas_com_fiscal
FROM carmel.v_vendas_notas
WHERE data_emissao IS NOT NULL
GROUP BY hotel, data_emissao::date;


-- ------------------------------------------------------------------------------
-- 4. FUNÇÕES AUXILIARES IMMUTABLE (necessárias para expression indexes)
-- ------------------------------------------------------------------------------

-- text::date e to_date() são STABLE no PostgreSQL (dependem de locale/DateStyle).
-- Wrappers IMMUTABLE são seguros aqui porque o formato dos dados é sempre ISO 8601 fixo.
CREATE OR REPLACE FUNCTION carmel.text_to_date_iso(text)
RETURNS date LANGUAGE sql IMMUTABLE STRICT AS
$$ SELECT to_date($1, 'YYYY-MM-DD') $$;

-- text::timestamptz é STABLE (depende de timezone). Wrapper interpreta como UTC.
-- Usado apenas para expression index; queries via view ainda retornam timestamptz correto.
CREATE OR REPLACE FUNCTION carmel.text_to_ts_utc(text)
RETURNS timestamptz LANGUAGE sql IMMUTABLE STRICT AS
$$ SELECT ($1::timestamp) AT TIME ZONE 'UTC' $$;


-- ------------------------------------------------------------------------------
-- 5. ÍNDICES PARA O RELATÓRIO DE DISCREPÂNCIAS
-- ------------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_nfe_raw_xmls_data_emissao
    ON carmel.nfe_raw_xmls ((carmel.text_to_ts_utc(data->>'dhEmi')));

CREATE INDEX IF NOT EXISTS idx_nfe_raw_xmls_hotel
    ON carmel.nfe_raw_xmls ((data->>'hotel'));

CREATE INDEX IF NOT EXISTS idx_nfe_raw_xmls_nNF_serie
    ON carmel.nfe_raw_xmls ((data->>'nNF'), (data->>'serie'));

CREATE INDEX IF NOT EXISTS idx_pdv_raw_notas_data_venda
    ON carmel.pdv_raw_notas ((carmel.text_to_date_iso(data->>'Business Date')));

CREATE INDEX IF NOT EXISTS idx_pdv_raw_notas_hotel
    ON carmel.pdv_raw_notas ((data->>'hotel'));

CREATE INDEX IF NOT EXISTS idx_fiscal_raw_lancamentos_numero_serie_empresa
    ON carmel.fiscal_raw_lancamentos ((data->>'ANNUMERODOCUMENTO'), (data->>'ANSERIE'), (data->>'FKEMPRESA'));

CREATE INDEX IF NOT EXISTS idx_nfe_raw_cancelamentos_chave
    ON carmel.nfe_raw_cancelamentos ((data->>'chNFe'));
