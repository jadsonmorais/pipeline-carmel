-- ==============================================================================
-- MIGRAÇÃO: Materializar v_fiscal_lancamentos
-- Rodar UMA VEZ para criar mv_fiscal_lancamentos e recriar views dependentes.
-- ==============================================================================

-- 1. Derruba MV atual (se existir) + views dependentes
DROP MATERIALIZED VIEW IF EXISTS carmel.mv_fiscal_lancamentos CASCADE;

-- 2. Cria MV já populada (o join pesado roda aqui — pode levar alguns minutos)
CREATE MATERIALIZED VIEW carmel.mv_fiscal_lancamentos AS
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
    n.nota_id                                         AS chave_nfe,
    n.data->>'cStat'                                  AS status_sefaz,
    n.data->>'nProt'                                  AS protocolo_sefaz,
    (n.data->>'dhRecbto')::timestamptz                AS recebimento_sefaz,
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
) n ON true;

-- 3. Índice único (necessário para REFRESH CONCURRENTLY no futuro)
CREATE UNIQUE INDEX idx_mv_fiscal_lancamentos_pk
    ON carmel.mv_fiscal_lancamentos (lancamento_id);

-- 4. Wrapper fino — mantém compatibilidade com todo código existente
CREATE OR REPLACE VIEW carmel.v_fiscal_lancamentos AS
SELECT * FROM carmel.mv_fiscal_lancamentos;

-- 5. Recria views que o CASCADE derrubou
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

-- 6. Verificação
SELECT COUNT(*) AS total_mv FROM carmel.mv_fiscal_lancamentos;
