-- =============================================================================
-- GCM (Guest Check Management) — Oracle Simphony POS
-- Relatório de vendas detalhadas por data de lançamento
-- =============================================================================

CREATE TABLE IF NOT EXISTS carmel.gcm_raw_line_items (
    line_item_id BIGINT PRIMARY KEY,
    data         JSONB NOT NULL,
    extracted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Índices para filtros comuns
CREATE INDEX IF NOT EXISTS idx_gcm_business_date
    ON carmel.gcm_raw_line_items ((data->>'businessDate'));

CREATE INDEX IF NOT EXISTS idx_gcm_hotel
    ON carmel.gcm_raw_line_items ((data->>'locationRef'));

CREATE INDEX IF NOT EXISTS idx_gcm_order_type
    ON carmel.gcm_raw_line_items ((data->>'orderTypeName'));

CREATE INDEX IF NOT EXISTS idx_gcm_guest_check
    ON carmel.gcm_raw_line_items (((data->>'guestCheckID')::bigint));

-- =============================================================================
-- View de validação: itens de linha com campos extraídos do JSONB
-- =============================================================================
CREATE OR REPLACE VIEW carmel.v_gcm_line_items AS
SELECT
    line_item_id,
    data->>'businessDate'                                   AS data_negocio,
    data->>'locationName'                                   AS hotel,
    data->>'orderTypeName'                                  AS tipo_pedido,
    (data->>'guestCheckID')::bigint                         AS comanda_id,
    (data->>'checkNum')::int                                AS numero_comanda,
    data->>'revenueCenterName'                              AS centro_receita,
    data->>'majorGroupName'                                 AS grupo,
    data->>'familyGroupName'                                AS subgrupo,
    (data->>'menuItemNum')::int                             AS codigo_produto,
    data->>'menuItemName1'                                  AS produto,
    (data->>'lineCount')::int                               AS quantidade,
    (data->>'lineTotal')::numeric                           AS valor_total,
    CASE WHEN (data->>'lineCount')::int > 0
         THEN (data->>'lineTotal')::numeric / (data->>'lineCount')::int
         ELSE 0
    END                                                     AS valor_unitario,
    (data->>'transactionEmployeeFirstName')
        || ' ' || (data->>'transactionEmployeeLastName')    AS colaborador,
    (data->>'isVoidFlag')::int = 1                          AS cancelado,
    (data->>'transactionDateTime')::timestamp               AS dt_transacao,
    extracted_at
FROM carmel.gcm_raw_line_items;
