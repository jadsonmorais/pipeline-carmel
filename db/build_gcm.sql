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
