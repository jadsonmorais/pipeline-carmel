-- ==============================================================================
-- PROJETO INFRASPEAK INTEGRATION - CARMEL HOTÉIS
-- ARQUIVO: build.sql
-- DESCRIÇÃO: Script de recriação completa do banco de dados (Tabelas, Views e Auditoria)
-- ATUALIZAÇÃO: Inclusão de event_id nas views analíticas e regras de backlog na auditoria
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- 0. SCHEMA
-- ------------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS carmel;

-- ------------------------------------------------------------------------------
-- 1. TABELAS BRUTAS (CAMADA BRONZE / RAW)
-- ------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS carmel.infraspeak_raw_failures (
    failure_id VARCHAR(255) PRIMARY KEY,
    data JSONB NOT NULL,
    extracted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS carmel.infraspeak_raw_failure_details (
    failure_id VARCHAR(255) PRIMARY KEY,
    data JSONB NOT NULL,
    extracted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS carmel.infraspeak_raw_works (
    work_id VARCHAR(255) PRIMARY KEY,
    data JSONB NOT NULL,
    extracted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS carmel.infraspeak_raw_work_details (
    work_id VARCHAR(255) PRIMARY KEY,
    data JSONB NOT NULL,
    extracted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS carmel.infraspeak_raw_scheduled_works (
    scheduled_work_id VARCHAR(255) PRIMARY KEY,
    data JSONB NOT NULL,
    extracted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS carmel.infraspeak_raw_scheduled_work_details (
    scheduled_work_id VARCHAR(255) PRIMARY KEY,
    data JSONB NOT NULL,
    extracted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS carmel.infraspeak_raw_operators (
    operator_id VARCHAR(255) PRIMARY KEY,
    data JSONB NOT NULL,
    extracted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);


-- ------------------------------------------------------------------------------
-- PDV / FISCAL / SEFAZ (CAMADA BRONZE / RAW)
-- ------------------------------------------------------------------------------

-- PDV Simphony: um registro por nota fiscal emitida no PDV
-- PK = chave NF-e 44 dígitos (Invoice Data Info 8) — chave de conciliação com SEFAZ
CREATE TABLE IF NOT EXISTS carmel.pdv_raw_notas (
    nota_id VARCHAR(44) PRIMARY KEY,
    data JSONB NOT NULL,
    extracted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);


-- ------------------------------------------------------------------------------
-- 2. VIEWS DE DIMENSÃO (CAMADA PRATA/OURO)
-- ------------------------------------------------------------------------------

-- DIMENSÃO: Operadores
CREATE OR REPLACE VIEW carmel.v_operadores AS
SELECT 
    operator_id,
    (data -> 'attributes') ->> 'full_name' AS nome_operador,
    (data -> 'attributes') ->> 'email' AS email,
    (data -> 'attributes') ->> 'phone' AS telefone,
    (data -> 'attributes') ->> 'cellphone' AS celular,
    (data -> 'attributes') ->> 'account' AS tipo_conta,
    ((data -> 'attributes') ->> 'cost_per_hour')::numeric AS custo_por_hora,
    ((data -> 'attributes') ->> 'warehouse_id')::integer AS warehouse_id,
    ((data -> 'attributes') ->> 'entity_id')::integer AS entity_id,
    (data -> 'attributes') ->> 'timezone' AS fuso_horario,
    ((data -> 'attributes') ->> 'is_from_network')::boolean AS is_from_network,
    ((data -> 'attributes') ->> 'created_at')::timestamp without time zone AS data_criacao_infraspeak,
    extracted_at AS data_ultima_sincronizacao
FROM carmel.infraspeak_raw_operators;

-- DIMENSÃO: Planos Mestres de Manutenção
CREATE OR REPLACE VIEW carmel.v_detalhe_planos_manutencao AS
WITH union_works AS (
    SELECT 
        d.work_id,
        ((d.data -> 'data') -> 'attributes') ->> 'name' AS nome_plano,
        ((d.data -> 'data') -> 'attributes') ->> 'description' AS descricao,
        ((d.data -> 'data') -> 'attributes') ->> 'type' AS tipo_plano,
        ((d.data -> 'data') -> 'attributes') ->> 'periodicity' AS periodicidade,
        (((d.data -> 'data') -> 'attributes') -> 'recurrence') ->> 'FREQ' AS frequencia,
        (((d.data -> 'data') -> 'attributes') -> 'recurrence') ->> 'INTERVAL' AS intervalo_frequencia,
        (((d.data -> 'data') -> 'attributes') -> 'recurrence') ->> 'DTSTART' AS data_inicio_recorrencia,
        (((d.data -> 'data') -> 'attributes') ->> 'client_id')::integer AS client_id,
        (((d.data -> 'data') -> 'attributes') ->> 'entity_id')::integer AS entity_id,
        (((d.data -> 'data') -> 'attributes') ->> 'supplier_id')::integer AS supplier_id,
        (((d.data -> 'data') -> 'attributes') ->> 'cost_center_id')::integer AS cost_center_id,
        (((d.data -> 'data') -> 'attributes') ->> 'work_type_id')::integer AS work_type_id,
        (((d.data -> 'data') -> 'attributes') ->> 'auto_close')::boolean AS auto_close,
        (((d.data -> 'data') -> 'attributes') ->> 'auto_pilot')::boolean AS auto_pilot,
        loc.local_name,
        loc.local_full_name,
        wt.work_type_name,
        d.extracted_at
    FROM carmel.infraspeak_raw_work_details d
    LEFT JOIN LATERAL (
        SELECT inc -> 'attributes' ->> 'name' AS local_name, inc -> 'attributes' ->> 'full_name' AS local_full_name
        FROM jsonb_array_elements(COALESCE(d.data -> 'included', '[]'::jsonb)) AS inc
        WHERE inc ->> 'type' IN ('location', 'location-folder') LIMIT 1
    ) loc ON true
    LEFT JOIN LATERAL (
        SELECT inc -> 'attributes' ->> 'name' AS work_type_name
        FROM jsonb_array_elements(COALESCE(d.data -> 'included', '[]'::jsonb)) AS inc
        WHERE inc ->> 'type' = 'work_type' LIMIT 1
    ) wt ON true
    
    UNION ALL
    
    SELECT 
        w.work_id,
        (w.data -> 'attributes') ->> 'name' AS nome_plano,
        (w.data -> 'attributes') ->> 'description' AS descricao,
        (w.data -> 'attributes') ->> 'type' AS tipo_plano,
        (w.data -> 'attributes') ->> 'periodicity' AS periodicidade,
        ((w.data -> 'attributes') -> 'recurrence') ->> 'FREQ' AS frequencia,
        ((w.data -> 'attributes') -> 'recurrence') ->> 'INTERVAL' AS intervalo_frequencia,
        ((w.data -> 'attributes') -> 'recurrence') ->> 'DTSTART' AS data_inicio_recorrencia,
        ((w.data -> 'attributes') ->> 'client_id')::integer AS client_id,
        ((w.data -> 'attributes') ->> 'entity_id')::integer AS entity_id,
        ((w.data -> 'attributes') ->> 'supplier_id')::integer AS supplier_id,
        ((w.data -> 'attributes') ->> 'cost_center_id')::integer AS cost_center_id,
        ((w.data -> 'attributes') ->> 'work_type_id')::integer AS work_type_id,
        ((w.data -> 'attributes') ->> 'auto_close')::boolean AS auto_close,
        ((w.data -> 'attributes') ->> 'auto_pilot')::boolean AS auto_pilot,
        loc.local_name,
        loc.local_full_name,
        wt.work_type_name,
        w.extracted_at
    FROM carmel.infraspeak_raw_works w
    LEFT JOIN LATERAL (
        SELECT inc -> 'attributes' ->> 'name' AS local_name, inc -> 'attributes' ->> 'full_name' AS local_full_name
        FROM jsonb_array_elements(COALESCE(w.data -> 'included', '[]'::jsonb)) AS inc
        WHERE inc ->> 'type' IN ('location', 'location-folder') LIMIT 1
    ) loc ON true
    LEFT JOIN LATERAL (
        SELECT inc -> 'attributes' ->> 'name' AS work_type_name
        FROM jsonb_array_elements(COALESCE(w.data -> 'included', '[]'::jsonb)) AS inc
        WHERE inc ->> 'type' = 'work_type' LIMIT 1
    ) wt ON true
    WHERE NOT EXISTS (SELECT 1 FROM carmel.infraspeak_raw_work_details d WHERE d.work_id = w.work_id)
)
SELECT 
    *,
    CASE
        WHEN local_full_name ~~* 'CARMEL CUMBUCO%' THEN 'CUMBUCO'
        WHEN local_full_name ~~* 'CARMEL TAÍBA%' THEN 'TAÍBA'
        WHEN local_full_name ~~* 'CARMEL CHARME%' THEN 'CHARME'
        WHEN local_full_name ~~* 'MAGNA PRAIA HOTEL%' THEN 'MAGNA'
        ELSE TRIM(BOTH FROM split_part(COALESCE(local_full_name, local_name), '-', 1))
    END AS hotel
FROM union_works;


-- ------------------------------------------------------------------------------
-- 3. VIEWS DE FATOS E DETALHES (CAMADA PRATA/OURO)
-- ------------------------------------------------------------------------------

-- FATO: Ocorrências (Scheduled Works)
CREATE OR REPLACE VIEW carmel.v_detalhe_ocorrencias AS
WITH union_scheduled AS (
    SELECT 
        d.scheduled_work_id,
        (((d.data -> 'data') -> 'attributes') ->> 'work_id')::integer AS work_id,
        ((d.data -> 'data') -> 'attributes') ->> 'state' AS status,
        (((d.data -> 'data') -> 'attributes') ->> 'visit')::integer AS visita_numero,
        (((d.data -> 'data') -> 'attributes') ->> 'entity_id')::integer AS entity_id,
        (((d.data -> 'data') -> 'attributes') ->> 'running')::boolean AS running,
        (((d.data -> 'data') -> 'attributes') ->> 'start_date')::timestamp without time zone AS data_agendada,
        (((d.data -> 'data') -> 'attributes') ->> 'original_start_date')::timestamp without time zone AS data_agendada_original,
        (((d.data -> 'data') -> 'attributes') ->> 'real_start_date')::timestamp without time zone AS data_inicio_real,
        (((d.data -> 'data') -> 'attributes') ->> 'completed_date')::timestamp without time zone AS data_conclusao,
        (((d.data -> 'data') -> 'attributes') ->> 'last_status_change_date')::timestamp without time zone AS data_ultima_alteracao,
        (((d.data -> 'data') -> 'attributes') ->> 'completed_percentage')::numeric AS percentual_conclusao,
        ((d.data -> 'data') -> 'attributes') ->> 'time_running' AS tempo_execucao_iso,
        ((d.data -> 'data') -> 'attributes') ->> 'manpower_duration' AS duracao_mao_de_obra_iso,
        (((d.data -> 'data') -> 'attributes') ->> 'manpower_cost')::numeric AS custo_mao_de_obra,
        (((d.data -> 'data') -> 'attributes') ->> 'started_by_id')::integer AS started_by_id,
        (((d.data -> 'data') -> 'attributes') ->> 'completed_by_id')::integer AS completed_by_id,
        loc.local_name,
        loc.local_full_name,
        w.nome_plano,
        wt.work_type_name,
        d.extracted_at
    FROM carmel.infraspeak_raw_scheduled_work_details d
    LEFT JOIN LATERAL (
        SELECT inc -> 'attributes' ->> 'name' AS local_name, inc -> 'attributes' ->> 'full_name' AS local_full_name
        FROM jsonb_array_elements(COALESCE(d.data -> 'included', '[]'::jsonb)) AS inc
        WHERE inc ->> 'type' IN ('location', 'location-folder') LIMIT 1
    ) loc ON true
    LEFT JOIN LATERAL (
        SELECT inc -> 'attributes' ->> 'name' AS nome_plano
        FROM jsonb_array_elements(COALESCE(d.data -> 'included', '[]'::jsonb)) AS inc
        WHERE inc ->> 'type' = 'work' LIMIT 1
    ) w ON true
    LEFT JOIN LATERAL (
        SELECT inc -> 'attributes' ->> 'name' AS work_type_name
        FROM jsonb_array_elements(COALESCE(d.data -> 'included', '[]'::jsonb)) AS inc
        WHERE inc ->> 'type' = 'work_type' LIMIT 1
    ) wt ON true
    
    UNION ALL
    
    SELECT 
        sw.scheduled_work_id,
        ((sw.data -> 'attributes') ->> 'work_id')::integer AS work_id,
        (sw.data -> 'attributes') ->> 'state' AS status,
        ((sw.data -> 'attributes') ->> 'visit')::integer AS visita_numero,
        ((sw.data -> 'attributes') ->> 'entity_id')::integer AS entity_id,
        ((sw.data -> 'attributes') ->> 'running')::boolean AS running,
        ((sw.data -> 'attributes') ->> 'start_date')::timestamp without time zone AS data_agendada,
        ((sw.data -> 'attributes') ->> 'original_start_date')::timestamp without time zone AS data_agendada_original,
        ((sw.data -> 'attributes') ->> 'real_start_date')::timestamp without time zone AS data_inicio_real,
        ((sw.data -> 'attributes') ->> 'completed_date')::timestamp without time zone AS data_conclusao,
        ((sw.data -> 'attributes') ->> 'last_status_change_date')::timestamp without time zone AS data_ultima_alteracao,
        ((sw.data -> 'attributes') ->> 'completed_percentage')::numeric AS percentual_conclusao,
        (sw.data -> 'attributes') ->> 'time_running' AS tempo_execucao_iso,
        (sw.data -> 'attributes') ->> 'manpower_duration' AS duracao_mao_de_obra_iso,
        ((sw.data -> 'attributes') ->> 'manpower_cost')::numeric AS custo_mao_de_obra,
        ((sw.data -> 'attributes') ->> 'started_by_id')::integer AS started_by_id,
        ((sw.data -> 'attributes') ->> 'completed_by_id')::integer AS completed_by_id,
        loc.local_name,
        loc.local_full_name,
        w.nome_plano,
        wt.work_type_name,
        sw.extracted_at
    FROM carmel.infraspeak_raw_scheduled_works sw
    LEFT JOIN LATERAL (
        SELECT inc -> 'attributes' ->> 'name' AS local_name, inc -> 'attributes' ->> 'full_name' AS local_full_name
        FROM jsonb_array_elements(COALESCE(sw.data -> 'included', '[]'::jsonb)) AS inc
        WHERE inc ->> 'type' IN ('location', 'location-folder') LIMIT 1
    ) loc ON true
    LEFT JOIN LATERAL (
        SELECT inc -> 'attributes' ->> 'name' AS nome_plano
        FROM jsonb_array_elements(COALESCE(sw.data -> 'included', '[]'::jsonb)) AS inc
        WHERE inc ->> 'type' = 'work' LIMIT 1
    ) w ON true
    LEFT JOIN LATERAL (
        SELECT inc -> 'attributes' ->> 'name' AS work_type_name
        FROM jsonb_array_elements(COALESCE(sw.data -> 'included', '[]'::jsonb)) AS inc
        WHERE inc ->> 'type' = 'work_type' LIMIT 1
    ) wt ON true
    WHERE NOT EXISTS (SELECT 1 FROM carmel.infraspeak_raw_scheduled_work_details d WHERE d.scheduled_work_id = sw.scheduled_work_id)
)
SELECT 
    u.scheduled_work_id,
    u.work_id,
    u.status,
    u.visita_numero,
    u.entity_id,
    u.running,
    u.data_agendada,
    u.data_agendada_original,
    u.data_inicio_real,
    u.data_conclusao,
    u.data_ultima_alteracao,
    u.percentual_conclusao,
    u.tempo_execucao_iso,
    u.duracao_mao_de_obra_iso,
    u.custo_mao_de_obra,
    u.started_by_id,
    u.completed_by_id,
    COALESCE(u.nome_plano, p.nome_plano) AS nome_plano,
    COALESCE(u.work_type_name, p.work_type_name) AS work_type_name,
    COALESCE(u.local_name, p.local_name) AS local_name,
    COALESCE(u.local_full_name, p.local_full_name) AS local_full_name,
    CASE
        WHEN COALESCE(u.local_full_name, p.local_full_name) ~~* 'CARMEL CUMBUCO%' THEN 'CUMBUCO'
        WHEN COALESCE(u.local_full_name, p.local_full_name) ~~* 'CARMEL TAÍBA%' THEN 'TAÍBA'
        WHEN COALESCE(u.local_full_name, p.local_full_name) ~~* 'CARMEL CHARME%' THEN 'CHARME'
        WHEN COALESCE(u.local_full_name, p.local_full_name) ~~* 'MAGNA PRAIA HOTEL%' THEN 'MAGNA'
        ELSE TRIM(BOTH FROM split_part(COALESCE(u.local_full_name, u.local_name, p.local_full_name, p.local_name), '-', 1))
    END AS hotel,
    u.extracted_at
FROM union_scheduled u
LEFT JOIN carmel.v_detalhe_planos_manutencao p ON u.work_id = p.work_id;


-- ------------------------------------------------------------------------------
-- 4. VIEWS ANALÍTICAS (CÁLCULO DE TEMPO E PRODUTIVIDADE)
-- ------------------------------------------------------------------------------

-- ANALÍTICA: Apontamento de Horas - Chamados
CREATE OR REPLACE VIEW carmel.v_trabalho_analitico_operador_chamados AS 
WITH events AS (
    SELECT 
        failure_id,
        (val ->> 'id')::bigint AS event_id,
        ((val -> 'attributes') ->> 'operator_id')::integer AS timesheet_owner_id
    FROM carmel.infraspeak_raw_failure_details,
    LATERAL jsonb_array_elements(COALESCE(data -> 'included', '[]'::jsonb)) val
    WHERE val ->> 'type' = 'event'
),
registries AS (
    SELECT 
        failure_id,
        ((val -> 'attributes') ->> 'event_id')::bigint AS event_id,
        (val -> 'attributes') ->> 'action' AS action,
        ((val -> 'attributes') ->> 'date')::timestamp without time zone AS event_time,
        (val ->> 'id')::bigint AS event_registry_id
    FROM carmel.infraspeak_raw_failure_details,
    LATERAL jsonb_array_elements(COALESCE(data -> 'included', '[]'::jsonb)) val
    WHERE val ->> 'type' = 'event_registry'
),
raw_events AS (
    SELECT 
        r.failure_id,
        r.event_id,
        e.timesheet_owner_id AS operator_id,
        r.action,
        r.event_time,
        r.event_registry_id
    FROM registries r
    JOIN events e ON r.failure_id = e.failure_id AND r.event_id = e.event_id
    WHERE e.timesheet_owner_id IS NOT NULL
),
ordered_events AS (
    SELECT 
        failure_id,
        event_id,
        operator_id,
        action,
        event_time,
        lead(event_time) OVER (PARTITION BY failure_id, event_id ORDER BY event_time, event_registry_id) AS next_time,
        lead(action) OVER (PARTITION BY failure_id, event_id ORDER BY event_time, event_registry_id) AS next_action
    FROM raw_events
)
SELECT 
    failure_id,
    operator_id,
    action AS status_inicio,
    next_action AS status_fim,
    event_time AS data_inicio,
    next_time AS data_fim,
    next_time - event_time AS duracao,
    EXTRACT(epoch FROM next_time - event_time) / 3600::numeric AS horas_decimais
FROM ordered_events
WHERE action IN ('STARTED', 'RESUMED') 
  AND next_action IN ('PAUSED', 'COMPLETED') 
  AND next_time IS NOT NULL;


-- ANALÍTICA: Apontamento de Horas - Ocorrências (Preventivas)
CREATE OR REPLACE VIEW carmel.v_trabalho_analitico_operador_ocorrencias AS 
WITH events AS (
    -- 1. Descobre os eventos válidos (Filtra o Evento Mestre Fantasma)
    SELECT 
        scheduled_work_id,
        (val ->> 'id')::bigint AS event_id,
        ((val -> 'attributes') ->> 'operator_id')::integer AS timesheet_owner_id
    FROM carmel.infraspeak_raw_scheduled_work_details,
    LATERAL jsonb_array_elements(COALESCE(data -> 'included', '[]'::jsonb)) val
    WHERE val ->> 'type' = 'event'
),
registries AS (
    -- 2. Puxa todos os registros de botões clicados
    SELECT 
        scheduled_work_id,
        ((val -> 'attributes') ->> 'event_id')::bigint AS event_id,
        (val -> 'attributes') ->> 'action' AS action,
        ((val -> 'attributes') ->> 'date')::timestamp without time zone AS event_time,
        (val ->> 'id')::bigint AS event_registry_id
    FROM carmel.infraspeak_raw_scheduled_work_details,
    LATERAL jsonb_array_elements(COALESCE(data -> 'included', '[]'::jsonb)) val
    WHERE val ->> 'type' = 'event_registry'
),
raw_events AS (
    -- 3. Cruza os dados: O registro só passa se o evento pai tiver um dono humano!
    SELECT 
        r.scheduled_work_id,
        r.event_id,
        e.timesheet_owner_id AS operator_id, -- Assume o dono real da folha de horas
        r.action,
        r.event_time,
        r.event_registry_id
    FROM registries r
    JOIN events e ON r.scheduled_work_id = e.scheduled_work_id AND r.event_id = e.event_id
    WHERE e.timesheet_owner_id IS NOT NULL
),
ordered_events AS (
    SELECT 
        scheduled_work_id,
        event_id,
        operator_id,
        action,
        event_time,
        lead(event_time) OVER (PARTITION BY scheduled_work_id, event_id ORDER BY event_time, event_registry_id) AS next_time,
        lead(action) OVER (PARTITION BY scheduled_work_id, event_id ORDER BY event_time, event_registry_id) AS next_action
    FROM raw_events
)
SELECT 
    scheduled_work_id,
    operator_id,
    action AS status_inicio,
    next_action AS status_fim,
    event_time AS data_inicio,
    next_time AS data_fim,
    next_time - event_time AS duracao,
    EXTRACT(epoch FROM next_time - event_time) / 3600::numeric AS horas_decimais
FROM ordered_events
WHERE action IN ('STARTED', 'RESUMED') 
  AND next_action IN ('PAUSED', 'COMPLETED') 
  AND next_time IS NOT NULL;


-- ------------------------------------------------------------------------------
-- 4. VIEWS PDV / FISCAL / SEFAZ
-- ------------------------------------------------------------------------------

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
-- 5. QUERIES DE AUDITORIA E SAÚDE DO BANCO (EXECUTAR MANUALMENTE QUANDO NECESSÁRIO)
-- ------------------------------------------------------------------------------
/*
-- A. Auditoria de Ocorrências (Scheduled Works) Concluídas >= 2026 sem detalhes
-- Nota: Contempla backlog de anos anteriores resolvido no ano atual
SELECT scheduled_work_id 
FROM carmel.infraspeak_raw_scheduled_works 
WHERE scheduled_work_id NOT IN (SELECT scheduled_work_id FROM carmel.infraspeak_raw_scheduled_work_details)
  AND UPPER(data -> 'attributes' ->> 'state') = 'COMPLETED'
  AND (
       (data -> 'attributes' ->> 'start_date')::date >= '2026-01-01'
       OR 
       (data -> 'attributes' ->> 'completed_date')::date >= '2026-01-01'
  );

-- B. Auditoria de Chamados (Failures) criados >= 2026 sem detalhes
SELECT failure_id 
FROM carmel.infraspeak_raw_failures 
WHERE failure_id NOT IN (SELECT failure_id FROM carmel.infraspeak_raw_failure_details)
  AND (data -> 'attributes' ->> 'date')::date >= '2026-01-01';

-- C. Auditoria de Planos Mestres (Works) atualizados >= 2026 sem detalhes
SELECT work_id 
FROM carmel.infraspeak_raw_works 
WHERE work_id NOT IN (SELECT work_id FROM carmel.infraspeak_raw_work_details)
  AND (data -> 'attributes' ->> 'updated_at')::date >= '2026-01-01';
*/