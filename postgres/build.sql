-- Tables

CREATE TABLE carmel.infraspeak_raw_works (
	work_id int4 NOT NULL,
	"data" jsonb NULL,
	extracted_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT infraspeak_raw_works_pkey PRIMARY KEY (work_id)
);

CREATE TABLE carmel.infraspeak_raw_work_details (
	work_id int4 NOT NULL,
	"data" jsonb NULL,
	extracted_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT infraspeak_raw_work_details_pkey PRIMARY KEY (work_id)
);

CREATE TABLE carmel.infraspeak_raw_failures (
	failure_id int4 NOT NULL,
	"data" jsonb NULL,
	extracted_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT infraspeak_raw_failures_pkey PRIMARY KEY (failure_id)
);

CREATE TABLE carmel.infraspeak_raw_failure_details (
	failure_id int4 NOT NULL,
	"data" jsonb NULL,
	extracted_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT infraspeak_raw_failure_details_pkey PRIMARY KEY (failure_id)
);

-- Views

-- carmel.v_trabalho_analitico_operador_chamados fonte

CREATE OR REPLACE VIEW carmel.v_trabalho_analitico_operador_chamados
AS WITH raw_events AS (
         SELECT infraspeak_raw_failure_details.failure_id,
            ((evt.value -> 'attributes'::text) ->> 'operator_id'::text)::integer AS operator_id,
            (evt.value -> 'attributes'::text) ->> 'action'::text AS action,
            ((evt.value -> 'attributes'::text) ->> 'date'::text)::timestamp without time zone AS event_time
           FROM carmel.infraspeak_raw_failure_details,
            LATERAL jsonb_array_elements(infraspeak_raw_failure_details.data -> 'included'::text) evt(value)
          WHERE (evt.value ->> 'type'::text) = 'event_registry'::text
        ), ordered_events AS (
         SELECT raw_events.failure_id,
            raw_events.operator_id,
            raw_events.action,
            raw_events.event_time,
            lead(raw_events.event_time) OVER (PARTITION BY raw_events.failure_id, raw_events.operator_id ORDER BY raw_events.event_time) AS next_time,
            lead(raw_events.action) OVER (PARTITION BY raw_events.failure_id, raw_events.operator_id ORDER BY raw_events.event_time) AS next_action
           FROM raw_events
        )
 SELECT ordered_events.failure_id,
    ordered_events.operator_id,
    ordered_events.action AS status_inicio,
    ordered_events.next_action AS status_fim,
    ordered_events.event_time AS data_inicio,
    ordered_events.next_time AS data_fim,
    ordered_events.next_time - ordered_events.event_time AS duracao,
    EXTRACT(epoch FROM ordered_events.next_time - ordered_events.event_time) / 3600::numeric AS horas_decimais
   FROM ordered_events
  WHERE (ordered_events.action = ANY (ARRAY['STARTED'::text, 'RESUMED'::text])) AND (ordered_events.next_action = ANY (ARRAY['PAUSED'::text, 'COMPLETED'::text])) AND ordered_events.next_time IS NOT NULL;

-- carmel.v_trabalho_analitico_operador_ocorrencias fonte

CREATE OR REPLACE VIEW carmel.v_trabalho_analitico_operador_ocorrencias
AS WITH raw_events AS (
         SELECT infraspeak_raw_work_details.work_id,
            ((evt.value -> 'attributes'::text) ->> 'operator_id'::text)::integer AS operator_id,
            (evt.value -> 'attributes'::text) ->> 'action'::text AS action,
            ((evt.value -> 'attributes'::text) ->> 'date'::text)::timestamp without time zone AS event_time
           FROM carmel.infraspeak_raw_work_details,
            LATERAL jsonb_array_elements(infraspeak_raw_work_details.data -> 'included'::text) evt(value)
          WHERE (evt.value ->> 'type'::text) = 'event_registry'::text
        ), ordered_events AS (
         SELECT raw_events.work_id,
            raw_events.operator_id,
            raw_events.action,
            raw_events.event_time,
            lead(raw_events.event_time) OVER (PARTITION BY raw_events.work_id, raw_events.operator_id ORDER BY raw_events.event_time) AS next_time,
            lead(raw_events.action) OVER (PARTITION BY raw_events.work_id, raw_events.operator_id ORDER BY raw_events.event_time) AS next_action
           FROM raw_events
        )
 SELECT ordered_events.work_id AS ocorrencia_id,
    ordered_events.operator_id,
    ordered_events.action AS status_inicio,
    ordered_events.next_action AS status_fim,
    ordered_events.event_time AS data_inicio,
    ordered_events.next_time AS data_fim,
    ordered_events.next_time - ordered_events.event_time AS duracao,
    EXTRACT(epoch FROM ordered_events.next_time - ordered_events.event_time) / 3600::numeric AS horas_decimais
   FROM ordered_events
  WHERE (ordered_events.action = ANY (ARRAY['STARTED'::text, 'RESUMED'::text])) AND (ordered_events.next_action = ANY (ARRAY['PAUSED'::text, 'COMPLETED'::text])) AND ordered_events.next_time IS NOT NULL;


CREATE OR REPLACE VIEW carmel.v_detalhe_chamados_failures AS
WITH union_failures AS (
    -- Parte 1: Dados da tabela de detalhes (Caminho: data -> data -> attributes)
    SELECT 
        failure_id,
        (data -> 'data' -> 'attributes' ->> 'uuid') AS uuid,
        (data -> 'data' -> 'attributes' ->> 'status') AS status,
        (data -> 'data' -> 'attributes' ->> 'state_description') AS descricao_estado,
        ((data -> 'data' -> 'attributes' ->> 'problem_id')::integer) AS problem_id,
        (data -> 'data' -> 'attributes' ->> 'problem_name') AS problem_name,
        (data -> 'data' -> 'attributes' ->> 'description') AS descricao,
        (data -> 'data' -> 'attributes' ->> 'observations') AS observacoes,
        ((data -> 'data' -> 'attributes' ->> 'report_date')::timestamp) AS data_reporte,
        ((data -> 'data' -> 'attributes' ->> 'approved_date')::timestamp) AS data_aprovacao,
        ((data -> 'data' -> 'attributes' ->> 'started_date')::timestamp) AS data_inicio,
        ((data -> 'data' -> 'attributes' ->> 'completed_date')::timestamp) AS data_conclusao,
        ((data -> 'data' -> 'attributes' ->> 'last_status_change_date')::timestamp) AS data_ultima_alteracao,
        (data -> 'data' -> 'attributes' ->> 'client_name') AS client_name,
        (data -> 'data' -> 'attributes' ->> 'local_name') AS local_name,
        ((data -> 'data' -> 'attributes' -> 'time_statistics' ->> 'time_to_approve')) AS tempo_ate_aprovacao,
        ((data -> 'data' -> 'attributes' -> 'time_statistics' ->> 'time_to_complete')) AS tempo_ate_conclusao,
        ((data -> 'data' -> 'attributes' -> 'time_statistics' ->> 'real_duration')) AS duracao_real,
        ((data -> 'data' -> 'attributes' ->> 'manpower_cost')::numeric) AS custo_mao_de_obra,
        ((data -> 'data' -> 'attributes' ->> 'priority')::integer) AS prioridade_nivel,
        (data -> 'data' -> 'attributes' ->> 'priority_text') AS prioridade_texto,
        extracted_at
    FROM carmel.infraspeak_raw_failure_details

    UNION ALL

    -- Parte 2: Dados da tabela bulk (Caminho: data -> attributes)
    -- Apenas se o ID não existir na tabela de detalhes para evitar duplicidade
    SELECT 
        failure_id,
        (data -> 'attributes' ->> 'uuid') AS uuid,
        (data -> 'attributes' ->> 'status') AS status,
        (data -> 'attributes' ->> 'state_description') AS descricao_estado,
        ((data -> 'attributes' ->> 'problem_id')::integer) AS problem_id,
        (data -> 'attributes' ->> 'problem_name') AS problem_name,
        (data -> 'attributes' ->> 'description') AS descricao,
        (data -> 'attributes' ->> 'observations') AS observacoes,
        ((data -> 'attributes' ->> 'report_date')::timestamp) AS data_reporte,
        ((data -> 'attributes' ->> 'approved_date')::timestamp) AS data_aprovacao,
        ((data -> 'attributes' ->> 'started_date')::timestamp) AS data_inicio,
        ((data -> 'attributes' ->> 'completed_date')::timestamp) AS data_conclusao,
        ((data -> 'attributes' ->> 'last_status_change_date')::timestamp) AS data_ultima_alteracao,
        (data -> 'attributes' ->> 'client_name') AS client_name,
        (data -> 'attributes' ->> 'local_name') AS local_name,
        ((data -> 'attributes' -> 'time_statistics' ->> 'time_to_approve')) AS tempo_ate_aprovacao,
        ((data -> 'attributes' -> 'time_statistics' ->> 'time_to_complete')) AS tempo_ate_conclusao,
        ((data -> 'attributes' -> 'time_statistics' ->> 'real_duration')) AS duracao_real,
        ((data -> 'attributes' ->> 'manpower_cost')::numeric) AS custo_mao_de_obra,
        ((data -> 'attributes' ->> 'priority')::integer) AS prioridade_nivel,
        (data -> 'attributes' ->> 'priority_text') AS prioridade_texto,
        extracted_at
    FROM carmel.infraspeak_raw_failures f
    WHERE NOT EXISTS (SELECT 1 FROM carmel.infraspeak_raw_failure_details d WHERE d.failure_id = f.failure_id)
)
SELECT 
     *,
    -- Nível 1: Valor antes do primeiro '-'
    TRIM(split_part(problem_name, '-', 1)) AS tipo_problema,
    
    -- Nível 2: Valor até o segundo '-' (ex: Parte1 - Parte2)
    CASE 
        WHEN problem_name LIKE '%-%-%' THEN 
            TRIM(split_part(problem_name, '-', 1)) || ' - ' || TRIM(split_part(problem_name, '-', 2))
        ELSE problem_name 
    END AS tipo_problema_detalhado,

    -- Coluna: Identificação do Hotel baseada no local_name [9-11]
    CASE
        WHEN local_name ILIKE 'CARMEL CUMBUCO%' THEN 'CUMBUCO'
        WHEN local_name ILIKE 'CARMEL TAÍBA%' THEN 'TAÍBA'
        WHEN local_name ILIKE 'CARMEL CHARME%' THEN 'CHARME'
        WHEN local_name ILIKE 'MAGNA PRAIA HOTEL%' THEN 'MAGNA'
        ELSE TRIM(split_part(local_name, '-', 1))
    END AS hotel
FROM union_failures; 