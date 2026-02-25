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

-- carmel.v_detalhe_chamados_failures fonte

CREATE OR REPLACE VIEW carmel.v_detalhe_chamados_failures
AS SELECT infraspeak_raw_failure_details.failure_id,
    (infraspeak_raw_failure_details.data -> 'attributes'::text) ->> 'uuid'::text AS uuid,
    (infraspeak_raw_failure_details.data -> 'attributes'::text) ->> 'status'::text AS status,
    (infraspeak_raw_failure_details.data -> 'attributes'::text) ->> 'state_description'::text AS descricao_estado,
    ((infraspeak_raw_failure_details.data -> 'attributes'::text) ->> 'problem_id'::text)::integer AS problem_id,
    (infraspeak_raw_failure_details.data -> 'attributes'::text) ->> 'problem_name'::text AS problem_name,
    (infraspeak_raw_failure_details.data -> 'attributes'::text) ->> 'description'::text AS descricao,
    (infraspeak_raw_failure_details.data -> 'attributes'::text) ->> 'observations'::text AS observacoes,
    ((infraspeak_raw_failure_details.data -> 'attributes'::text) ->> 'report_date'::text)::timestamp without time zone AS data_reporte,
    ((infraspeak_raw_failure_details.data -> 'attributes'::text) ->> 'approved_date'::text)::timestamp without time zone AS data_aprovacao,
    ((infraspeak_raw_failure_details.data -> 'attributes'::text) ->> 'started_date'::text)::timestamp without time zone AS data_inicio,
    ((infraspeak_raw_failure_details.data -> 'attributes'::text) ->> 'completed_date'::text)::timestamp without time zone AS data_conclusao,
    ((infraspeak_raw_failure_details.data -> 'attributes'::text) ->> 'last_status_change_date'::text)::timestamp without time zone AS data_ultima_alteracao,
    (infraspeak_raw_failure_details.data -> 'attributes'::text) ->> 'client_name'::text AS client_name,
    (infraspeak_raw_failure_details.data -> 'attributes'::text) ->> 'local_name'::text AS local_name,
    ((infraspeak_raw_failure_details.data -> 'attributes'::text) -> 'time_statistics'::text) ->> 'time_to_approve'::text AS tempo_ate_aprovacao,
    ((infraspeak_raw_failure_details.data -> 'attributes'::text) -> 'time_statistics'::text) ->> 'time_to_complete'::text AS tempo_ate_conclusao,
    ((infraspeak_raw_failure_details.data -> 'attributes'::text) -> 'time_statistics'::text) ->> 'real_duration'::text AS duracao_real,
    ((infraspeak_raw_failure_details.data -> 'attributes'::text) ->> 'manpower_cost'::text)::numeric AS custo_mao_de_obra,
    ((infraspeak_raw_failure_details.data -> 'attributes'::text) ->> 'priority'::text)::integer AS prioridade_nivel,
    (infraspeak_raw_failure_details.data -> 'attributes'::text) ->> 'priority_text'::text AS prioridade_texto,
    infraspeak_raw_failure_details.extracted_at AS data_sincronizacao
   FROM carmel.infraspeak_raw_failure_details;

-- carmel.v_detalhe_scheduled_works fonte

CREATE OR REPLACE VIEW carmel.v_detalhe_scheduled_works
AS WITH work_parent AS (
         SELECT infraspeak_raw_work_details.work_id,
            e.value -> 'attributes'::text AS attr,
            e.value ->> 'id'::text AS parent_work_id
           FROM carmel.infraspeak_raw_work_details,
            LATERAL jsonb_array_elements(infraspeak_raw_work_details.data -> 'included'::text) e(value)
          WHERE (e.value ->> 'type'::text) = 'work'::text
        ), audit_info AS (
         SELECT infraspeak_raw_work_details.work_id,
            e.value -> 'attributes'::text AS attr,
            e.value ->> 'id'::text AS audit_id
           FROM carmel.infraspeak_raw_work_details,
            LATERAL jsonb_array_elements(infraspeak_raw_work_details.data -> 'included'::text) e(value)
          WHERE (e.value ->> 'type'::text) = 'audit_stats'::text
        )
 SELECT w.work_id AS id,
    ((w.data -> 'data'::text) -> 'attributes'::text) ->> 'state'::text AS state,
    (((w.data -> 'data'::text) -> 'attributes'::text) ->> 'completed_date'::text)::timestamp without time zone AS completed_date,
    (((w.data -> 'data'::text) -> 'attributes'::text) ->> 'original_start_date'::text)::timestamp without time zone AS original_start_date,
    (((w.data -> 'data'::text) -> 'attributes'::text) ->> 'start_date'::text)::timestamp without time zone AS start_date,
    (((w.data -> 'data'::text) -> 'attributes'::text) ->> 'real_start_date'::text)::timestamp without time zone AS real_start_date,
    (((w.data -> 'data'::text) -> 'attributes'::text) ->> 'completed_percentage'::text)::numeric AS completed_percentage,
    ((w.data -> 'data'::text) -> 'attributes'::text) ->> 'observation'::text AS observation,
    (((w.data -> 'data'::text) -> 'attributes'::text) ->> 'confirmed'::text)::boolean AS confirmed,
    wp.attr ->> 'name'::text AS "work_data.attributes.name",
    wp.attr ->> 'description'::text AS "work_data.attributes.description",
    wp.parent_work_id AS "relationships.work.data.id",
    wp.attr ->> 'periodicity'::text AS "work_data.attributes.periodicity",
    (wp.attr ->> 'client_id'::text)::integer AS "work_data.attributes.client_id",
    (wp.attr -> 'recurrence'::text) ->> 'FREQ'::text AS "work_data.attributes.recurrence.FREQ",
    (wp.attr ->> 'auto_pilot'::text)::boolean AS "work_data.attributes.auto_pilot",
    (wp.attr ->> 'auto_close'::text)::boolean AS "work_data.attributes.auto_close",
    (wp.attr ->> 'is_protected'::text)::boolean AS "work_data.attributes.is_protected",
    ( SELECT string_agg((e.value -> 'attributes'::text) ->> 'full_name'::text, ', '::text) AS string_agg
           FROM jsonb_array_elements(w.data -> 'included'::text) e(value)
          WHERE (e.value ->> 'type'::text) = 'location'::text) AS "Locais",
    ( SELECT (e.value -> 'attributes'::text) ->> 'name'::text
           FROM jsonb_array_elements(w.data -> 'included'::text) e(value)
          WHERE (e.value ->> 'type'::text) = 'work_type'::text
         LIMIT 1) AS "Tipo de Trabalho",
    (ai.attr ->> 'max_value'::text)::numeric AS max_value,
    (ai.attr ->> 'current_value'::text)::numeric AS current_value,
    (ai.attr ->> 'category_id'::text)::integer AS category_id,
    wp.attr ->> 'type'::text AS type_audit,
    ( SELECT (e.value -> 'attributes'::text) ->> 'name'::text
           FROM jsonb_array_elements(w.data -> 'included'::text) e(value)
          WHERE (e.value ->> 'type'::text) = 'category'::text
         LIMIT 1) AS name_category
   FROM carmel.infraspeak_raw_work_details w
     LEFT JOIN work_parent wp ON w.work_id = wp.work_id
     LEFT JOIN audit_info ai ON w.work_id = ai.work_id;

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