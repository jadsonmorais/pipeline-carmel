-- carmel.v_detalhe_chamados_failures fonte

CREATE OR REPLACE VIEW carmel.v_detalhe_chamados_failures
AS SELECT t.failure_id,
    a.attr ->> 'uuid'::text AS uuid,
    a.attr ->> 'status'::text AS status,
    a.attr ->> 'state_description'::text AS descricao_estado,
    (a.attr ->> 'problem_id'::text)::integer AS problem_id,
    a.attr ->> 'problem_name'::text AS problem_name,
    a.attr ->> 'description'::text AS descricao,
    a.attr ->> 'observations'::text AS observacoes,
    (a.attr ->> 'report_date'::text)::timestamp without time zone AS data_reporte,
    (a.attr ->> 'approved_date'::text)::timestamp without time zone AS data_aprovacao,
    (a.attr ->> 'started_date'::text)::timestamp without time zone AS data_inicio,
    (a.attr ->> 'completed_date'::text)::timestamp without time zone AS data_conclusao,
    (a.attr ->> 'last_status_change_date'::text)::timestamp without time zone AS data_ultima_alteracao,
    a.attr ->> 'client_name'::text AS client_name,
    a.attr ->> 'local_name'::text AS local_name,
        CASE
            WHEN (a.attr ->> 'local_name'::text) ~~* 'CARMEL CUMBUCO%'::text THEN 'CUMBUCO'::text
            WHEN (a.attr ->> 'local_name'::text) ~~* 'CARMEL TAÍBA%'::text THEN 'TAÍBA'::text
            WHEN (a.attr ->> 'local_name'::text) ~~* 'CARMEL CHARME%'::text THEN 'CHARME'::text
            WHEN (a.attr ->> 'local_name'::text) ~~* 'MAGNA PRAIA HOTEL%'::text THEN 'MAGNA'::text
            ELSE split_part(a.attr ->> 'local_name'::text, '-'::text, 1)
        END AS hotel,
    (a.attr -> 'time_statistics'::text) ->> 'time_to_approve'::text AS tempo_ate_aprovacao,
    (a.attr -> 'time_statistics'::text) ->> 'time_to_complete'::text AS tempo_ate_conclusao,
    (a.attr -> 'time_statistics'::text) ->> 'real_duration'::text AS duracao_real,
    (a.attr ->> 'manpower_cost'::text)::numeric AS custo_mao_de_obra,
    (a.attr ->> 'priority'::text)::integer AS prioridade_nivel,
    a.attr ->> 'priority_text'::text AS prioridade_texto,
    t.extracted_at AS data_sincronizacao
   FROM carmel.infraspeak_raw_failure t
     CROSS JOIN LATERAL ( SELECT COALESCE(t.data -> 'attributes'::text, (t.data -> 'data'::text) -> 'attributes'::text) AS attr) a;