-- DIAGNÓSTICO SOMENTE LEITURA do schema ATUAL, PostgreSQL 18.
-- Não seleciona tokens, nomes de pacientes nem payload clínico.
-- Executar por operador autorizado no banco correto; nenhum PUT/PACS envolvido.
-- Na base legada, datas são timestamp sem timezone: conferir configuração TZ.
BEGIN READ ONLY;
SET LOCAL statement_timeout = '5s';
SET LOCAL lock_timeout = '200ms';

SELECT version(), current_database(), current_setting('TimeZone') AS timezone;

-- Configuração operacional; to_jsonb permite verificar presença da coluna nova
-- antes/depois da migração sem erro por coluna Empresa ID inexistente.
SELECT id, name, enabled, orders_api_station_id,
       to_jsonb(u)->>'orders_api_company_id' AS company_id,
       pacs_ip, pacs_port, pacs_aet, calling_aet, store_port,
       max_parallel_moves, find_interval_seconds, compact_workers, send_workers,
       move_timeout_first, move_timeout_second, move_timeout_prior
FROM units AS u WHERE deleted_at IS NULL ORDER BY id;

SELECT a.id AS unit_a, b.id AS unit_b,
       a.pacs_ip = b.pacs_ip AND a.pacs_port = b.pacs_port AS same_pacs_endpoint,
       a.orders_api_url = b.orders_api_url AS same_orders_endpoint,
       a.orders_api_token = b.orders_api_token AS same_integration_token,
       a.calling_aet = b.calling_aet AS duplicate_calling_aet,
       a.store_port = b.store_port AS duplicate_store_port,
       a.receive_dir = b.receive_dir AS same_receive_dir,
       a.send_dir = b.send_dir AS same_send_dir
FROM units a JOIN units b ON a.id < b.id
WHERE a.deleted_at IS NULL AND b.deleted_at IS NULL;

SELECT unit_id, status, count(*) AS orders,
       min(created_at) AS oldest_created_at,
       min(last_find_at) AS oldest_last_find,
       min(retrieve_at) AS earliest_scheduled_move
FROM orders WHERE archived_at IS NULL GROUP BY unit_id, status ORDER BY unit_id, status;

SELECT unit_id, api_read_status, count(*) AS orders,
       min(created_at) AS oldest_order, max(api_read_attempts) AS max_attempts
FROM orders WHERE archived_at IS NULL
GROUP BY unit_id, api_read_status ORDER BY unit_id, api_read_status;

SELECT unit_id, prior_status, count(*) AS orders, min(prior_due_at) AS oldest_due
FROM orders WHERE archived_at IS NULL
GROUP BY unit_id, prior_status ORDER BY unit_id, prior_status;

SELECT unit_id, status, count(*) AS images,
       min(created_at) AS oldest_created, min(next_attempt_at) AS earliest_retry,
       max(attempts) AS max_attempts
FROM image_transfers GROUP BY unit_id, status ORDER BY unit_id, status;

SELECT unit_id, last_http_status, count(*) AS failed_images
FROM image_transfers WHERE status = 'upload_error'
GROUP BY unit_id, last_http_status ORDER BY unit_id, failed_images DESC;

-- MOVE aparentemente órfão exige conferir processo/PACS antes de reexecutar.
SELECT id AS order_id, unit_id, status, heartbeat_at,
       current_timestamp - heartbeat_at AS heartbeat_age
FROM orders WHERE archived_at IS NULL AND status IN ('retrieving','retrieving_second')
ORDER BY heartbeat_at NULLS FIRST LIMIT 100;

-- Não imprime SQL text: uma query pode conter valores clínicos/segredos.
SELECT pid, application_name, client_addr, state, wait_event_type, wait_event,
       current_timestamp - xact_start AS transaction_age,
       current_timestamp - query_start AS query_age,
       pg_blocking_pids(pid) AS blocking_pids, query_id
FROM pg_stat_activity
WHERE datname = current_database() AND pid <> pg_backend_pid()
ORDER BY xact_start NULLS LAST;

SELECT state, application_name, count(*) AS connections
FROM pg_stat_activity WHERE datname = current_database()
GROUP BY state, application_name ORDER BY connections DESC;

SELECT relname, n_live_tup, n_dead_tup, n_mod_since_analyze,
       last_autovacuum, last_autoanalyze, autovacuum_count, autoanalyze_count,
       pg_size_pretty(pg_total_relation_size(relid)) AS total_size
FROM pg_stat_user_tables
WHERE relname IN ('orders','order_events','image_transfers','audit_logs',
                  'historical_series','historical_image_links','dicom_rule_applications')
ORDER BY pg_total_relation_size(relid) DESC;

SELECT relname, indexrelname, idx_scan,
       pg_size_pretty(pg_relation_size(indexrelid)) AS index_size
FROM pg_stat_user_indexes WHERE relname IN ('orders','image_transfers','order_events')
ORDER BY relname, indexrelname;

SELECT extname FROM pg_extension WHERE extname IN ('pg_stat_statements','pgstattuple');
-- Se pg_stat_statements existir e estiver carregado, executar separadamente:
-- SELECT queryid, calls, total_exec_time, mean_exec_time, rows,
--        shared_blks_hit, shared_blks_read, temp_blks_written, wal_bytes
-- FROM pg_stat_statements WHERE dbid = (SELECT oid FROM pg_database
--                                      WHERE datname = current_database())
-- ORDER BY total_exec_time DESC LIMIT 20;
-- Comparar deltas entre duas amostras, sem zerar estatísticas compartilhadas.

COMMIT;
-- Complementar com logs do período, métricas de host/containers/disco/rede,
-- stdout sanitizado DCMTK, versão implantada, configuração efetiva do worker,
-- HTTP GET latency/record_count e resumo orders.api.ingest.summary.
-- EXPLAIN sem ANALYZE é a primeira inspeção em produção; ANALYZE executa a query.
-- Claims UPDATE e consultas FOR UPDATE não devem ser usados como diagnóstico
-- inocente. Reproduzir seus planos com dados representativos em homologação.
