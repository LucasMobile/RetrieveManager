#!/usr/bin/env bash
# Coleta um diagnóstico completo do Retrieve Manager em execução, sem segredos
# (tokens, senhas, chaves) e sem dados de paciente (nome, Patient ID, data de
# nascimento; números longos como accession são mascarados).
#
# Uso, no servidor, a partir da pasta do docker-compose.yml:
#   bash collect_diagnostics.sh            # últimos 3 dias de logs
#   DAYS=7 bash collect_diagnostics.sh     # outra janela
#   PREFIX=retrieve bash collect_diagnostics.sh
#
# Gera /tmp/retrieve-diag-<data>.tar.gz.

set -u

PREFIX="${PREFIX:-retrieve}"
DAYS="${DAYS:-3}"
LOG_LINES="${LOG_LINES:-50000}"
STAMP="$(date +%Y%m%d-%H%M%S)"
OUT="/tmp/retrieve-diag-${STAMP}"
PG="${PREFIX}-postgres-1"
WORKER="${PREFIX}-worker-1"
mkdir -p "$OUT"/{host,docker,logs,db,app}

redact() {
  sed -E \
    -e 's/(([A-Za-z_]*(PASSWORD|PASSWD|SECRET|TOKEN|API_KEY|PRIVATE_KEY|CREDENTIAL)[A-Za-z_]*)["]?[[:space:]]*[=:][[:space:]]*)[^[:space:],}]+/\1***REDACTED***/Ig' \
    -e 's#(://[^:/@[:space:]]+):[^@/[:space:]]+@#\1:***@#g' \
    -e 's/([Bb]earer[[:space:]]+)[A-Za-z0-9._~+\/=-]+/\1***REDACTED***/g'
}

step() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }

run() {  # run <arquivo> <comando...>: saída + erros, redigidos
  local file="$1"
  shift
  { "$@" 2>&1 || echo "(falhou: código $?)"; } | redact > "$file"
}

# -- máquina ------------------------------------------------------------------
step "Host"
run "$OUT/host/system.txt" sh -c 'date; echo; uname -a; echo; uptime; echo; nproc; echo; free -m; echo; df -h; echo; df -i'
run "$OUT/host/listening_ports.txt" sh -c 'ss -ltnp 2>/dev/null || netstat -ltnp'
run "$OUT/host/dmesg_tail.txt" sh -c 'dmesg -T 2>/dev/null | tail -n 300 | grep -iE "oom|killed|error|fail|i/o|nfs|cifs" || true'

# -- docker -------------------------------------------------------------------
step "Docker"
run "$OUT/docker/version.txt" docker version
run "$OUT/docker/ps.txt" docker ps -a --no-trunc --format 'table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.RunningFor}}\t{{.Ports}}'
run "$OUT/docker/stats.txt" docker stats --no-stream --format 'table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.NetIO}}\t{{.BlockIO}}\t{{.PIDs}}'
run "$OUT/docker/images.txt" docker images --digests
run "$OUT/docker/system_df.txt" docker system df -v
run "$OUT/docker/compose_config.yml" docker compose config
CONTAINERS="$(docker ps -a --format '{{.Names}}' | grep "^${PREFIX}-" || true)"
for name in $CONTAINERS; do
  run "$OUT/docker/inspect_${name}.json" docker inspect --format '{{json .State}}
image={{.Image}} created={{.Created}} restarts={{.RestartCount}}
mounts={{json .Mounts}}
hostconfig.memory={{.HostConfig.Memory}} cpus={{.HostConfig.NanoCpus}}' "$name"
  run "$OUT/docker/env_${name}.txt" docker exec "$name" env
done

# -- versão do código implantado -------------------------------------------------
step "Código implantado"
if docker inspect "$WORKER" >/dev/null 2>&1; then
  run "$OUT/app/checksums.txt" docker exec "$WORKER" sh -c 'cd /app && find app -name "*.py" -o -name "*.html" -o -name "*.js" -o -name "*.css" | sort | xargs sha256sum'
  run "$OUT/app/fix_markers.txt" docker exec "$WORKER" sh -c 'cd /app && for m in status_condition identity_sha256 confirmed_during_get _find_patient_id_diverges; do printf "%-28s %s\n" "$m" "$(grep -rl "$m" app | tr "\n" " ")"; done'
  run "$OUT/app/pip_freeze.txt" docker exec "$WORKER" python -m pip freeze
fi

# -- logs -----------------------------------------------------------------------
step "Logs (${DAYS} dias)"
for name in $CONTAINERS; do
  docker logs --timestamps --since "$((DAYS * 24))h" "$name" 2>&1 \
    | tail -n "$LOG_LINES" \
    | sed -E 's/[0-9]{8,}/#/g' \
    | redact > "$OUT/logs/${name}.log"
done

# -- banco ----------------------------------------------------------------------
step "Banco de dados"
PGUSER="$(docker exec "$PG" printenv POSTGRES_USER 2>/dev/null || echo retrieve)"
PGDB="$(docker exec "$PG" printenv POSTGRES_DB 2>/dev/null || echo retrieve)"

q() {  # q <nome> <sql>: cada comando roda sozinho; um erro não derruba os outros
  printf "SET statement_timeout = '120s';\n%s\n" "$2" \
    | docker exec -i "$PG" psql -U "$PGUSER" -d "$PGDB" -P pager=off -X -q -e \
        -v ON_ERROR_STOP=0 -f - 2>&1 \
    | redact > "$OUT/db/$1.txt"
}

# Máscara de números longos (accession, IDs de paciente) dentro de textos.
MASK="regexp_replace(%s, '[0-9]{6,}', '#', 'g')"
mask() { printf "$MASK" "$1"; }

q 00_postgres "
select version();
select pg_size_pretty(pg_database_size(current_database())) as db_size;
select name, setting, unit from pg_settings where name in
 ('max_connections','shared_buffers','work_mem','maintenance_work_mem',
  'effective_cache_size','max_wal_size','checkpoint_timeout','autovacuum',
  'idle_in_transaction_session_timeout','statement_timeout','TimeZone');
select extname, extversion from pg_extension;"

q 01_tables "
select relname as tabela, n_live_tup as linhas, n_dead_tup as mortas,
       pg_size_pretty(pg_total_relation_size(relid)) as tamanho,
       last_autovacuum, last_autoanalyze, seq_scan, idx_scan
from pg_stat_user_tables order by pg_total_relation_size(relid) desc;"

q 02_indexes "
select relname as tabela, indexrelname as indice, idx_scan,
       pg_size_pretty(pg_relation_size(indexrelid)) as tamanho
from pg_stat_user_indexes order by idx_scan asc, pg_relation_size(indexrelid) desc;"

q 03_activity "
select pid, usename, application_name, state, wait_event_type, wait_event,
       now() - xact_start as transacao, now() - query_start as consulta,
       left(regexp_replace(query, '\s+', ' ', 'g'), 300) as sql
from pg_stat_activity where datname = current_database() order by xact_start nulls last;
select l.locktype, l.mode, l.granted, l.relation::regclass, a.pid, now() - a.query_start as ha
from pg_locks l join pg_stat_activity a on a.pid = l.pid where not l.granted;"

q 04_units "
select id, name, enabled, deleted_at is not null as deleted,
       orders_api_url is not null and orders_api_url <> '' as api_configurada,
       orders_api_station_id, orders_api_company_id,
       pacs_aet, pacs_ip, pacs_port, pacs_patient_id_wildcard, calling_aet,
       store_port, store_allowed_ips, store_allowed_aets,
       receive_dir, send_dir, error_dir, cloud_url,
       move_timeout_first, move_timeout_update, retrieve_prior_enabled,
       move_timeout_prior, max_parallel_moves, find_interval_seconds,
       compact_workers, send_workers, created_at, updated_at
from units order by id;
select * from modality_rules order by modality;
select * from unit_compression_settings;
select * from unit_compress_rules order by unit_id, modality;
select * from unit_drop_modalities order by unit_id, code;
select * from unit_prior_modalities order by unit_id, code;
select r.id, r.name, r.enabled, r.priority, r.combinator, r.action, r.action_tag,
       r.action_value, r.system_key, c.position, c.tag, c.operator, c.value
from dicom_rules r left join dicom_rule_conditions c on c.rule_id = r.id
order by r.priority, r.id, c.position;
select * from dicom_rule_units order by rule_id, unit_id;"

q 10_orders_status "
select unit_id, archived_at is not null as arquivado, status, prior_status,
       api_read_status, count(*)
from orders group by 1,2,3,4,5 order by 1,2,6 desc;
select date_trunc('day', created_at) as dia, unit_id, count(*) as pedidos,
       count(*) filter (where found_at is not null) as encontrados,
       count(*) filter (where status = 'error') as erro,
       count(*) filter (where prior_status = 'error') as erro_historico,
       count(*) filter (where archive_reason <> '') as arquivados_com_motivo
from orders where created_at >= now() - interval '14 days'
group by 1,2 order by 1 desc, 2;"

q 11_orders_errors "
select id, unit_id, status, prior_status, api_read_status, attempts, prior_attempts,
       api_read_attempts, modality, created_at, found_at, done_at, updated_at,
       $(mask last_error) as last_error,
       $(mask prior_last_error) as prior_last_error,
       $(mask api_read_last_error) as api_read_last_error
from orders
where archived_at is null
  and (status = 'error' or prior_status = 'error' or last_error <> ''
       or prior_last_error <> '' or api_read_last_error <> '')
order by updated_at desc limit 500;
select $(mask last_error) as erro, count(*) from orders where last_error <> ''
group by 1 order by 2 desc limit 50;
select $(mask prior_last_error) as erro_historico, count(*) from orders
where prior_last_error <> '' group by 1 order by 2 desc limit 50;
select $(mask api_read_last_error) as erro_api, count(*) from orders
where api_read_last_error <> '' group by 1 order by 2 desc limit 50;
select archive_reason, count(*) from orders where archived_at is not null
group by 1 order by 2 desc limit 30;"

q 12_orders_stuck "
select id, unit_id, status, prior_status, now() - heartbeat_at as heartbeat_ha,
       now() - prior_heartbeat_at as prior_heartbeat_ha, retrieve_at, monitor_next_at,
       prior_due_at, monitor_until, updated_at
from orders
where archived_at is null and (
  (status in ('retrieving','retrieving_update','receiving')
     and (heartbeat_at is null or heartbeat_at < now() - interval '15 minutes'))
  or (prior_status = 'retrieving'
     and (prior_heartbeat_at is null or prior_heartbeat_at < now() - interval '15 minutes'))
  or (status = 'wait_retrieve' and retrieve_at < now() - interval '15 minutes')
  or (status = 'monitoring' and monitor_next_at < now() - interval '15 minutes')
  or (status = 'watching' and created_at < now() - interval '20 hours')
  or (api_read_status = 'pending' and created_at < now() - interval '10 minutes'))
order by updated_at limit 300;"

q 13_orders_timing "
select unit_id, modality, count(*),
  percentile_cont(0.5) within group (order by extract(epoch from found_at - created_at))/60 as p50_min_ate_achar,
  percentile_cont(0.9) within group (order by extract(epoch from found_at - created_at))/60 as p90_min_ate_achar,
  percentile_cont(0.5) within group (order by extract(epoch from done_at - found_at))/60 as p50_min_achar_ate_fim,
  percentile_cont(0.9) within group (order by extract(epoch from done_at - found_at))/60 as p90_min_achar_ate_fim,
  avg(monitor_checks) as media_verificacoes, avg(monitor_new_images) as media_imagens_novas
from orders where created_at >= now() - interval '7 days' and found_at is not null
group by 1,2 order by 1,3 desc;"

q 14_order_events "
select level, $(mask "regexp_replace(message, '[0-9]+', '#', 'g')") as mensagem, count(*),
       min(created_at) as primeira, max(created_at) as ultima
from order_events where created_at >= now() - interval '${DAYS} days'
group by 1,2 order by (level = 'error') desc, (level = 'warn') desc, 3 desc limit 300;
select e.created_at, e.order_id, o.unit_id, e.level, $(mask e.message) as mensagem,
       left($(mask e.detail), 600) as detalhe
from order_events e join orders o on o.id = e.order_id
where e.level in ('warn','error') and e.created_at >= now() - interval '${DAYS} days'
order by e.created_at desc limit 400;"

q 20_instances "
select unit_id, state, count(*), pg_size_pretty(sum(source_bytes)) as volume,
       min(received_at) as mais_antiga, max(received_at) as mais_recente
from dicom_instances group by 1,2 order by 1,2;
select unit_id, state, $(mask last_error) as erro, count(*) from dicom_instances
where last_error <> '' group by 1,2,3 order by 4 desc limit 50;
select date_trunc('hour', received_at) as hora, unit_id, state, count(*)
from dicom_instances where received_at >= now() - interval '${DAYS} days'
  and state in ('conflict','error','missing','rejected')
group by 1,2,3 order by 1 desc limit 300;
select unit_id, state, count(*), now() - min(received_at) as mais_velha
from dicom_instances where state in ('received','compacting') group by 1,2;
select transfer_syntax, modality, count(*) from dicom_instances
where received_at >= now() - interval '${DAYS} days' group by 1,2 order by 3 desc limit 40;
select calling_aet, peer_ip, count(*) from dicom_instances
where received_at >= now() - interval '${DAYS} days' group by 1,2 order by 3 desc;"

q 21_conflicts "
select o.transfer_syntax as ts_original, c.transfer_syntax as ts_conflito,
       (o.source_bytes = c.source_bytes) as mesmo_tamanho, count(*)
from dicom_instances c join dicom_instances o
  on o.unit_id = c.unit_id and o.sop_uid = c.sop_uid and o.state <> 'conflict'
where c.state = 'conflict' group by 1,2,3 order by 4 desc;
select date_trunc('hour', c.received_at) as hora, count(*),
       min(c.received_at - o.received_at) as menor_intervalo,
       max(c.received_at - o.received_at) as maior_intervalo
from dicom_instances c join dicom_instances o
  on o.unit_id = c.unit_id and o.sop_uid = c.sop_uid and o.state <> 'conflict'
where c.state = 'conflict' group by 1 order by 1 desc limit 48;"

q 22_studies "
select unit_id, count(*) as estudos, sum(instance_count) as instancias,
       max(last_received_at) as ultimo_recebimento
from dicom_studies group by 1;
select count(*) as estudos_sem_pedido from dicom_studies s
where not exists (select 1 from orders o where o.study_uid = s.study_uid)
  and s.first_received_at >= now() - interval '${DAYS} days';"

q 30_transfers "
select unit_id, status, count(*), max(attempts) as max_tentativas,
       min(created_at) as mais_antiga, max(updated_at) as ultima_atualizacao
from image_transfers group by 1,2 order by 1,2;
select unit_id, status, last_http_status, $(mask last_error) as erro, count(*)
from image_transfers where last_error <> '' or last_http_status >= 300
group by 1,2,3,4 order by 5 desc limit 60;
select date_trunc('hour', updated_at) as hora, unit_id, status, count(*)
from image_transfers where updated_at >= now() - interval '${DAYS} days'
group by 1,2,3 order by 1 desc limit 300;
select unit_id, status, count(*), now() - min(next_attempt_at) as atraso_maximo
from image_transfers where status not in ('uploaded','discarded')
  and next_attempt_at < now() group by 1,2;"

q 31_rule_applications "
select unit_id, rule_name, action, count(*), max(created_at) as ultima
from dicom_rule_applications where created_at >= now() - interval '${DAYS} days'
group by 1,2,3 order by 4 desc limit 60;"

q 40_prior "
select unit_id, status, count(*), max(attempts) from historical_series group by 1,2 order by 1,2;
select unit_id, $(mask last_error) as erro, count(*) from historical_series
where last_error <> '' group by 1,2 order by 3 desc limit 40;
select count(*) as estudos_historicos, count(distinct order_id) as pedidos from historical_studies;
select count(*) as vinculos_imagens from historical_image_links;"

q 41_manual_moves "
select unit_id, status, count(*), max(completed_at) as ultimo
from manual_move_requests group by 1,2 order by 1,2;
select id, unit_id, status, $(mask last_error) as erro, created_at, started_at, completed_at
from manual_move_requests where last_error <> '' order by created_at desc limit 50;"

q 50_audit "
select created_at, actor_username, actor_role, action, resource_type, resource_id,
       $(mask summary) as resumo
from audit_logs order by created_at desc limit 300;"

q 51_users "select id, username, role from users order by id;"

# -- pastas das unidades --------------------------------------------------------
step "Pastas das unidades"
if docker inspect "$WORKER" >/dev/null 2>&1; then
  docker exec -i "$WORKER" python - > "$OUT/app/folders.txt" 2>&1 <<'PY'
import os
import time
from pathlib import Path

from sqlalchemy import select

from app.db import SessionLocal
from app.models import Unit

now = time.time()
with SessionLocal() as db:
    units = list(db.scalars(select(Unit).where(Unit.deleted_at.is_(None))))
for unit in units:
    print(f"== unidade {unit.id} {unit.name}")
    for label in ("receive_dir", "send_dir", "error_dir"):
        folder = Path(getattr(unit, label) or "")
        if not str(folder) or not folder.exists():
            print(f"  {label:12} {folder}  (não existe)")
            continue
        files = hidden = size = 0
        oldest = None
        for root, _dirs, names in os.walk(folder):
            for name in names:
                try:
                    stat = os.stat(os.path.join(root, name))
                except OSError:
                    continue
                files += 1
                hidden += name.startswith(".")
                size += stat.st_size
                oldest = stat.st_mtime if oldest is None else min(oldest, stat.st_mtime)
        age = f"{(now - oldest) / 3600:.1f} h" if oldest else "-"
        usage = os.statvfs(folder)
        free = usage.f_bavail * usage.f_frsize / 2**30
        print(
            f"  {label:12} {folder}  arquivos={files} ocultos/tmp={hidden} "
            f"volume={size / 2**20:.1f} MB mais_antigo={age} livre={free:.1f} GB"
        )
PY
fi

# -- pacote ---------------------------------------------------------------------
step "Compactando"
tar -C /tmp -czf "${OUT}.tar.gz" "$(basename "$OUT")" && rm -rf "$OUT"
echo
echo "Pronto: ${OUT}.tar.gz ($(du -h "${OUT}.tar.gz" | cut -f1))"
echo "Antes de enviar, você pode conferir o conteúdo com: tar -tzf ${OUT}.tar.gz"
