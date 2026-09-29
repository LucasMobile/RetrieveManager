-- MOBILEMED: PROPOSTA DE SCHEMA/QUERIES, NÃO UMA MIGRAÇÃO DE PRODUÇÃO.
-- PostgreSQL 18. Requer migrações versionadas, backfill e testes de concorrência.
-- Os exemplos com :param são templates SQLAlchemy, executados separadamente.
-- Não executar este arquivo inteiro. Nenhuma instrução foi aplicada ao DB real.
-- public.units é o cadastro existente; os demais objetos abaixo são novos.

CREATE SCHEMA mm_pipeline;

-- Roteamento imutável por versão. secret_ref é referência, nunca segredo.
CREATE TABLE mm_pipeline.unit_routes (
    unit_id integer NOT NULL REFERENCES public.units(id),
    version integer NOT NULL CHECK (version > 0),
    company_id varchar(64) NOT NULL CHECK (company_id ~ '^[1-9][0-9]{0,63}$'),
    station_id varchar(64) NOT NULL,
    feed_key text NOT NULL,
    api_endpoint text NOT NULL,
    secret_ref text NOT NULL,
    pacs_key text NOT NULL,
    calling_aet varchar(16) NOT NULL,
    destination_key text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (unit_id, version)
);

CREATE TABLE mm_pipeline.source_orders (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    unit_id integer NOT NULL,
    route_version integer NOT NULL,
    source_system text NOT NULL,
    source_order_id text NOT NULL,
    accession_number text NOT NULL,
    generation integer NOT NULL DEFAULT 1 CHECK (generation > 0),
    state text NOT NULL DEFAULT 'pending',
    correlation_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    archived_at timestamptz,
    UNIQUE (unit_id, id),
    UNIQUE (unit_id, source_system, source_order_id),
    FOREIGN KEY (unit_id, route_version)
        REFERENCES mm_pipeline.unit_routes(unit_id, version)
);
-- Campos clínicos validados ficam em tabela/registro de acesso controlado,
-- não no job. Resolver identidade estável de origem antes de migrar accession.
CREATE INDEX ix_source_orders_active
    ON mm_pipeline.source_orders(unit_id, state, created_at, id)
    WHERE archived_at IS NULL;
CREATE INDEX ix_source_orders_accession
    ON mm_pipeline.source_orders(unit_id, accession_number);

CREATE TABLE mm_pipeline.order_ack (
    unit_id integer NOT NULL,
    order_id bigint NOT NULL,
    source_revision text NOT NULL,
    route_version integer NOT NULL,
    state text NOT NULL DEFAULT 'pending',
    confirmed_at timestamptz,
    remote_receipt text,
    PRIMARY KEY (unit_id, order_id, source_revision, route_version),
    FOREIGN KEY (unit_id, order_id) REFERENCES mm_pipeline.source_orders(unit_id, id),
    FOREIGN KEY (unit_id, route_version) REFERENCES mm_pipeline.unit_routes(unit_id, version)
);

CREATE TABLE mm_pipeline.exams (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    unit_id integer NOT NULL REFERENCES public.units(id),
    pacs_key text NOT NULL,
    study_uid varchar(64) NOT NULL,
    state text NOT NULL DEFAULT 'find_ok',
    completeness text NOT NULL DEFAULT 'unknown'
        CHECK (completeness IN ('unknown', 'partial', 'verified')),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (unit_id, id),
    UNIQUE (unit_id, pacs_key, study_uid)
);
CREATE TABLE mm_pipeline.order_exams (
    unit_id integer NOT NULL,
    order_id bigint NOT NULL,
    exam_id bigint NOT NULL,
    relation text NOT NULL CHECK (relation IN ('current', 'historical')),
    PRIMARY KEY (unit_id, order_id, exam_id, relation),
    FOREIGN KEY (unit_id, order_id) REFERENCES mm_pipeline.source_orders(unit_id, id),
    FOREIGN KEY (unit_id, exam_id) REFERENCES mm_pipeline.exams(unit_id, id)
);

CREATE TABLE mm_pipeline.instances (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    unit_id integer NOT NULL,
    exam_id bigint NOT NULL,
    pacs_key text NOT NULL,
    series_uid varchar(64) NOT NULL,
    sop_uid varchar(64) NOT NULL,
    source_sha256 char(64) NOT NULL,
    source_path text NOT NULL,
    source_bytes bigint NOT NULL CHECK (source_bytes >= 0),
    state text NOT NULL DEFAULT 'received',
    received_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (unit_id, id),
    UNIQUE (unit_id, pacs_key, sop_uid),
    FOREIGN KEY (unit_id, exam_id) REFERENCES mm_pipeline.exams(unit_id, id)
);
CREATE INDEX ix_instances_exam_series
    ON mm_pipeline.instances(unit_id, exam_id, series_uid, id);

CREATE TABLE mm_pipeline.artifacts (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    unit_id integer NOT NULL,
    instance_id bigint NOT NULL,
    profile_version integer NOT NULL,
    destination_key text NOT NULL,
    object_key text NOT NULL,
    local_path text,
    sha256 char(64),
    size_bytes bigint,
    state text NOT NULL DEFAULT 'compress_pending',
    upload_id text,
    remote_receipt jsonb,
    verified_at timestamptz,
    UNIQUE (unit_id, id),
    UNIQUE (unit_id, instance_id, profile_version, destination_key),
    UNIQUE (destination_key, object_key),
    FOREIGN KEY (unit_id, instance_id) REFERENCES mm_pipeline.instances(unit_id, id)
);
CREATE INDEX ix_artifacts_pending
    ON mm_pipeline.artifacts(unit_id, state, id)
    WHERE verified_at IS NULL;
-- object_key inclui empresa/unidade e identidade de artefato; nunca PatientName.
-- Mesmo SOP + conteúdo diferente: registrar conflito/quarentena, nunca overwrite.

CREATE TABLE mm_pipeline.stage_ownership (
    stage text NOT NULL,
    unit_id integer NOT NULL REFERENCES public.units(id),
    owner text NOT NULL CHECK (owner IN ('legacy', 'v2')),
    mode text NOT NULL CHECK (mode IN ('active', 'draining', 'paused')),
    epoch bigint NOT NULL DEFAULT 1,
    PRIMARY KEY (stage, unit_id)
);

-- Tombstones sobrevivem ao arquivamento dos jobs, conforme horizonte de replay.
CREATE TABLE mm_pipeline.job_identity (
    stage text NOT NULL,
    unit_id integer NOT NULL REFERENCES public.units(id),
    dedupe_key text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (stage, unit_id, dedupe_key)
);

CREATE TABLE mm_pipeline.jobs (
    stage text NOT NULL CHECK (stage IN
        ('get', 'put', 'find', 'move', 'receive', 'compress', 'upload', 'verify')),
    id bigint GENERATED ALWAYS AS IDENTITY,
    unit_id integer NOT NULL,
    route_version integer NOT NULL,
    dedupe_key text NOT NULL,
    schema_version integer NOT NULL DEFAULT 1,
    order_id bigint,
    exam_id bigint,
    instance_id bigint,
    artifact_id bigint,
    scope jsonb NOT NULL DEFAULT '{}', -- feed/generation/pass/series; sem PHI/tokens
    correlation_id uuid NOT NULL,
    state text NOT NULL DEFAULT 'ready' CHECK (state IN
        ('ready', 'retry_wait', 'leased', 'uncertain', 'blocked', 'dead', 'succeeded', 'cancelled')),
    priority_class smallint NOT NULL DEFAULT 1 CHECK (priority_class BETWEEN 0 AND 3),
    available_at timestamptz NOT NULL DEFAULT now(),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts integer NOT NULL CHECK (max_attempts > 0),
    owner_epoch bigint,
    lease_token uuid,
    lease_owner text,
    lease_expires_at timestamptz,
    last_error_class text,
    last_error_code text,
    last_error_detail text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    PRIMARY KEY (stage, id),
    UNIQUE (stage, unit_id, dedupe_key),
    FOREIGN KEY (stage, unit_id, dedupe_key)
        REFERENCES mm_pipeline.job_identity(stage, unit_id, dedupe_key),
    FOREIGN KEY (unit_id, route_version) REFERENCES mm_pipeline.unit_routes(unit_id, version),
    FOREIGN KEY (unit_id, order_id) REFERENCES mm_pipeline.source_orders(unit_id, id),
    FOREIGN KEY (unit_id, exam_id) REFERENCES mm_pipeline.exams(unit_id, id),
    FOREIGN KEY (unit_id, instance_id) REFERENCES mm_pipeline.instances(unit_id, id),
    FOREIGN KEY (unit_id, artifact_id) REFERENCES mm_pipeline.artifacts(unit_id, id),
    CHECK (state <> 'leased' OR
        (lease_token IS NOT NULL AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL))
) PARTITION BY LIST (stage);

CREATE TABLE mm_pipeline.jobs_get PARTITION OF mm_pipeline.jobs FOR VALUES IN ('get');
CREATE TABLE mm_pipeline.jobs_put PARTITION OF mm_pipeline.jobs FOR VALUES IN ('put');
CREATE TABLE mm_pipeline.jobs_find PARTITION OF mm_pipeline.jobs FOR VALUES IN ('find');
CREATE TABLE mm_pipeline.jobs_move PARTITION OF mm_pipeline.jobs FOR VALUES IN ('move');
CREATE TABLE mm_pipeline.jobs_receive PARTITION OF mm_pipeline.jobs FOR VALUES IN ('receive');
CREATE TABLE mm_pipeline.jobs_compress PARTITION OF mm_pipeline.jobs FOR VALUES IN ('compress');
CREATE TABLE mm_pipeline.jobs_upload PARTITION OF mm_pipeline.jobs FOR VALUES IN ('upload');
-- Optional future reconciliation only. The current gateway's HTTP 200 already
-- confirms durable storage; do NOT enqueue verify after a received HTTP 200.
CREATE TABLE mm_pipeline.jobs_verify PARTITION OF mm_pipeline.jobs FOR VALUES IN ('verify');

CREATE INDEX ix_jobs_ready ON mm_pipeline.jobs
    (unit_id, priority_class, available_at, id)
    WHERE state IN ('ready', 'retry_wait');
CREATE INDEX ix_jobs_expired ON mm_pipeline.jobs(lease_expires_at, id)
    WHERE state = 'leased';
CREATE INDEX ix_jobs_dead ON mm_pipeline.jobs(unit_id, updated_at, id)
    WHERE state IN ('dead', 'blocked', 'uncertain');
-- stage na query permite pruning; o índice por prioridade serve a faixa
-- escolhida pelo escalonador justo. Não varrer todas as unidades por id global.

CREATE TABLE mm_pipeline.resource_slots (
    resource_key text NOT NULL, -- pacs:X:find, pacs:X:move, unit:Y:move, cpu:compact
    slot_no integer NOT NULL,
    job_stage text,
    job_id bigint,
    lease_token uuid,
    held_until timestamptz,
    PRIMARY KEY (resource_key, slot_no),
    FOREIGN KEY (job_stage, job_id) REFERENCES mm_pipeline.jobs(stage, id),
    CHECK ((job_id IS NULL AND job_stage IS NULL AND lease_token IS NULL AND held_until IS NULL)
        OR (job_id IS NOT NULL AND job_stage IS NOT NULL AND lease_token IS NOT NULL AND held_until IS NOT NULL))
);

CREATE TABLE mm_pipeline.job_events (
    id bigint GENERATED ALWAYS AS IDENTITY,
    occurred_at timestamptz NOT NULL DEFAULT now(),
    stage text NOT NULL,
    job_id bigint NOT NULL,
    unit_id integer NOT NULL,
    event text NOT NULL,
    attempt integer NOT NULL,
    duration_ms double precision,
    error_class text,
    error_code text,
    diagnostic_ref text, -- referência restrita à stack sanitizada
    PRIMARY KEY (occurred_at, id)
) PARTITION BY RANGE (occurred_at);
-- Criar partições mensais correntes/futuras ANTES de habilitar produtores.
-- Reconciliador de schema cria meses futuros, monitora e arquiva; sem cron manual.
-- Não há FK de events para jobs: eventos sobrevivem ao arquivamento do job.

CREATE VIEW mm_pipeline.dead_letters AS
SELECT stage, id AS job_id, unit_id, order_id, exam_id, instance_id,
       artifact_id, attempts, state, last_error_class, last_error_code,
       last_error_detail, updated_at
FROM mm_pipeline.jobs WHERE state IN ('dead', 'blocked', 'uncertain');

-- CLAIM: o scheduler escolhe unidade e faixa de prioridade com justiça.
-- Todas as instruções deste bloco pertencem à mesma transação curta.
BEGIN;
SET LOCAL statement_timeout = '3s';
SET LOCAL lock_timeout = '200ms';
SELECT epoch FROM mm_pipeline.stage_ownership
WHERE stage = :stage AND unit_id = :unit_id AND owner = 'v2' AND mode = 'active'
FOR SHARE;
-- Se zero linhas: ROLLBACK. Guardar o epoch retornado como :owner_epoch.
WITH candidate AS (
    SELECT stage, id FROM mm_pipeline.jobs
    WHERE stage = :stage AND unit_id = :unit_id
      AND state IN ('ready', 'retry_wait') AND available_at <= now()
      AND attempts < max_attempts AND priority_class = :priority_class
    ORDER BY available_at, id
    LIMIT :free_execution_slots
    FOR UPDATE SKIP LOCKED
)
UPDATE mm_pipeline.jobs AS j
SET state = 'leased', lease_token = gen_random_uuid(), lease_owner = :worker_id,
    lease_expires_at = now() + make_interval(secs => :lease_seconds),
    attempts = attempts + 1, owner_epoch = :owner_epoch, updated_at = now()
FROM candidate AS c
WHERE j.stage = c.stage AND j.id = c.id
RETURNING j.*;
-- Para cada job, adquirir TODOS os slots necessários na mesma transação.
-- Aplicação itera resource_keys em ordem lexical; slots NÃO são conexões.
WITH available AS (
    SELECT resource_key, slot_no FROM mm_pipeline.resource_slots
    WHERE resource_key = :resource_key AND job_id IS NULL
    ORDER BY slot_no LIMIT 1 FOR UPDATE SKIP LOCKED
)
UPDATE mm_pipeline.resource_slots AS s
SET job_stage = :stage, job_id = :job_id, lease_token = :lease_token,
    held_until = :lease_expires_at
FROM available AS a
WHERE s.resource_key = a.resource_key AND s.slot_no = a.slot_no
RETURNING s.resource_key, s.slot_no;
-- Se qualquer recurso faltar: ROLLBACK (inclusive attempt). Não segurar slot
-- esperando outro. Escolher lotes pequenos, preferencialmente 1 para MOVE.
COMMIT;
-- FECHAR SESSION/CONEXÃO AQUI. Só então executar HTTP/DCMTK/codec/filesystem.

-- HEARTBEAT separado do executor, sessão curta; se zero linhas, parar I/O.
BEGIN;
UPDATE mm_pipeline.jobs AS j
SET lease_expires_at = now() + make_interval(secs => :lease_seconds), updated_at = now()
WHERE stage = :stage AND id = :job_id AND state = 'leased'
  AND lease_token = :lease_token AND lease_owner = :worker_id
  AND lease_expires_at > now()
  AND EXISTS (SELECT 1 FROM mm_pipeline.stage_ownership AS o
              WHERE o.stage = j.stage AND o.unit_id = j.unit_id
                AND o.owner = 'v2' AND o.epoch = j.owner_epoch)
RETURNING lease_expires_at;
-- Somente se dono válido: renovar seus slots para o mesmo vencimento retornado.
UPDATE mm_pipeline.resource_slots SET held_until = :new_expiry
WHERE job_stage = :stage AND job_id = :job_id AND lease_token = :lease_token;
COMMIT;

-- SUCESSO: exemplo compressão -> artefato -> fila upload na mesma transação.
-- :sha256/:size_bytes vêm da validação realizada FORA da transação.
BEGIN;
UPDATE mm_pipeline.jobs AS j
SET state = 'succeeded', finished_at = now(), updated_at = now(),
    lease_expires_at = NULL, lease_owner = NULL, lease_token = NULL
WHERE stage = 'compress' AND id = :job_id AND unit_id = :unit_id
  AND state = 'leased' AND lease_token = :lease_token
  AND owner_epoch = :owner_epoch AND lease_expires_at > now()
  AND EXISTS (SELECT 1 FROM mm_pipeline.stage_ownership AS o
              WHERE o.stage = j.stage AND o.unit_id = j.unit_id
                AND o.owner = 'v2' AND o.epoch = j.owner_epoch)
RETURNING artifact_id, route_version, correlation_id;
-- Se zero linhas: ROLLBACK; NÃO atualizar artefato/próxima etapa.
-- Conferir artifact_id e route_version contra os valores retornados.
UPDATE mm_pipeline.artifacts
SET state = 'compressed', sha256 = :sha256, size_bytes = :size_bytes,
    local_path = :immutable_path
WHERE id = :artifact_id AND unit_id = :unit_id AND state = 'compress_pending';
-- Exigir uma linha alterada ou reconhecer checkpoint idêntico já confirmado.
WITH new_identity AS (
    INSERT INTO mm_pipeline.job_identity(stage, unit_id, dedupe_key)
    VALUES ('upload', :unit_id, :upload_dedupe_key)
    ON CONFLICT DO NOTHING RETURNING stage, unit_id, dedupe_key
)
INSERT INTO mm_pipeline.jobs
    (stage, unit_id, dedupe_key, route_version, artifact_id, correlation_id, max_attempts)
SELECT stage, unit_id, dedupe_key, :route_version, :artifact_id, :correlation_id, 10
FROM new_identity;
UPDATE mm_pipeline.resource_slots
SET job_stage = NULL, job_id = NULL, lease_token = NULL, held_until = NULL
WHERE job_stage = 'compress' AND job_id = :job_id AND lease_token = :lease_token;
-- Inserir evento da transição (partição do mês previamente criada).
COMMIT;

-- RETRY/DLQ: calcular atraso com jitter fora da transação; obter classificação
-- de erro antes do UPDATE. Estados blocked/uncertain têm reavaliação própria.
BEGIN;
UPDATE mm_pipeline.jobs AS j
SET state = CASE WHEN :retryable AND attempts < max_attempts
                 THEN 'retry_wait' ELSE 'dead' END,
    available_at = now() + make_interval(secs => :jitter_delay_seconds),
    last_error_class = :error_class, last_error_code = :error_code,
    last_error_detail = :sanitized_detail, updated_at = now(),
    lease_token = NULL, lease_owner = NULL, lease_expires_at = NULL
WHERE stage = :stage AND id = :job_id AND state = 'leased'
  AND lease_token = :lease_token AND lease_expires_at > now()
  AND EXISTS (SELECT 1 FROM mm_pipeline.stage_ownership AS o
              WHERE o.stage = j.stage AND o.unit_id = j.unit_id
                AND o.owner = 'v2' AND o.epoch = j.owner_epoch)
RETURNING state;
-- Zero linhas => ROLLBACK. Com sucesso, liberar slots do token e registrar evento
-- nesta mesma transação. Não liberar slot de MOVE remoto ainda possivelmente ativo.
COMMIT;

-- REAPER: expiração não significa que o PACS/HTTP parou.
-- Invalidar token antes de reconciliar. Slot permanece retido até decisão.
WITH expired AS (
    SELECT stage, id FROM mm_pipeline.jobs
    WHERE stage = :stage AND state = 'leased' AND lease_expires_at < now()
    ORDER BY lease_expires_at, id LIMIT 100 FOR UPDATE SKIP LOCKED
)
UPDATE mm_pipeline.jobs AS j
SET state = 'uncertain', lease_token = NULL, lease_owner = NULL,
    lease_expires_at = NULL, updated_at = now(), last_error_class = 'LeaseExpired'
FROM expired AS e WHERE j.stage = e.stage AND j.id = e.id
RETURNING j.stage, j.id, j.unit_id, j.artifact_id, j.scope;
-- Reconciliador busca uncertain após restart, valida efeitos/manifestos/receipts,
-- libera slots antigos e move a succeeded/retry_wait/dead em transação curta.

-- BACKFILL/OPERAÇÃO: índices de tabelas já grandes exigem migração separada.
-- Exemplo de candidato ao caminho ACK LEGADO; medir EXPLAIN antes de criar.
-- CREATE INDEX CONCURRENTLY ix_orders_ack_pending_unit_attempt_id
-- ON public.orders(unit_id, api_read_attempts, id)
-- WHERE archived_at IS NULL AND api_read_status = 'pending';
-- CREATE INDEX CONCURRENTLY não pode rodar dentro de um bloco de transação.
-- Para particionadas já povoadas, construir/ligar índices por partição com o
-- procedimento do PostgreSQL; não presumir CONCURRENTLY no parent particionado.
