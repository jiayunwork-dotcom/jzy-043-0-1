-- TaskForge schema for PostgreSQL 16. All task state transitions are
-- guarded by optimistic state checks inside transactions.

CREATE TABLE IF NOT EXISTS tasks (
    id              TEXT PRIMARY KEY,
    idempotency_key TEXT,
    type            TEXT NOT NULL,
    payload         JSONB NOT NULL DEFAULT '{}'::jsonb,
    priority        SMALLINT NOT NULL,
    state           TEXT NOT NULL,
    run_at          TIMESTAMPTZ,
    timeout_ms      BIGINT NOT NULL,
    retry_kind      TEXT NOT NULL,
    retry_base_ms   BIGINT NOT NULL,
    retry_cron      TEXT NOT NULL DEFAULT '',
    attempt         INT NOT NULL DEFAULT 0,
    max_retries     INT NOT NULL,
    callback_url    TEXT NOT NULL DEFAULT '',
    result          JSONB,
    last_error      TEXT NOT NULL DEFAULT '',
    error_type      TEXT NOT NULL DEFAULT '',
    worker_id       TEXT NOT NULL DEFAULT '',
    dag_id          TEXT NOT NULL DEFAULT '',
    dag_node        TEXT NOT NULL DEFAULT '',
    ready_at        TIMESTAMPTZ,
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS tasks_idem_uniq ON tasks (idempotency_key) WHERE idempotency_key <> '';
CREATE INDEX IF NOT EXISTS tasks_state_idx ON tasks (state);
CREATE INDEX IF NOT EXISTS tasks_priority_idx ON tasks (priority);
CREATE INDEX IF NOT EXISTS tasks_type_idx ON tasks (type);
CREATE INDEX IF NOT EXISTS tasks_created_idx ON tasks (created_at);
CREATE INDEX IF NOT EXISTS tasks_dag_idx ON tasks (dag_id);
CREATE INDEX IF NOT EXISTS tasks_worker_idx ON tasks (worker_id);

CREATE TABLE IF NOT EXISTS attempts (
    id            TEXT PRIMARY KEY,
    task_id       TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    attempt_no    INT NOT NULL,
    worker_id     TEXT NOT NULL,
    state         TEXT NOT NULL,
    started_at    TIMESTAMPTZ NOT NULL,
    ended_at      TIMESTAMPTZ,
    error_type    TEXT NOT NULL DEFAULT '',
    error_message TEXT NOT NULL DEFAULT '',
    result        JSONB
);
CREATE INDEX IF NOT EXISTS attempts_task_idx ON attempts (task_id, attempt_no);

CREATE TABLE IF NOT EXISTS workers (
    id              TEXT PRIMARY KEY,
    total_slots     INT NOT NULL,
    active_slots    INT NOT NULL DEFAULT 0,
    status          TEXT NOT NULL,
    last_heartbeat  TIMESTAMPTZ NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL,
    succeeded       BIGINT NOT NULL DEFAULT 0,
    failed          BIGINT NOT NULL DEFAULT 0,
    preempted       BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS dags (
    id             TEXT PRIMARY KEY,
    name           TEXT NOT NULL,
    status         TEXT NOT NULL,
    failure_policy TEXT NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL,
    finished_at    TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS dag_nodes (
    dag_id       TEXT NOT NULL REFERENCES dags(id) ON DELETE CASCADE,
    node_id      TEXT NOT NULL,
    task_id      TEXT NOT NULL DEFAULT '',
    state        TEXT NOT NULL,
    depends_on   JSONB NOT NULL DEFAULT '[]'::jsonb,
    def          JSONB NOT NULL,
    started_at   TIMESTAMPTZ,
    finished_at  TIMESTAMPTZ,
    error        TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (dag_id, node_id)
);

CREATE TABLE IF NOT EXISTS audit_events (
    id         BIGSERIAL PRIMARY KEY,
    task_id    TEXT NOT NULL DEFAULT '',
    dag_id     TEXT NOT NULL DEFAULT '',
    worker_id  TEXT NOT NULL DEFAULT '',
    event      TEXT NOT NULL,
    from_state TEXT NOT NULL DEFAULT '',
    to_state   TEXT NOT NULL DEFAULT '',
    detail     TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS audit_task_idx ON audit_events (task_id, id);
CREATE INDEX IF NOT EXISTS audit_dag_idx ON audit_events (dag_id, id);
CREATE INDEX IF NOT EXISTS audit_created_idx ON audit_events (created_at);
