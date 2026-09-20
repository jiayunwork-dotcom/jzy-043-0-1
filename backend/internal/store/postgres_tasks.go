package store

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"taskforge/internal/domain"
)

//go:embed schema.sql
var schemaSQL string

// PgStore implements Repository against PostgreSQL via pgx.
type PgStore struct {
	pool *pgxpool.Pool
}

func NewPostgres(ctx context.Context, dsn string) (*PgStore, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = 20
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &PgStore{pool: pool}, nil
}

func (s *PgStore) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

func (s *PgStore) Migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, schemaSQL)
	return err
}

func (s *PgStore) Close() { s.pool.Close() }

const taskCols = `id, idempotency_key, type, payload, priority, state, run_at, timeout_ms,
	retry_kind, retry_base_ms, retry_cron, attempt, max_retries, callback_url, result,
	last_error, error_type, worker_id, dag_id, dag_node, ready_at, started_at,
	finished_at, created_at, updated_at`

func scanTask(row pgx.Row) (*domain.Task, error) {
	var (
		t              domain.Task
		priority       int16
		timeoutMS      int64
		retryBaseMS    int64
		payload, res   []byte
		idem, callback string
		runAt          *time.Time
		readyAt        *time.Time
	)
	err := row.Scan(
		&t.ID, &idem, &t.Type, &payload, &priority, &t.State, &runAt, &timeoutMS,
		&t.RetryPolicy.Kind, &retryBaseMS, &t.RetryPolicy.Cron, &t.Attempt, &t.MaxRetries, &callback, &res,
		&t.LastError, &t.ErrorType, &t.WorkerID, &t.DAGID, &t.DAGNode, &readyAt, &t.StartedAt,
		&t.FinishedAt, &t.CreatedAt, &t.UpdatedAt,
	)
	if err != nil {
		return nil, mapErr(err)
	}
	t.IdempotencyKey = idem
	t.CallbackURL = callback
	t.Priority = domain.Priority(priority)
	t.Timeout = domain.Duration(time.Duration(timeoutMS) * time.Millisecond)
	t.RetryPolicy.BaseInterval = domain.Duration(time.Duration(retryBaseMS) * time.Millisecond)
	t.RetryPolicy.MaxRetries = t.MaxRetries
	t.Payload = payload
	if res != nil {
		t.Result = res
	}
	t.RunAt = runAt
	if readyAt != nil {
		t.ReadyAt = *readyAt
	}
	return &t, nil
}

func mapErr(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrNotFound
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return domain.ErrConflict
	}
	return err
}

func (s *PgStore) CreateTask(ctx context.Context, t *domain.Task) (*domain.Task, error) {
	// Idempotency: if a task with the same key exists, return it untouched.
	if t.IdempotencyKey != "" {
		existing, err := s.getByIdem(ctx, t.IdempotencyKey)
		if err == nil {
			return existing, nil
		}
		if !errors.Is(err, domain.ErrNotFound) {
			return nil, err
		}
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO tasks (id, idempotency_key, type, payload, priority, state, run_at, timeout_ms,
			retry_kind, retry_base_ms, retry_cron, attempt, max_retries, callback_url,
			dag_id, dag_node, ready_at, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$18)`,
		t.ID, nullEmpty(t.IdempotencyKey), t.Type, jsonOrEmpty(t.Payload), int16(t.Priority),
		string(t.State), t.RunAt, t.Timeout.Std().Milliseconds(),
		string(t.RetryPolicy.Kind), t.RetryPolicy.BaseInterval.Std().Milliseconds(), t.RetryPolicy.Cron,
		t.Attempt, t.MaxRetries, t.CallbackURL, t.DAGID, t.DAGNode, t.ReadyAt, t.CreatedAt)
	if err != nil {
		if mapped := mapErr(err); errors.Is(mapped, domain.ErrConflict) {
			return s.getByIdem(ctx, t.IdempotencyKey)
		}
		return nil, err
	}
	return nil, nil
}

func nullEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func jsonOrEmpty(b []byte) []byte {
	if len(b) == 0 {
		return []byte("{}")
	}
	if !json.Valid(b) {
		out, _ := json.Marshal(map[string]string{"value": string(b)})
		return out
	}
	return b
}

func (s *PgStore) getByIdem(ctx context.Context, key string) (*domain.Task, error) {
	row := s.pool.QueryRow(ctx, "SELECT "+taskCols+" FROM tasks WHERE idempotency_key = $1", key)
	return scanTask(row)
}

func (s *PgStore) GetTask(ctx context.Context, id string) (*domain.Task, error) {
	row := s.pool.QueryRow(ctx, "SELECT "+taskCols+" FROM tasks WHERE id = $1", id)
	return scanTask(row)
}

func (s *PgStore) GetTasks(ctx context.Context, ids []string) ([]*domain.Task, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	rows, err := s.pool.Query(ctx, "SELECT "+taskCols+" FROM tasks WHERE id = ANY($1)", ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return collectTasks(rows)
}

func collectTasks(rows pgx.Rows) ([]*domain.Task, error) {
	var out []*domain.Task
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *PgStore) ListTasks(ctx context.Context, f TaskFilter) (TaskListResult, error) {
	var where []string
	var args []any
	add := func(cond string, v any) {
		args = append(args, v)
		where = append(where, fmt.Sprintf(cond, len(args)))
	}
	if len(f.States) > 0 {
		add("state = ANY($%d)", statesToStrings(f.States))
	}
	if len(f.Priorities) > 0 {
		ps := make([]int16, len(f.Priorities))
		for i, p := range f.Priorities {
			ps[i] = int16(p)
		}
		add("priority = ANY($%d)", ps)
	}
	if len(f.Types) > 0 {
		add("type = ANY($%d)", f.Types)
	}
	if f.From != nil {
		add("created_at >= $%d", *f.From)
	}
	if f.To != nil {
		add("created_at <= $%d", *f.To)
	}
	if f.DAGID != "" {
		add("dag_id = $%d", f.DAGID)
	}
	if f.WorkerID != "" {
		add("worker_id = $%d", f.WorkerID)
	}
	clause := ""
	if len(where) > 0 {
		clause = "WHERE " + strings.Join(where, " AND ")
	}
	var total int64
	if err := s.pool.QueryRow(ctx, "SELECT count(*) FROM tasks "+clause, args...).Scan(&total); err != nil {
		return TaskListResult{}, err
	}
	limit := f.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	args = append(args, limit, f.Offset)
	q := "SELECT " + taskCols + " FROM tasks " + clause +
		fmt.Sprintf(" ORDER BY created_at DESC LIMIT $%d OFFSET $%d", len(args)-1, len(args))
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return TaskListResult{}, err
	}
	defer rows.Close()
	tasks, err := collectTasks(rows)
	if err != nil {
		return TaskListResult{}, err
	}
	return TaskListResult{Tasks: tasks, Total: total}, nil
}

func statesToStrings(ss []domain.TaskState) []string {
	out := make([]string, len(ss))
	for i, s := range ss {
		out[i] = string(s)
	}
	return out
}

// ---- state transitions (all CAS-guarded inside a transaction) -----------

func (s *PgStore) addAuditTx(ctx context.Context, tx pgx.Tx, e *domain.AuditEvent) {
	_, _ = tx.Exec(ctx, `INSERT INTO audit_events
		(task_id, dag_id, worker_id, event, from_state, to_state, detail, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		e.TaskID, e.DAGID, e.WorkerID, e.Event, e.FromState, e.ToState, e.Detail, e.CreatedAt)
}

// MarkReady transitions pending -> ready.
func (s *PgStore) MarkReady(ctx context.Context, id string, now time.Time) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE tasks SET state='ready', ready_at=$2, updated_at=$2, run_at=NULL, worker_id=''
		WHERE id=$1 AND state='pending'`, id, now)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrConflict
	}
	return s.AddAudit(ctx, &domain.AuditEvent{TaskID: id, Event: "ready", FromState: "pending",
		ToState: "ready", CreatedAt: now})
}

func (s *PgStore) StartAttempt(ctx context.Context, taskID, workerID string, leaseExpiresAt, now time.Time) (*domain.Attempt, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var t domain.Task
	var priority int16
	row := tx.QueryRow(ctx, `SELECT state, priority FROM tasks WHERE id=$1 FOR UPDATE`, taskID)
	if err := row.Scan(&t.State, &priority); err != nil {
		return nil, mapErr(err)
	}
	if t.State != domain.StateReady {
		return nil, domain.ErrConflict
	}
	var attemptNo int
	if err := tx.QueryRow(ctx,
		`UPDATE tasks SET state='running', worker_id=$2, started_at=COALESCE(started_at,$3),
			updated_at=$3, attempt=attempt+1 WHERE id=$1 AND state='ready' RETURNING attempt-1`,
		taskID, workerID, now).Scan(&attemptNo); err != nil {
		return nil, mapErr(err)
	}
	att := &domain.Attempt{
		ID:        newID(),
		TaskID:    taskID,
		AttemptNo: attemptNo,
		WorkerID:  workerID,
		State:     domain.AttemptRunning,
		StartedAt: now,
	}
	if _, err := tx.Exec(ctx, `INSERT INTO attempts (id, task_id, attempt_no, worker_id, state, started_at)
		VALUES ($1,$2,$3,$4,'running',$5)`, att.ID, taskID, attemptNo, workerID, now); err != nil {
		return nil, err
	}
	s.addAuditTx(ctx, tx, &domain.AuditEvent{TaskID: taskID, WorkerID: workerID,
		Event: "start", FromState: "ready", ToState: "running",
		Detail:    fmt.Sprintf("attempt=%d lease_until=%s", attemptNo, leaseExpiresAt.Format(time.RFC3339)),
		CreatedAt: now})
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return att, nil
}

func (s *PgStore) RenewLease(ctx context.Context, taskID, workerID string, expiresAt time.Time) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE tasks SET updated_at=$3 WHERE id=$1 AND worker_id=$2 AND state='running'`,
		taskID, workerID, expiresAt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrConflict
	}
	return nil
}

// PreemptRunning picks a running task with priority >= minPriority on the
// worker and returns it to ready, at the queue head. The currently running
// attempt is marked preempted.
func (s *PgStore) PreemptRunning(ctx context.Context, workerID string, minPriority domain.Priority, protect []string, now time.Time) (*domain.Task, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	row := tx.QueryRow(ctx, `
		SELECT id, priority FROM tasks
		WHERE worker_id=$1 AND state='running' AND priority >= $2
			AND NOT (id = ANY($3))
		ORDER BY priority DESC, updated_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED`,
		workerID, int16(minPriority), protect)
	var id string
	var pri int16
	if err := row.Scan(&id, &pri); err != nil {
		return nil, mapErr(err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE tasks SET state='ready', ready_at=$2, updated_at=$2
		WHERE id=$1 AND state='running'`, id, now); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE attempts SET state='preempted', ended_at=$3
		WHERE task_id=$1 AND worker_id=$2 AND state='running'`, id, workerID, now); err != nil {
		return nil, err
	}
	s.addAuditTx(ctx, tx, &domain.AuditEvent{TaskID: id, WorkerID: workerID,
		Event: "preempt", FromState: "running", ToState: "ready", CreatedAt: now})
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return s.GetTask(ctx, id)
}

func (s *PgStore) CompleteAttempt(ctx context.Context, att *domain.Attempt,
	next domain.TaskState, result []byte, errMsg, errType string, finishedAt time.Time) error {

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var fromState string
	row := tx.QueryRow(ctx, `SELECT state FROM tasks WHERE id=$1 FOR UPDATE`, att.TaskID)
	if err := row.Scan(&fromState); err != nil {
		return mapErr(err)
	}
	// A task whose lease was reaped / which was preempted is no longer
	// owned by this execution; the late result must be discarded.
	if fromState != string(domain.StateRunning) {
		return domain.ErrConflict
	}

	var finishedCol any
	if next == domain.StateSucceeded || next == domain.StateDead {
		finishedCol = finishedAt
	}
	if _, err := tx.Exec(ctx, `
		UPDATE tasks SET state=$2, result=$3, last_error=$4, error_type=$5,
			finished_at=COALESCE(finished_at,$6), updated_at=$7, ready_at=$7
		WHERE id=$1 AND state='running'`,
		att.TaskID, string(next), jsonOrEmpty(result), errMsg, errType, finishedCol, finishedAt); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE attempts SET state=$2, ended_at=$3, error_type=$4, error_message=$5, result=$6
		WHERE id=$1`, att.ID, string(att.State), finishedAt, errType, errMsg, jsonOrEmpty(result)); err != nil {
		return err
	}
	s.addAuditTx(ctx, tx, &domain.AuditEvent{TaskID: att.TaskID, WorkerID: att.WorkerID,
		Event: "complete:" + string(next), FromState: fromState, ToState: string(next),
		Detail: firstNonEmpty(errMsg, string(att.State)), CreatedAt: finishedAt})
	return tx.Commit(ctx)
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func (s *PgStore) RequeueRunning(ctx context.Context, taskID string, now time.Time, reason string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var from string
	if err := tx.QueryRow(ctx, `SELECT state FROM tasks WHERE id=$1 FOR UPDATE`, taskID).Scan(&from); err != nil {
		return mapErr(err)
	}
	if from != string(domain.StateRunning) {
		return domain.ErrConflict
	}
	if _, err := tx.Exec(ctx, `
		UPDATE tasks SET state='ready', ready_at=$2, updated_at=$2, worker_id=''
		WHERE id=$1`, taskID, now); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE attempts SET state='timeout', ended_at=$2, error_message=$3
		WHERE task_id=$1 AND state='running'`, taskID, now, reason); err != nil {
		return err
	}
	s.addAuditTx(ctx, tx, &domain.AuditEvent{TaskID: taskID, Event: "requeue",
		FromState: "running", ToState: "ready", Detail: reason, CreatedAt: now})
	return tx.Commit(ctx)
}

// FailAttemptForRetry marks the running attempt failed and reschedules the
// task to pending in one transaction (no transient dead state).
func (s *PgStore) FailAttemptForRetry(ctx context.Context, att *domain.Attempt,
	errMsg, errType string, runAt time.Time) error {

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var from string
	if err := tx.QueryRow(ctx, `SELECT state FROM tasks WHERE id=$1 FOR UPDATE`, att.TaskID).Scan(&from); err != nil {
		return mapErr(err)
	}
	if from != string(domain.StateRunning) {
		return domain.ErrConflict
	}
	if _, err := tx.Exec(ctx, `
		UPDATE tasks SET state='pending', run_at=$2, updated_at=$2, worker_id='',
			last_error=$3, error_type=$4
		WHERE id=$1`, att.TaskID, runAt, errMsg, errType); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE attempts SET state='failed', ended_at=$2, error_type=$3, error_message=$4
		WHERE id=$1`, att.ID, runAt, errType, errMsg); err != nil {
		return err
	}
	s.addAuditTx(ctx, tx, &domain.AuditEvent{TaskID: att.TaskID, WorkerID: att.WorkerID,
		Event: "retry-scheduled", FromState: "running", ToState: "pending",
		Detail: "run_at=" + runAt.Format(time.RFC3339), CreatedAt: runAt})
	return tx.Commit(ctx)
}

func (s *PgStore) TagDAG(ctx context.Context, taskID, dagID, nodeID string) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE tasks SET dag_id=$2, dag_node=$3, updated_at=$4 WHERE id=$1`,
		taskID, dagID, nodeID, time.Now())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrNotFound
	}
	return nil
}

func (s *PgStore) RequeueDead(ctx context.Context, ids []string, now time.Time) (int64, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `
		UPDATE tasks SET state='ready', ready_at=$2, updated_at=$2,
			finished_at=NULL, last_error='', error_type=''
		WHERE id = ANY($1) AND state='dead'`, ids, now)
	if err != nil {
		return 0, err
	}
	n := tag.RowsAffected()
	if n > 0 {
		s.addAuditTx(ctx, tx, &domain.AuditEvent{Event: "dead-retry-batch",
			ToState: "ready", Detail: fmt.Sprintf("%d tasks", n), CreatedAt: now})
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return n, nil
}

func (s *PgStore) DiscardDead(ctx context.Context, ids []string, now time.Time) (int64, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `
		UPDATE tasks SET state='canceled', finished_at=$2, updated_at=$2
		WHERE id = ANY($1) AND state='dead'`, ids, now)
	if err != nil {
		return 0, err
	}
	n := tag.RowsAffected()
	if n > 0 {
		s.addAuditTx(ctx, tx, &domain.AuditEvent{Event: "dead-discard-batch",
			ToState: "canceled", Detail: fmt.Sprintf("%d tasks", n), CreatedAt: now})
	}
	return n, tx.Commit(ctx)
}

func (s *PgStore) ScheduleRetry(ctx context.Context, taskID string, runAt time.Time) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var from string
	if err := tx.QueryRow(ctx, `SELECT state FROM tasks WHERE id=$1 FOR UPDATE`, taskID).Scan(&from); err != nil {
		return mapErr(err)
	}
	if from != string(domain.StateRunning) {
		return domain.ErrConflict
	}
	if _, err := tx.Exec(ctx, `
		UPDATE tasks SET state='pending', run_at=$2, updated_at=$2, worker_id=''
		WHERE id=$1`, taskID, runAt); err != nil {
		return err
	}
	s.addAuditTx(ctx, tx, &domain.AuditEvent{TaskID: taskID, Event: "retry-scheduled",
		FromState: "running", ToState: "pending",
		Detail: "run_at=" + runAt.Format(time.RFC3339), CreatedAt: runAt})
	return tx.Commit(ctx)
}

const attemptCols = `id, task_id, attempt_no, worker_id, state, started_at, ended_at,
	error_type, error_message, result`

func scanAttempt(row pgx.Row) (*domain.Attempt, error) {
	var a domain.Attempt
	var res []byte
	var ended *time.Time
	err := row.Scan(&a.ID, &a.TaskID, &a.AttemptNo, &a.WorkerID, &a.State,
		&a.StartedAt, &ended, &a.ErrorType, &a.ErrorMessage, &res)
	if err != nil {
		return nil, mapErr(err)
	}
	a.EndedAt = ended
	a.Result = res
	return &a, nil
}

func (s *PgStore) ListAttempts(ctx context.Context, taskID string) ([]*domain.Attempt, error) {
	rows, err := s.pool.Query(ctx, "SELECT "+attemptCols+" FROM attempts WHERE task_id=$1 ORDER BY attempt_no", taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.Attempt
	for rows.Next() {
		a, err := scanAttempt(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *PgStore) RecoverableTasks(ctx context.Context) ([]*domain.Task, error) {
	rows, err := s.pool.Query(ctx,
		"SELECT "+taskCols+" FROM tasks WHERE state IN ('ready','pending','running')")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return collectTasks(rows)
}

// ReapRunningOnWorker requeues all running tasks of a lost worker and
// returns the recovered tasks.
func (s *PgStore) ReapRunningOnWorker(ctx context.Context, workerID string, now time.Time, offline bool) ([]*domain.Task, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT id FROM tasks
		WHERE worker_id=$1 AND state='running' FOR UPDATE`, workerID)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()

	if len(ids) > 0 {
		if _, err := tx.Exec(ctx, `
			UPDATE tasks SET state='ready', ready_at=$2, updated_at=$2, worker_id=''
			WHERE id = ANY($1)`, ids, now); err != nil {
			return nil, err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE attempts SET state='timeout', ended_at=$2, error_message='worker lost'
			WHERE worker_id=$1 AND state='running'`, workerID, now); err != nil {
			return nil, err
		}
	}
	if offline {
		if _, err := tx.Exec(ctx,
			`UPDATE workers SET status='offline', active_slots=0 WHERE id=$1`, workerID); err != nil {
			return nil, err
		}
	}
	s.addAuditTx(ctx, tx, &domain.AuditEvent{WorkerID: workerID, Event: "worker-reaped",
		Detail: fmt.Sprintf("requeued %d tasks", len(ids)), CreatedAt: now})
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	return s.GetTasks(ctx, ids)
}

func (s *PgStore) MarkWorkersOfflineBefore(ctx context.Context, cutoff time.Time) ([]string, error) {
	rows, err := s.pool.Query(ctx,
		`UPDATE workers SET status='offline', active_slots=0
		 WHERE status IN ('online','draining') AND last_heartbeat < $1 RETURNING id`, cutoff)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *PgStore) CancelTask(ctx context.Context, id string, now time.Time) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE tasks SET state='canceled', finished_at=$2, updated_at=$2
		 WHERE id=$1 AND state IN ('ready','pending')`, id, now)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrConflict
	}
	return s.AddAudit(ctx, &domain.AuditEvent{TaskID: id, Event: "cancel",
		FromState: "ready/pending", ToState: "canceled", CreatedAt: now})
}

func (s *PgStore) DeadStats(ctx context.Context) ([]DeadStat, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT COALESCE(NULLIF(error_type,''),'unknown') AS et, count(*)
		FROM tasks WHERE state='dead' GROUP BY et ORDER BY count(*) DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DeadStat
	for rows.Next() {
		var d DeadStat
		if err := rows.Scan(&d.ErrorType, &d.Count); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func (s *PgStore) CountDead(ctx context.Context) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx, `SELECT count(*) FROM tasks WHERE state='dead'`).Scan(&n)
	return n, err
}

func newID() string { return uuid.NewString() }
