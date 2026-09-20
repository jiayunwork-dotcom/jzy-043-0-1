package store

import (
	"context"
	"time"

	"taskforge/internal/domain"
)

const workerCols = `id, total_slots, active_slots, status, last_heartbeat, started_at,
	succeeded, failed, preempted`

func scanWorker(row interface {
	Scan(dest ...any) error
}) (*domain.Worker, error) {
	var w domain.Worker
	var status string
	err := row.Scan(&w.ID, &w.TotalSlots, &w.ActiveSlots, &status, &w.LastHeartbeat,
		&w.StartedAt, &w.Succeeded, &w.Failed, &w.Preempted)
	if err != nil {
		return nil, mapErr(err)
	}
	w.Status = domain.WorkerStatus(status)
	return &w, nil
}

func (s *PgStore) CreateWorker(ctx context.Context, w *domain.Worker) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO workers (id, total_slots, active_slots, status, last_heartbeat, started_at,
			succeeded, failed, preempted)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		ON CONFLICT (id) DO UPDATE SET status='online', total_slots=EXCLUDED.total_slots,
			last_heartbeat=EXCLUDED.last_heartbeat`,
		w.ID, w.TotalSlots, w.ActiveSlots, string(w.Status), w.LastHeartbeat, w.StartedAt,
		w.Succeeded, w.Failed, w.Preempted)
	return err
}

func (s *PgStore) HeartbeatWorker(ctx context.Context, id string, slots, active int, now time.Time) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE workers SET last_heartbeat=$2, active_slots=$3,
			status=CASE WHEN status='offline' THEN 'online' ELSE status END
		WHERE id=$1`, id, now, active)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrNotFound
	}
	return nil
}

func (s *PgStore) DrainWorker(ctx context.Context, id string, now time.Time) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE workers SET status='draining', last_heartbeat=$2 WHERE id=$1 AND status='online'`,
		id, now)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrConflict
	}
	return s.AddAudit(ctx, &domain.AuditEvent{WorkerID: id, Event: "drain",
		ToState: "draining", CreatedAt: now})
}

func (s *PgStore) ListWorkers(ctx context.Context) ([]*domain.Worker, error) {
	rows, err := s.pool.Query(ctx, "SELECT "+workerCols+" FROM workers ORDER BY started_at")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.Worker
	for rows.Next() {
		w, err := scanWorker(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

func (s *PgStore) WorkerStats(ctx context.Context) (online, draining, offline int64, err error) {
	rows, err := s.pool.Query(ctx, `SELECT status, count(*) FROM workers GROUP BY status`)
	if err != nil {
		return 0, 0, 0, err
	}
	defer rows.Close()
	for rows.Next() {
		var status string
		var n int64
		if err := rows.Scan(&status, &n); err != nil {
			return 0, 0, 0, err
		}
		switch domain.WorkerStatus(status) {
		case domain.WorkerOnline:
			online = n
		case domain.WorkerDraining:
			draining = n
		case domain.WorkerOffline:
			offline = n
		}
	}
	return online, draining, offline, rows.Err()
}

func (s *PgStore) BumpWorkerCounters(ctx context.Context, id string, succeeded, failed, preempted int) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE workers SET succeeded = succeeded + $2,
			failed = failed + $3, preempted = preempted + $4
		WHERE id=$1`, id, succeeded, failed, preempted)
	return err
}

func (s *PgStore) AddAudit(ctx context.Context, e *domain.AuditEvent) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO audit_events
		(task_id, dag_id, worker_id, event, from_state, to_state, detail, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		e.TaskID, e.DAGID, e.WorkerID, e.Event, e.FromState, e.ToState, e.Detail, e.CreatedAt)
	return err
}

func (s *PgStore) ListAudit(ctx context.Context, taskID, dagID string, limit int) ([]*domain.AuditEvent, error) {
	if limit <= 0 {
		limit = 200
	}
	q := "SELECT id, task_id, dag_id, worker_id, event, from_state, to_state, detail, created_at " +
		"FROM audit_events WHERE 1=1"
	var args []any
	if taskID != "" {
		args = append(args, taskID)
		q += " AND task_id = $" + itoa(len(args))
	}
	if dagID != "" {
		args = append(args, dagID)
		q += " AND dag_id = $" + itoa(len(args))
	}
	args = append(args, limit)
	q += " ORDER BY id DESC LIMIT $" + itoa(len(args))
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.AuditEvent
	for rows.Next() {
		var a domain.AuditEvent
		if err := rows.Scan(&a.ID, &a.TaskID, &a.DAGID, &a.WorkerID, &a.Event,
			&a.FromState, &a.ToState, &a.Detail, &a.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, &a)
	}
	return out, rows.Err()
}
