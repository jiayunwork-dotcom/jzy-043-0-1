package store

import (
	"context"
	"encoding/json"
	"time"

	"taskforge/internal/domain"
)

func (s *PgStore) CreateDAG(ctx context.Context, d *domain.DAG) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `
		INSERT INTO dags (id, name, status, failure_policy, created_at)
		VALUES ($1,$2,$3,$4,$5)`,
		d.ID, d.Name, string(d.Status), string(d.FailurePolicy), d.CreatedAt); err != nil {
		return err
	}
	for _, n := range d.Nodes {
		defJSON, _ := json.Marshal(n.Def)
		depJSON, _ := json.Marshal(n.DependsOn)
		if _, err := tx.Exec(ctx, `
			INSERT INTO dag_nodes (dag_id, node_id, task_id, state, depends_on, def,
				started_at, finished_at, error)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
			n.DAGID, n.NodeID, n.TaskID, string(n.State), depJSON, defJSON,
			n.StartedAt, n.FinishedAt, n.Error); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (s *PgStore) GetDAG(ctx context.Context, id string) (*domain.DAG, error) {
	d := &domain.DAG{ID: id}
	var status, policy string
	err := s.pool.QueryRow(ctx,
		`SELECT name, status, failure_policy, created_at, finished_at FROM dags WHERE id=$1`, id).
		Scan(&d.Name, &status, &policy, &d.CreatedAt, &d.FinishedAt)
	if err != nil {
		return nil, mapErr(err)
	}
	d.Status = domain.DAGStatus(status)
	d.FailurePolicy = domain.NodeFailurePolicy(policy)

	rows, err := s.pool.Query(ctx, `
		SELECT node_id, task_id, state, depends_on, def, started_at, finished_at, error
		FROM dag_nodes WHERE dag_id=$1 ORDER BY node_id`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			n               domain.DAGNode
			state, dep, def []byte
			stateStr        string
			started, ended  *time.Time
			taskID          string
			errMsg          string
		)
		if err := rows.Scan(&n.NodeID, &taskID, &stateStr, &dep, &def,
			&started, &ended, &errMsg); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(dep, &n.DependsOn); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(def, &n.Def); err != nil {
			return nil, err
		}
		n.DAGID = id
		n.TaskID = taskID
		n.State = domain.NodeState(stateStr)
		n.StartedAt = started
		n.FinishedAt = ended
		n.Error = errMsg
		_ = state
		d.Nodes = append(d.Nodes, &n)
	}
	return d, rows.Err()
}

func (s *PgStore) ListDAGs(ctx context.Context, limit int) ([]*domain.DAG, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx,
		`SELECT id, name, status, failure_policy, created_at, finished_at
		 FROM dags ORDER BY created_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.DAG
	for rows.Next() {
		var d domain.DAG
		var status, policy string
		if err := rows.Scan(&d.ID, &d.Name, &status, &policy, &d.CreatedAt, &d.FinishedAt); err != nil {
			return nil, err
		}
		d.Status = domain.DAGStatus(status)
		d.FailurePolicy = domain.NodeFailurePolicy(policy)
		out = append(out, &d)
	}
	return out, rows.Err()
}

func (s *PgStore) SaveNode(ctx context.Context, n *domain.DAGNode) error {
	defJSON, _ := json.Marshal(n.Def)
	depJSON, _ := json.Marshal(n.DependsOn)
	tag, err := s.pool.Exec(ctx, `
		UPDATE dag_nodes SET task_id=$3, state=$4, depends_on=$5, def=$6,
			started_at=$7, finished_at=$8, error=$9
		WHERE dag_id=$1 AND node_id=$2`,
		n.DAGID, n.NodeID, n.TaskID, string(n.State), depJSON, defJSON,
		n.StartedAt, n.FinishedAt, n.Error)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrNotFound
	}
	return s.AddAudit(ctx, &domain.AuditEvent{DAGID: n.DAGID, TaskID: n.TaskID,
		Event: "node:" + string(n.State), ToState: string(n.State),
		Detail: n.NodeID, CreatedAt: time.Now()})
}

func (s *PgStore) SaveDAGStatus(ctx context.Context, id string, st domain.DAGStatus, finishedAt *time.Time) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE dags SET status=$2, finished_at=$3 WHERE id=$1`, id, string(st), finishedAt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrNotFound
	}
	return s.AddAudit(ctx, &domain.AuditEvent{DAGID: id, Event: "dag:" + string(st),
		ToState: string(st), CreatedAt: time.Now()})
}
