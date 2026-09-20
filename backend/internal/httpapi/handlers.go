package httpapi

import (
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"

	"taskforge/internal/dag"
	"taskforge/internal/domain"
	"taskforge/internal/store"
)

func (s *Server) submitTask(c *fiber.Ctx) error {
	var req submitReq
	if err := c.BodyParser(&req); err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid JSON: "+err.Error())
	}
	pri, err := domain.ParsePriority(req.Priority)
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, err.Error())
	}
	payload, _ := json.Marshal(req.Payload)
	in := engineSubmit(req, pri, payload)
	t, idem, err := s.eng.Submit(c.Context(), in)
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, err.Error())
	}
	c.Status(fiber.StatusCreated)
	if idem {
		c.Status(fiber.StatusOK)
	}
	return c.JSON(fiber.Map{"task": toTaskResp(t), "idempotent_hit": idem})
}

func (s *Server) listTasks(c *fiber.Ctx) error {
	f := store.TaskFilter{
		Limit:  c.QueryInt("limit", 100),
		Offset: c.QueryInt("offset", 0),
	}
	if v := c.Query("state"); v != "" {
		for _, part := range splitCSV(v) {
			f.States = append(f.States, domain.TaskState(part))
		}
	}
	if v := c.Query("priority"); v != "" {
		for _, part := range splitCSV(v) {
			p, err := domain.ParsePriority(part)
			if err == nil {
				f.Priorities = append(f.Priorities, p)
			}
		}
	}
	if v := c.Query("type"); v != "" {
		f.Types = splitCSV(v)
	}
	if v := c.Query("from"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			f.From = &t
		}
	}
	if v := c.Query("to"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			f.To = &t
		}
	}
	f.DAGID = c.Query("dag_id")

	res, err := s.repo.ListTasks(c.Context(), f)
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, err.Error())
	}
	out := make([]taskResp, 0, len(res.Tasks))
	for _, t := range res.Tasks {
		out = append(out, toTaskResp(t))
	}
	return c.JSON(fiber.Map{"tasks": out, "total": res.Total})
}

func (s *Server) getTask(c *fiber.Ctx) error {
	t, err := s.repo.GetTask(c.Context(), c.Params("id"))
	if err != nil {
		return mapStoreErr(err)
	}
	attempts, _ := s.repo.ListAttempts(c.Context(), t.ID)
	ar := make([]attemptResp, 0, len(attempts))
	for _, a := range attempts {
		ar = append(ar, toAttemptResp(a))
	}
	audit, _ := s.repo.ListAudit(c.Context(), t.ID, "", 200)
	return c.JSON(fiber.Map{"task": toTaskResp(t), "attempts": ar, "audit": audit})
}

func (s *Server) cancelTask(c *fiber.Ctx) error {
	if err := s.repo.CancelTask(c.Context(), c.Params("id"), time.Now()); err != nil {
		return mapStoreErr(err)
	}
	return c.JSON(fiber.Map{"ok": true})
}

func (s *Server) taskTypes(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{"types": s.eng.Registry().Types()})
}

// ---- dead letter ---------------------------------------------------------

func (s *Server) listDead(c *fiber.Ctx) error {
	res, err := s.repo.ListTasks(c.Context(), store.TaskFilter{
		States: []domain.TaskState{domain.StateDead},
		Limit:  c.QueryInt("limit", 200),
	})
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, err.Error())
	}
	out := make([]taskResp, 0, len(res.Tasks))
	for _, t := range res.Tasks {
		out = append(out, toTaskResp(t))
	}
	stats, _ := s.repo.DeadStats(c.Context())
	return c.JSON(fiber.Map{"tasks": out, "total": res.Total, "error_stats": stats})
}

type batchReq struct {
	IDs []string `json:"ids"`
}

func (s *Server) retryDead(c *fiber.Ctx) error {
	var req batchReq
	if err := c.BodyParser(&req); err != nil {
		return fiber.NewError(fiber.StatusBadRequest, err.Error())
	}
	n, err := s.eng.RetryDead(c.Context(), req.IDs)
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, err.Error())
	}
	return c.JSON(fiber.Map{"requeued": n})
}

func (s *Server) discardDead(c *fiber.Ctx) error {
	var req batchReq
	if err := c.BodyParser(&req); err != nil {
		return fiber.NewError(fiber.StatusBadRequest, err.Error())
	}
	n, err := s.eng.DiscardDead(c.Context(), req.IDs)
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, err.Error())
	}
	return c.JSON(fiber.Map{"discarded": n})
}

func (s *Server) deadStats(c *fiber.Ctx) error {
	stats, err := s.repo.DeadStats(c.Context())
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, err.Error())
	}
	return c.JSON(fiber.Map{"stats": stats})
}

// ---- workers -------------------------------------------------------------

func (s *Server) listWorkers(c *fiber.Ctx) error {
	details, err := s.eng.WorkerDetails(c.Context())
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, err.Error())
	}
	return c.JSON(fiber.Map{"workers": details})
}

func (s *Server) drainWorker(c *fiber.Ctx) error {
	if err := s.eng.DrainWorker(c.Context(), c.Params("id")); err != nil {
		return mapStoreErr(err)
	}
	return c.JSON(fiber.Map{"ok": true})
}

func (s *Server) simulateLoss(c *fiber.Ctx) error {
	if err := s.eng.SimulateWorkerLoss(c.Context(), c.Params("id")); err != nil {
		return mapStoreErr(err)
	}
	return c.JSON(fiber.Map{"ok": true})
}

// ---- DAG -----------------------------------------------------------------

type nodeDefReq struct {
	ID          string   `json:"id"`
	TaskType    string   `json:"task_type"`
	Payload     any      `json:"payload"`
	Priority    string   `json:"priority"`
	TimeoutSecs float64  `json:"timeout_seconds"`
	DependsOn   []string `json:"depends_on"`
	OnFailure   string   `json:"on_failure"`
	MaxRetries  int      `json:"max_retries"`
}

type dagReq struct {
	Name          string       `json:"name"`
	FailurePolicy string       `json:"failure_policy"`
	Nodes         []nodeDefReq `json:"nodes"`
}

func (r dagReq) toDef() (*domain.DAGDef, error) {
	def := &domain.DAGDef{Name: r.Name,
		FailurePolicy: domain.NodeFailurePolicy(r.FailurePolicy)}
	for _, n := range r.Nodes {
		pri, err := domain.ParsePriority(n.Priority)
		if err != nil {
			return nil, err
		}
		raw, _ := json.Marshal(n.Payload)
		def.Nodes = append(def.Nodes, domain.DAGNodeDef{
			ID: n.ID, TaskType: n.TaskType, Payload: raw, Priority: pri,
			Timeout:    domain.Duration(time.Duration(n.TimeoutSecs * float64(time.Second))),
			DependsOn:  n.DependsOn,
			OnFailure:  domain.NodeFailurePolicy(n.OnFailure),
			MaxRetries: n.MaxRetries,
		})
	}
	return def, nil
}

func (s *Server) createDAG(c *fiber.Ctx) error {
	var req dagReq
	if err := c.BodyParser(&req); err != nil {
		return fiber.NewError(fiber.StatusBadRequest, err.Error())
	}
	def, err := req.toDef()
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, err.Error())
	}
	// Cycle detection runs explicitly before instantiation.
	if err := dag.Validate(def); err != nil {
		status := fiber.StatusBadRequest
		if errors.Is(err, domain.ErrCycle) {
			status = fiber.StatusUnprocessableEntity
		}
		return fiber.NewError(status, err.Error())
	}
	d, err := s.eng.DAGs().Instantiate(c.Context(), def)
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, err.Error())
	}
	c.Status(fiber.StatusCreated)
	return c.JSON(fiber.Map{"dag": dagResp(d)})
}

func (s *Server) validateDAG(c *fiber.Ctx) error {
	var req dagReq
	if err := c.BodyParser(&req); err != nil {
		return fiber.NewError(fiber.StatusBadRequest, err.Error())
	}
	def, err := req.toDef()
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, err.Error())
	}
	if err := dag.Validate(def); err != nil {
		status := fiber.StatusBadRequest
		if errors.Is(err, domain.ErrCycle) {
			status = fiber.StatusUnprocessableEntity
		}
		return fiber.NewError(status, err.Error())
	}
	levels, _ := dag.Levels(def)
	return c.JSON(fiber.Map{"valid": true, "levels": levels})
}

func (s *Server) listDAGs(c *fiber.Ctx) error {
	ds, err := s.repo.ListDAGs(c.Context(), 100)
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, err.Error())
	}
	return c.JSON(fiber.Map{"dags": ds})
}

func (s *Server) getDAG(c *fiber.Ctx) error {
	d, err := s.repo.GetDAG(c.Context(), c.Params("id"))
	if err != nil {
		return mapStoreErr(err)
	}
	levels, _ := dag.Levels(&domain.DAGDef{
		Name: d.Name, FailurePolicy: d.FailurePolicy, Nodes: nodeDefs(d)})
	return c.JSON(fiber.Map{"dag": dagResp(d), "levels": levels})
}

func (s *Server) audit(c *fiber.Ctx) error {
	limit := 200
	if v := c.Query("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			limit = n
		}
	}
	events, err := s.repo.ListAudit(c.Context(), c.Query("task_id"), c.Query("dag_id"), limit)
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, err.Error())
	}
	return c.JSON(fiber.Map{"audit": events})
}

func splitCSV(s string) []string {
	var out []string
	cur := ""
	for _, r := range s {
		if r == ',' {
			if cur != "" {
				out = append(out, cur)
			}
			cur = ""
			continue
		}
		cur += string(r)
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}

func mapStoreErr(err error) error {
	switch {
	case errors.Is(err, domain.ErrNotFound):
		return fiber.NewError(fiber.StatusNotFound, err.Error())
	case errors.Is(err, domain.ErrConflict):
		return fiber.NewError(fiber.StatusConflict, err.Error())
	case errors.Is(err, domain.ErrCycle):
		return fiber.NewError(fiber.StatusUnprocessableEntity, err.Error())
	default:
		return fiber.NewError(fiber.StatusInternalServerError, err.Error())
	}
}
