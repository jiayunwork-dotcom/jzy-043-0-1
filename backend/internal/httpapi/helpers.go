package httpapi

import (
	"time"

	"taskforge/internal/domain"
	"taskforge/internal/engine"
)

func engineSubmit(req submitReq, pri domain.Priority, payload []byte) engine.SubmitInput {
	in := engine.SubmitInput{
		Type:           req.Type,
		Payload:        payload,
		Priority:       pri,
		DelaySeconds:   req.DelaySeconds,
		TimeoutSeconds: req.TimeoutSeconds,
		MaxRetries:     req.MaxRetries,
		CallbackURL:    req.CallbackURL,
		IdempotencyKey: req.IdempotencyKey,
	}
	if req.RetryPolicy != nil {
		kind := domain.RetryKind(req.RetryPolicy.Kind)
		if kind == "" {
			kind = domain.RetryExponential
		}
		in.RetryPolicy = &domain.RetryPolicy{
			Kind:         kind,
			BaseInterval: domain.Duration(time.Duration(req.RetryPolicy.BaseSeconds * float64(time.Second))),
			Cron:         req.RetryPolicy.Cron,
			MaxRetries:   req.RetryPolicy.MaxRetries,
		}
	}
	return in
}

func nodeDefs(d *domain.DAG) []domain.DAGNodeDef {
	out := make([]domain.DAGNodeDef, 0, len(d.Nodes))
	for _, n := range d.Nodes {
		out = append(out, n.Def)
	}
	return out
}

func dagResp(d *domain.DAG) any {
	return d
}
