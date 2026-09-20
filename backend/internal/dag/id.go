package dag

import (
	"github.com/google/uuid"

	"taskforge/internal/domain"
)

func newDAGID() string { return "dag_" + uuid.NewString() }

// retriesUsed returns how many node-level retries already happened.
func retriesUsed(n domain.DAGNodeDef) int { return n.RetriesUsed }

func bumpRetries(n *domain.DAGNodeDef) { n.RetriesUsed++ }
