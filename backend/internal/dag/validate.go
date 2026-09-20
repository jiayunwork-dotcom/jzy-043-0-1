// Package dag implements DAG definition validation (cycle detection) and
// runtime orchestration (dependency unlocking and node failure policies).
package dag

import (
	"fmt"

	"taskforge/internal/domain"
)

// Validate checks: unique node ids, no dangling dependencies, and no cycle.
func Validate(def *domain.DAGDef) error {
	if def == nil || len(def.Nodes) == 0 {
		return fmt.Errorf("dag has no nodes")
	}
	deps := make(map[string][]string, len(def.Nodes))
	seen := map[string]bool{}
	for _, n := range def.Nodes {
		if n.ID == "" {
			return fmt.Errorf("node with empty id")
		}
		if seen[n.ID] {
			return fmt.Errorf("duplicate node id %q", n.ID)
		}
		seen[n.ID] = true
		deps[n.ID] = n.DependsOn
	}
	for id, ds := range deps {
		for _, d := range ds {
			if !seen[d] {
				return fmt.Errorf("node %q depends on unknown node %q", id, d)
			}
			if d == id {
				return fmt.Errorf("%w: node %q depends on itself", domain.ErrCycle, id)
			}
		}
	}
	return detectCycle(deps)
}

// detectCycle runs DFS with three-color marking.
func detectCycle(deps map[string][]string) error {
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := map[string]int{}

	var visit func(id string, path []string) error
	visit = func(id string, path []string) error {
		switch color[id] {
		case gray:
			return fmt.Errorf("%w: %v -> %s", domain.ErrCycle, path, id)
		case black:
			return nil
		}
		color[id] = gray
		for _, d := range deps[id] {
			if err := visit(d, append(path, id)); err != nil {
				return err
			}
		}
		color[id] = black
		return nil
	}

	for id := range deps {
		if color[id] == white {
			if err := visit(id, nil); err != nil {
				return err
			}
		}
	}
	return nil
}

// Levels returns nodes grouped by topological layer (root nodes first),
// used by the frontend hierarchy view.
func Levels(def *domain.DAGDef) ([][]string, error) {
	if err := Validate(def); err != nil {
		return nil, err
	}
	indeg := map[string]int{}
	adj := map[string][]string{}
	nodes := map[string]*domain.DAGNodeDef{}
	for i := range def.Nodes {
		n := &def.Nodes[i]
		nodes[n.ID] = n
		indeg[n.ID] = len(n.DependsOn)
		for _, d := range n.DependsOn {
			adj[d] = append(adj[d], n.ID)
		}
	}
	var front []string
	for id, d := range indeg {
		if d == 0 {
			front = append(front, id)
		}
	}
	var levels [][]string
	visited := 0
	for len(front) > 0 {
		levels = append(levels, front)
		var next []string
		for _, id := range front {
			visited++
			for _, child := range adj[id] {
				indeg[child]--
				if indeg[child] == 0 {
					next = append(next, child)
				}
			}
		}
		front = next
	}
	if visited != len(def.Nodes) {
		return nil, domain.ErrCycle
	}
	return levels, nil
}
