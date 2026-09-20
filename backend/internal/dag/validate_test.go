package dag

import (
	"errors"
	"testing"

	"taskforge/internal/domain"
)

func TestValidateAcyclic(t *testing.T) {
	def := &domain.DAGDef{Nodes: []domain.DAGNodeDef{
		{ID: "A"},
		{ID: "B", DependsOn: []string{"A"}},
		{ID: "C", DependsOn: []string{"A"}},
		{ID: "D", DependsOn: []string{"B", "C"}},
	}}
	if err := Validate(def); err != nil {
		t.Fatalf("valid dag rejected: %v", err)
	}
	levels, err := Levels(def)
	if err != nil {
		t.Fatal(err)
	}
	if len(levels) != 3 {
		t.Fatalf("levels=%d want 3: %v", len(levels), levels)
	}
	if len(levels[0]) != 1 || levels[0][0] != "A" {
		t.Fatalf("root level wrong: %v", levels[0])
	}
	if len(levels[2]) != 1 || levels[2][0] != "D" {
		t.Fatalf("leaf level wrong: %v", levels[2])
	}
}

func TestValidateCycle(t *testing.T) {
	def := &domain.DAGDef{Nodes: []domain.DAGNodeDef{
		{ID: "A", DependsOn: []string{"B"}},
		{ID: "B", DependsOn: []string{"C"}},
		{ID: "C", DependsOn: []string{"A"}},
	}}
	err := Validate(def)
	if !errors.Is(err, domain.ErrCycle) {
		t.Fatalf("want ErrCycle, got %v", err)
	}
}

func TestValidateSelfLoopAndDangling(t *testing.T) {
	self := &domain.DAGDef{Nodes: []domain.DAGNodeDef{{ID: "A", DependsOn: []string{"A"}}}}
	if err := Validate(self); !errors.Is(err, domain.ErrCycle) {
		t.Fatalf("self loop: want cycle, got %v", err)
	}
	dangling := &domain.DAGDef{Nodes: []domain.DAGNodeDef{{ID: "A", DependsOn: []string{"X"}}}}
	if err := Validate(dangling); err == nil {
		t.Fatal("dangling dependency must be rejected")
	}
}
