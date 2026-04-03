package multiraft_test

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/replication/multiraft"
)

func TestNewValidatesRequiredOptions(t *testing.T) {
	_, err := multiraft.New(multiraft.Options{})
	if !errors.Is(err, multiraft.ErrInvalidOptions) {
		t.Fatalf("expected ErrInvalidOptions, got %v", err)
	}
}

func TestPublicTypesExposeApprovedFields(t *testing.T) {
	var opts multiraft.Options
	opts.NodeID = 1
	opts.TickInterval = time.Second
	opts.Workers = 1

	if opts.NodeID != 1 {
		t.Fatalf("unexpected NodeID: %d", opts.NodeID)
	}
}

func TestPublicAPIReflectsLearnerSupport(t *testing.T) {
	reqType := reflect.TypeOf(multiraft.BootstrapGroupRequest{})
	if _, ok := reqType.FieldByName("Learners"); ok {
		t.Fatal("BootstrapGroupRequest unexpectedly exposes Learners without end-to-end support")
	}

	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller() failed")
	}
	typesPath := filepath.Join(filepath.Dir(file), "types.go")

	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, typesPath, nil, 0)
	if err != nil {
		t.Fatalf("ParseFile() error = %v", err)
	}

	ast.Inspect(parsed, func(node ast.Node) bool {
		valueSpec, ok := node.(*ast.ValueSpec)
		if !ok {
			return true
		}
		for _, name := range valueSpec.Names {
			if name.Name == "RoleLearner" {
				t.Fatal("RoleLearner is exported but runtime does not surface learner status")
			}
		}
		return true
	})
}

func TestOpenGroupRegistersGroup(t *testing.T) {
	rt := newTestRuntime(t)
	err := rt.OpenGroup(context.Background(), multiraft.GroupOptions{
		ID:           10,
		Storage:      newFakeStorage(),
		StateMachine: newFakeStateMachine(),
	})
	if err != nil {
		t.Fatalf("OpenGroup() error = %v", err)
	}
	if got := rt.Groups(); !reflect.DeepEqual(got, []multiraft.GroupID{10}) {
		t.Fatalf("Groups() = %v", got)
	}
}

func TestOpenGroupRejectsDuplicateID(t *testing.T) {
	rt := newTestRuntime(t)
	if err := rt.OpenGroup(context.Background(), newGroupOptions(10)); err != nil {
		t.Fatalf("first OpenGroup() error = %v", err)
	}
	err := rt.OpenGroup(context.Background(), newGroupOptions(10))
	if !errors.Is(err, multiraft.ErrGroupExists) {
		t.Fatalf("expected ErrGroupExists, got %v", err)
	}
}

func TestBootstrapGroupCreatesInitialMembership(t *testing.T) {
	rt := newTestRuntime(t)
	err := rt.BootstrapGroup(context.Background(), multiraft.BootstrapGroupRequest{
		Group:  newGroupOptions(20),
		Voters: []multiraft.NodeID{1, 2, 3},
	})
	if err != nil {
		t.Fatalf("BootstrapGroup() error = %v", err)
	}
	st, err := rt.Status(20)
	if err != nil {
		t.Fatalf("Status() error = %v", err)
	}
	if st.GroupID != multiraft.GroupID(20) {
		t.Fatalf("Status().GroupID = %d", st.GroupID)
	}
}

func TestCloseGroupMakesFutureOperationsFail(t *testing.T) {
	rt := newTestRuntime(t)
	if err := rt.OpenGroup(context.Background(), newGroupOptions(10)); err != nil {
		t.Fatalf("OpenGroup() error = %v", err)
	}
	if err := rt.CloseGroup(context.Background(), 10); err != nil {
		t.Fatalf("CloseGroup() error = %v", err)
	}
	_, err := rt.Status(10)
	if !errors.Is(err, multiraft.ErrGroupNotFound) {
		t.Fatalf("expected ErrGroupNotFound, got %v", err)
	}
}
