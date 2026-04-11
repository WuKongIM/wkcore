package wklog

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFieldConstructors(t *testing.T) {
	errBoom := errors.New("boom")
	duration := 5 * time.Second

	tests := []struct {
		name  string
		field Field
		key   string
		typ   FieldType
		value any
	}{
		{name: "string", field: String("module", "cluster"), key: "module", typ: StringType, value: "cluster"},
		{name: "int", field: Int("retries", 3), key: "retries", typ: IntType, value: 3},
		{name: "int64", field: Int64("offset", 9), key: "offset", typ: Int64Type, value: int64(9)},
		{name: "uint64", field: Uint64("nodeID", 7), key: "nodeID", typ: Uint64Type, value: uint64(7)},
		{name: "float64", field: Float64("ratio", 1.5), key: "ratio", typ: Float64Type, value: 1.5},
		{name: "bool", field: Bool("leader", true), key: "leader", typ: BoolType, value: true},
		{name: "error", field: Error(errBoom), key: "error", typ: ErrorType, value: errBoom},
		{name: "duration", field: Duration("latency", duration), key: "latency", typ: DurationType, value: duration},
		{name: "any", field: Any("payload", map[string]int{"count": 1}), key: "payload", typ: AnyType, value: map[string]int{"count": 1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.key, tt.field.Key)
			require.Equal(t, tt.typ, tt.field.Type)
			require.Equal(t, tt.value, tt.field.Value)
		})
	}
}
