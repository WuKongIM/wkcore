package wklog

import "time"

type FieldType int

const (
	StringType FieldType = iota
	IntType
	Int64Type
	Uint64Type
	Float64Type
	BoolType
	ErrorType
	DurationType
	AnyType
)

type Field struct {
	Key   string
	Type  FieldType
	Value any
}

func String(key, val string) Field {
	return Field{Key: key, Type: StringType, Value: val}
}

func Int(key string, val int) Field {
	return Field{Key: key, Type: IntType, Value: val}
}

func Int64(key string, val int64) Field {
	return Field{Key: key, Type: Int64Type, Value: val}
}

func Uint64(key string, val uint64) Field {
	return Field{Key: key, Type: Uint64Type, Value: val}
}

func Float64(key string, val float64) Field {
	return Field{Key: key, Type: Float64Type, Value: val}
}

func Bool(key string, val bool) Field {
	return Field{Key: key, Type: BoolType, Value: val}
}

func Error(err error) Field {
	return Field{Key: "error", Type: ErrorType, Value: err}
}

func Duration(key string, val time.Duration) Field {
	return Field{Key: key, Type: DurationType, Value: val}
}

func Any(key string, val any) Field {
	return Field{Key: key, Type: AnyType, Value: val}
}
