package store

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// ─── parsePath ──────────────────────────────────────────────────────────────

func TestParsePath_Root(t *testing.T) {
	tokens, err := parsePath("$")
	require.NoError(t, err)
	require.Empty(t, tokens)
}

func TestParsePath_SingleField(t *testing.T) {
	tokens, err := parsePath("$.name")
	require.NoError(t, err)
	require.Equal(t, []pathToken{{field: "name", index: -1}}, tokens)
}

func TestParsePath_NestedFields(t *testing.T) {
	tokens, err := parsePath("$.a.b.c")
	require.NoError(t, err)
	require.Equal(t, []pathToken{
		{field: "a", index: -1},
		{field: "b", index: -1},
		{field: "c", index: -1},
	}, tokens)
}

func TestParsePath_ArrayIndex(t *testing.T) {
	tokens, err := parsePath("$.arr[0]")
	require.NoError(t, err)
	require.Equal(t, []pathToken{
		{field: "arr", index: -1},
		{index: 0},
	}, tokens)
}

func TestParsePath_ArrayThenField(t *testing.T) {
	tokens, err := parsePath("$.arr[0].name")
	require.NoError(t, err)
	require.Equal(t, []pathToken{
		{field: "arr", index: -1},
		{index: 0},
		{field: "name", index: -1},
	}, tokens)
}

func TestParsePath_Invalid(t *testing.T) {
	for _, p := range []string{"", "name", "$..", "$.", "$[-1]"} {
		_, err := parsePath(p)
		require.Error(t, err, "path %q should fail", p)
	}
}

// ─── jsonPathGet ────────────────────────────────────────────────────────────

func TestJsonPathGet_Root(t *testing.T) {
	doc := map[string]any{"name": "Alice"}
	val, err := jsonPathGet(doc, "$")
	require.NoError(t, err)
	require.Equal(t, doc, val)
}

func TestJsonPathGet_Field(t *testing.T) {
	doc := map[string]any{"name": "Alice", "age": float64(30)}
	val, err := jsonPathGet(doc, "$.name")
	require.NoError(t, err)
	require.Equal(t, "Alice", val)
}

func TestJsonPathGet_Nested(t *testing.T) {
	doc := map[string]any{
		"address": map[string]any{"city": "NYC"},
	}
	val, err := jsonPathGet(doc, "$.address.city")
	require.NoError(t, err)
	require.Equal(t, "NYC", val)
}

func TestJsonPathGet_Array(t *testing.T) {
	doc := map[string]any{
		"tags": []any{"go", "redis"},
	}
	val, err := jsonPathGet(doc, "$.tags[0]")
	require.NoError(t, err)
	require.Equal(t, "go", val)
}

func TestJsonPathGet_ArrayThenField(t *testing.T) {
	doc := map[string]any{
		"users": []any{
			map[string]any{"name": "Alice"},
			map[string]any{"name": "Bob"},
		},
	}
	val, err := jsonPathGet(doc, "$.users[1].name")
	require.NoError(t, err)
	require.Equal(t, "Bob", val)
}

func TestJsonPathGet_NotFound(t *testing.T) {
	doc := map[string]any{"name": "Alice"}
	_, err := jsonPathGet(doc, "$.missing")
	require.ErrorIs(t, err, ErrPathNotFound)
}

func TestJsonPathGet_IndexOutOfRange(t *testing.T) {
	doc := map[string]any{"arr": []any{1.0}}
	_, err := jsonPathGet(doc, "$.arr[5]")
	require.ErrorIs(t, err, ErrIndexOutOfRange)
}

// ─── jsonPathSet ────────────────────────────────────────────────────────────

func TestJsonPathSet_Root(t *testing.T) {
	doc, err := jsonPathSet(nil, "$", map[string]any{"x": float64(1)})
	require.NoError(t, err)
	require.Equal(t, map[string]any{"x": float64(1)}, doc)
}

func TestJsonPathSet_Field(t *testing.T) {
	var doc any = map[string]any{"name": "Alice", "age": float64(30)}
	doc, err := jsonPathSet(doc, "$.age", float64(31))
	require.NoError(t, err)
	require.Equal(t, float64(31), doc.(map[string]any)["age"])
}

func TestJsonPathSet_NewField(t *testing.T) {
	var doc any = map[string]any{"name": "Alice"}
	doc, err := jsonPathSet(doc, "$.email", "alice@example.com")
	require.NoError(t, err)
	require.Equal(t, "alice@example.com", doc.(map[string]any)["email"])
}

func TestJsonPathSet_ArrayElement(t *testing.T) {
	var doc any = map[string]any{"arr": []any{"a", "b", "c"}}
	doc, err := jsonPathSet(doc, "$.arr[1]", "X")
	require.NoError(t, err)
	require.Equal(t, "X", doc.(map[string]any)["arr"].([]any)[1])
}

func TestJsonPathSet_Nested(t *testing.T) {
	var doc any = map[string]any{
		"address": map[string]any{"city": "NYC", "zip": "10001"},
	}
	doc, err := jsonPathSet(doc, "$.address.city", "LA")
	require.NoError(t, err)
	require.Equal(t, "LA", doc.(map[string]any)["address"].(map[string]any)["city"])
}

// ─── jsonPathDel ────────────────────────────────────────────────────────────

func TestJsonPathDel_Root(t *testing.T) {
	doc := map[string]any{"name": "Alice"}
	newDoc, count, err := jsonPathDel(doc, "$")
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Nil(t, newDoc)
}

func TestJsonPathDel_Field(t *testing.T) {
	doc := map[string]any{"name": "Alice", "age": float64(30)}
	newDoc, count, err := jsonPathDel(doc, "$.age")
	require.NoError(t, err)
	require.Equal(t, 1, count)
	m := newDoc.(map[string]any)
	require.NotContains(t, m, "age")
	require.Equal(t, "Alice", m["name"])
}

func TestJsonPathDel_Missing(t *testing.T) {
	doc := map[string]any{"name": "Alice"}
	_, count, err := jsonPathDel(doc, "$.missing")
	require.NoError(t, err)
	require.Equal(t, 0, count)
}

func TestJsonPathDel_ArrayElement(t *testing.T) {
	doc := map[string]any{"arr": []any{"a", "b", "c"}}
	newDoc, count, err := jsonPathDel(doc, "$.arr[1]")
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Equal(t, []any{"a", "c"}, newDoc.(map[string]any)["arr"])
}

// ─── jsonPathType ───────────────────────────────────────────────────────────

func TestJsonPathType(t *testing.T) {
	doc := map[string]any{
		"name":   "Alice",
		"age":    float64(30),
		"active": true,
		"tags":   []any{"a"},
		"addr":   map[string]any{"city": "NYC"},
		"extra":  nil,
	}
	tests := []struct {
		path string
		want string
	}{
		{"$", "object"},
		{"$.name", "string"},
		{"$.age", "number"},
		{"$.active", "boolean"},
		{"$.tags", "array"},
		{"$.addr", "object"},
		{"$.extra", "null"},
	}
	for _, tc := range tests {
		got, err := jsonPathType(doc, tc.path)
		require.NoError(t, err, "path=%s", tc.path)
		require.Equal(t, tc.want, got, "path=%s", tc.path)
	}
}

// ─── jsonNumIncrBy ──────────────────────────────────────────────────────────

func TestJsonNumIncrBy(t *testing.T) {
	doc := map[string]any{"counter": float64(10)}
	newDoc, result, err := jsonNumIncrBy(doc, "$.counter", 5)
	require.NoError(t, err)
	require.Equal(t, float64(15), result)
	require.Equal(t, float64(15), newDoc.(map[string]any)["counter"])
}

func TestJsonNumIncrBy_NotANumber(t *testing.T) {
	doc := map[string]any{"name": "Alice"}
	_, _, err := jsonNumIncrBy(doc, "$.name", 1)
	require.ErrorIs(t, err, ErrNotANumber)
}

func TestJsonNumIncrBy_Root(t *testing.T) {
	doc := float64(42)
	newDoc, result, err := jsonNumIncrBy(doc, "$", 8)
	require.NoError(t, err)
	require.Equal(t, float64(50), result)
	require.Equal(t, float64(50), newDoc)
}
