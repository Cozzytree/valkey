package store

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Errors returned by the JSON path evaluator.
var (
	ErrPathSyntax    = errors.New("ERR invalid JSON path syntax")
	ErrPathNotFound  = errors.New("ERR path not found")
	ErrNotANumber    = errors.New("ERR value at path is not a number")
	ErrIndexOutOfRange = errors.New("ERR array index out of range")
)

// pathToken represents a single step in a JSON path.
type pathToken struct {
	field string // object field name (empty for array index)
	index int    // array index (-1 when field)
}

// parsePath parses a JSONPath-like string into tokens.
// Supported: "$", "$.field", "$.a.b", "$.arr[0]", "$.arr[0].name"
func parsePath(path string) ([]pathToken, error) {
	if path == "" || path[0] != '$' {
		return nil, ErrPathSyntax
	}
	if path == "$" {
		return nil, nil // root
	}
	if len(path) < 2 || path[1] != '.' {
		// Allow $[0] syntax
		if path[1] != '[' {
			return nil, ErrPathSyntax
		}
	}

	var tokens []pathToken
	rest := path[1:] // skip '$'

	for len(rest) > 0 {
		switch rest[0] {
		case '.':
			rest = rest[1:]
			if len(rest) == 0 {
				return nil, ErrPathSyntax
			}
			// Read field name until '.', '[', or end.
			end := strings.IndexAny(rest, ".[")
			if end == -1 {
				end = len(rest)
			}
			if end == 0 {
				return nil, ErrPathSyntax
			}
			tokens = append(tokens, pathToken{field: rest[:end], index: -1})
			rest = rest[end:]

		case '[':
			// Find closing ']'
			close := strings.IndexByte(rest, ']')
			if close == -1 {
				return nil, ErrPathSyntax
			}
			idxStr := rest[1:close]
			idx, err := strconv.Atoi(idxStr)
			if err != nil || idx < 0 {
				return nil, ErrPathSyntax
			}
			tokens = append(tokens, pathToken{index: idx})
			rest = rest[close+1:]

		default:
			return nil, ErrPathSyntax
		}
	}
	return tokens, nil
}

// jsonPathGet navigates to the given path and returns the value.
func jsonPathGet(doc any, path string) (any, error) {
	tokens, err := parsePath(path)
	if err != nil {
		return nil, err
	}
	cur := doc
	for _, tok := range tokens {
		cur, err = navigate(cur, tok)
		if err != nil {
			return nil, err
		}
	}
	return cur, nil
}

// jsonPathSet sets a value at the given path, returning the updated document.
func jsonPathSet(doc any, path string, value any) (any, error) {
	tokens, err := parsePath(path)
	if err != nil {
		return nil, err
	}
	if len(tokens) == 0 {
		return value, nil // replace root
	}
	return setAt(doc, tokens, value)
}

// jsonPathDel deletes the value at the given path, returning the updated doc and count of deletions.
func jsonPathDel(doc any, path string) (any, int, error) {
	tokens, err := parsePath(path)
	if err != nil {
		return doc, 0, err
	}
	if len(tokens) == 0 {
		return nil, 1, nil // delete root
	}
	newDoc, deleted, err := delAt(doc, tokens)
	if err != nil {
		return doc, 0, err
	}
	return newDoc, deleted, nil
}

// jsonPathType returns the JSON type name at the given path.
func jsonPathType(doc any, path string) (string, error) {
	val, err := jsonPathGet(doc, path)
	if err != nil {
		return "", err
	}
	return jsonTypeName(val), nil
}

func jsonTypeName(v any) string {
	switch v.(type) {
	case map[string]any:
		return "object"
	case []any:
		return "array"
	case string:
		return "string"
	case float64:
		return "number"
	case bool:
		return "boolean"
	case nil:
		return "null"
	default:
		return "unknown"
	}
}

// navigate steps into a single token.
func navigate(cur any, tok pathToken) (any, error) {
	if tok.index >= 0 {
		arr, ok := cur.([]any)
		if !ok {
			return nil, ErrPathNotFound
		}
		if tok.index >= len(arr) {
			return nil, ErrIndexOutOfRange
		}
		return arr[tok.index], nil
	}
	obj, ok := cur.(map[string]any)
	if !ok {
		return nil, ErrPathNotFound
	}
	val, exists := obj[tok.field]
	if !exists {
		return nil, ErrPathNotFound
	}
	return val, nil
}

// setAt recursively navigates to the parent and sets the value.
func setAt(doc any, tokens []pathToken, value any) (any, error) {
	if len(tokens) == 1 {
		tok := tokens[0]
		if tok.index >= 0 {
			arr, ok := doc.([]any)
			if !ok {
				return nil, ErrPathNotFound
			}
			if tok.index >= len(arr) {
				return nil, ErrIndexOutOfRange
			}
			arr[tok.index] = value
			return arr, nil
		}
		obj, ok := doc.(map[string]any)
		if !ok {
			return nil, ErrPathNotFound
		}
		obj[tok.field] = value
		return obj, nil
	}

	// Navigate one level deeper.
	tok := tokens[0]
	child, err := navigate(doc, tok)
	if err != nil {
		return nil, err
	}
	updated, err := setAt(child, tokens[1:], value)
	if err != nil {
		return nil, err
	}

	// Write child back (needed for array replacement).
	if tok.index >= 0 {
		doc.([]any)[tok.index] = updated
	} else {
		doc.(map[string]any)[tok.field] = updated
	}
	return doc, nil
}

// delAt recursively navigates to the parent and deletes the target.
func delAt(doc any, tokens []pathToken) (any, int, error) {
	if len(tokens) == 1 {
		tok := tokens[0]
		if tok.index >= 0 {
			arr, ok := doc.([]any)
			if !ok {
				return doc, 0, ErrPathNotFound
			}
			if tok.index >= len(arr) {
				return doc, 0, ErrPathNotFound
			}
			newArr := make([]any, 0, len(arr)-1)
			newArr = append(newArr, arr[:tok.index]...)
			newArr = append(newArr, arr[tok.index+1:]...)
			return newArr, 1, nil
		}
		obj, ok := doc.(map[string]any)
		if !ok {
			return doc, 0, ErrPathNotFound
		}
		if _, exists := obj[tok.field]; !exists {
			return doc, 0, nil
		}
		delete(obj, tok.field)
		return obj, 1, nil
	}

	tok := tokens[0]
	child, err := navigate(doc, tok)
	if err != nil {
		return doc, 0, err
	}
	updated, count, err := delAt(child, tokens[1:])
	if err != nil {
		return doc, 0, err
	}
	if tok.index >= 0 {
		doc.([]any)[tok.index] = updated
	} else {
		doc.(map[string]any)[tok.field] = updated
	}
	return doc, count, nil
}

// jsonNumIncrBy increments a number at the given path by n.
func jsonNumIncrBy(doc any, path string, n float64) (any, float64, error) {
	tokens, err := parsePath(path)
	if err != nil {
		return nil, 0, err
	}
	if len(tokens) == 0 {
		// Root must be a number.
		num, ok := doc.(float64)
		if !ok {
			return nil, 0, ErrNotANumber
		}
		result := num + n
		return result, result, nil
	}

	// Navigate to parent, get target, increment, set back.
	val, err := jsonPathGet(doc, path)
	if err != nil {
		return nil, 0, err
	}
	num, ok := val.(float64)
	if !ok {
		return nil, 0, ErrNotANumber
	}
	result := num + n
	doc, err = jsonPathSet(doc, path, result)
	if err != nil {
		return nil, 0, fmt.Errorf("ERR failed to set incremented value: %w", err)
	}
	return doc, result, nil
}
