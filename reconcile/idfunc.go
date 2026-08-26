package reconcile

import (
	"encoding/json"
	"errors"
	"fmt"
)

type IDFunc func(payload []byte) (ID, error)

func RawID() IDFunc {
	return func(payload []byte) (ID, error) {
		if len(payload) == 0 {
			return "", errors.New("reconcile: empty payload")
		}
		return ID(payload), nil
	}
}

func IDFromJSONField(field string) IDFunc {
	return func(payload []byte) (ID, error) {
		vals, err := jsonStringFields(payload, field)
		if err != nil {
			return "", err
		}
		return ID(vals[0]), nil
	}
}

func jsonStringFields(payload []byte, fields ...string) ([]string, error) {
	if len(fields) == 0 {
		return nil, errors.New("reconcile: no fields specified")
	}
	var obj map[string]any
	if err := json.Unmarshal(payload, &obj); err != nil {
		return nil, fmt.Errorf("reconcile: decode hint: %w", err)
	}
	out := make([]string, len(fields))
	for i, f := range fields {
		v, ok := obj[f]
		if !ok {
			return nil, fmt.Errorf("reconcile: hint has no field %q", f)
		}
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("reconcile: hint field %q is not a string", f)
		}
		if s == "" {
			return nil, fmt.Errorf("reconcile: hint field %q is empty", f)
		}
		out[i] = s
	}
	return out, nil
}
