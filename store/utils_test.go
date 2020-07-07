package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRemoveDSNOptions(t *testing.T) {
	tests := []struct {
		name        string
		in          string
		keys        []string
		expected    string
		expectedErr error
	}{
		{"no options, no keys", "kv://value", []string{}, "kv://value", nil},
		{"no options, multiple keys", "kv://value", []string{"a", "b", "c"}, "kv://value", nil},

		{"one option, no matching key", "kv://value?e=5", []string{"a", "b", "c"}, "kv://value?e=5", nil},
		{"one option, multiple matching key", "kv://value?c=3", []string{"c", "c", "c"}, "kv://value", nil},

		{"multiple options, no matching key", "kv://value?a=1&b=2&e=5", []string{"c", "f", "g"}, "kv://value?a=1&b=2&e=5", nil},
		{"multiple options, multiple matching key", "kv://value?a=1&b=2&e=5", []string{"b", "e", "e"}, "kv://value?a=1", nil},

		{"multiple options with duplicates, multiple matching key", "kv://value?a=1&b=2&e=5&a=11&b=22&e=55", []string{"b", "e", "e"}, "kv://value?a=1&a=11", nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual, err := RemoveDSNOptions(test.in, test.keys...)
			if test.expectedErr == nil {
				require.NoError(t, err)
				assert.Equal(t, test.expected, actual)
			} else {
				assert.Equal(t, test.expectedErr, err)
			}
		})
	}
}
