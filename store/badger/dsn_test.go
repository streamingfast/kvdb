package badger

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_parseDNS(t *testing.T) {
	tests := []struct {
		name        string
		dns         string
		expectError bool
		expectDSN   *dsn
	}{
		{
			name:        "local dir",
			dns:         "badger://badger-db.db",
			expectError: false,
			expectDSN: &dsn{
				dbPath: "badger-db.db",
			},
		},
		{
			name:        "absolute path",
			dns:         "badger:///Users/john/.dfusebox/kvdb/badger-db.db",
			expectError: false,
			expectDSN: &dsn{
				dbPath: "/Users/john/.dfusebox/kvdb/badger-db.db",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dsn, err := newDSN(test.dns)
			if test.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectDSN, dsn)
			}

		})
	}
}
