package badger

import (
	"fmt"
	"net/url"
	"strings"
)

type dsn struct {
	dbPath string
}

func newDSN(dsnString string) (*dsn, error) {
	u, err := url.Parse(dsnString)
	if err != nil {
		return nil, fmt.Errorf("cannot parse badger dsn %q: %w", dsnString, err)
	}

	paths := []string{}
	if u.Hostname() != "" {
		paths = append(paths, u.Hostname())
	}

	if u.Path != "" {
		paths = append(paths, u.Path)
	}

	return &dsn{
		dbPath: strings.Join(paths, "/"),
	}, nil
}
