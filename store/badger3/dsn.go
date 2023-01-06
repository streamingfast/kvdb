package badger

import (
	"fmt"
	"net/url"
	"strings"
)

type dsn struct {
	dbPath string
	params url.Values
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

	var params url.Values = nil
	if len(u.Query()) > 0 {
		params = u.Query()
	}

	return &dsn{
		dbPath: strings.Join(paths, "/"),
		params: params,
	}, nil
}
