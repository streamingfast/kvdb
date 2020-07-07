package store

import "net/url"

// RemoveDSNOptions takes a DSN url string and removes from it any query options
// matching one of the `key` received in parameter.
//
// For example, transforms `kv://path?option1=value&option2=test&option3=any` to
// `kv://path?option2=test` when passing `option1` and `option3` as the keys.
func RemoveDSNOptions(dsn string, keys ...string) (string, error) {
	dsnURL, err := url.Parse(dsn)
	if err != nil {
		return "", err
	}

	removeDSNOptionsFromURL(dsnURL, keys)
	return dsnURL.String(), nil
}

// RemoveDSNOptionsFromURL takes a DSN URL and removes from it any query options
// matching one of the `key` received in parameter.
//
// For example, transforms `kv://path?option1=value&option2=test&option3=any` to
// `kv://path?option2=test` when passing `option1` and `option3` as the keys.
func RemoveDSNOptionsFromURL(dsnURL *url.URL, keys ...string) *url.URL {
	copy := &url.URL{
		Scheme:     dsnURL.Scheme,
		Opaque:     dsnURL.Opaque,
		User:       dsnURL.User,
		Host:       dsnURL.Host,
		Path:       dsnURL.Path,
		RawPath:    dsnURL.RawPath,
		ForceQuery: dsnURL.ForceQuery,
		RawQuery:   dsnURL.RawQuery,
		Fragment:   dsnURL.Fragment,
	}

	removeDSNOptionsFromURL(copy, keys)
	return copy
}

func removeDSNOptionsFromURL(dsnURL *url.URL, keys []string) {
	query := dsnURL.Query()
	if len(query) <= 0 {
		return
	}

	for _, key := range keys {
		query.Del(key)
	}

	dsnURL.RawQuery = query.Encode()
}
