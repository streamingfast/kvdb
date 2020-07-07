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

	RemoveDSNOptionsFromURL(dsnURL, keys...)
	return dsnURL.String(), nil
}

// RemoveDSNOptionsFromURL takes a DSN URL and removes from it any query options
// matching one of the `key` received in parameter.
//
// For example, transforms `kv://path?option1=value&option2=test&option3=any` to
// `kv://path?option2=test` when passing `option1` and `option3` as the keys.
//
// *Note** This transforms the URL receive in place!
func RemoveDSNOptionsFromURL(dsnURL *url.URL, keys ...string) {
	query := dsnURL.Query()
	if len(query) <= 0 {
		return
	}

	for _, key := range keys {
		query.Del(key)
	}

	dsnURL.RawQuery = query.Encode()
}
