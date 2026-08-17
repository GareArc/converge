package reconcile

import "context"

type IDSource struct {
	page   func(ctx context.Context, cursor string) ([]ID, string, error)
	paged  bool
	single bool
}

func (s IDSource) IsZero() bool { return s.page == nil }

func SingleID() IDSource {
	return IDSource{
		single: true,
		page: func(context.Context, string) ([]ID, string, error) {
			return []ID{""}, "", nil
		},
	}
}

func IDs(fn func(ctx context.Context) ([]ID, error)) IDSource {
	if fn == nil {
		return IDSource{}
	}
	return IDSource{page: func(ctx context.Context, _ string) ([]ID, string, error) {
		ids, err := fn(ctx)
		return ids, "", err
	}}
}

func StringIDs(fn func(ctx context.Context) ([]string, error)) IDSource {
	if fn == nil {
		return IDSource{}
	}
	return IDs(func(ctx context.Context) ([]ID, error) {
		raw, err := fn(ctx)
		if err != nil {
			return nil, err
		}
		return ToIDs(raw...), nil
	})
}

func IDsByPage(fn func(ctx context.Context, cursor string) ([]ID, string, error)) IDSource {
	if fn == nil {
		return IDSource{}
	}
	return IDSource{paged: true, page: fn}
}
