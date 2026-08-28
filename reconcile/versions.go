package reconcile

import "context"

type Version uint64

type VersionSource interface {
	Latest(ctx context.Context, id ID) (Version, error)
}
