package convredis

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const scanPageSize = 200

func NewKV(rdb *redis.Client) *KV {
	return &KV{rdb: rdb}
}

type KV struct {
	rdb *redis.Client
}

func (k *KV) Get(ctx context.Context, key string) ([]byte, bool, error) {
	val, err := k.rdb.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return val, true, nil
}

func (k *KV) SetCAS(ctx context.Context, key string, old, new []byte) (bool, error) {
	present := "1"
	if old == nil {
		present = "0"
	}
	res, err := casScript.Run(ctx, k.rdb, []string{key}, present, string(old), string(new)).Int()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}

func (k *KV) Set(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	return k.rdb.Set(ctx, key, val, ttl).Err()
}

func (k *KV) Delete(ctx context.Context, key string) error {
	return k.rdb.Del(ctx, key).Err()
}

func (k *KV) Scan(ctx context.Context, prefix, cursor string) ([]string, string, error) {
	var cur uint64
	if cursor != "" {
		parsed, err := strconv.ParseUint(cursor, 10, 64)
		if err != nil {
			return nil, "", err
		}
		cur = parsed
	}
	keys, next, err := k.rdb.Scan(ctx, cur, prefix+"*", scanPageSize).Result()
	if err != nil {
		return nil, "", err
	}
	if next == 0 {
		return keys, "", nil
	}
	return keys, strconv.FormatUint(next, 10), nil
}
