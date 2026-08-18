package worker

import (
	"context"
	"testing"
)

func TestMetaRoundTrip(t *testing.T) {
	m := Meta{Task: "t", MessageID: "id", Attempt: 3, Headers: map[string]string{"k": "v"}}
	ctx := withMeta(context.Background(), m)
	got, ok := MetaFromContext(ctx)
	if !ok || got.Task != "t" || got.MessageID != "id" || got.Attempt != 3 || got.Headers["k"] != "v" {
		t.Fatalf("meta = %+v, %v", got, ok)
	}
	if _, ok := MetaFromContext(context.Background()); ok {
		t.Fatal("meta from bare context")
	}
}
