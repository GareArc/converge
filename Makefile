GO ?= go

.PHONY: test vet fmt-check depcheck cover check

test:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...

fmt-check:
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

depcheck:
	./scripts/depcheck.sh

cover:
	$(GO) test -race -coverprofile=coverage.out ./...

check: fmt-check vet depcheck test
