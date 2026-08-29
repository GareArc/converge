GO ?= go
MODULES := . adapters/redis adapters/otel bridges/kratos

.PHONY: test vet fmt-check depcheck cover check

test:
	@for m in $(MODULES); do (cd $$m && $(GO) test -race ./...) || exit 1; done

vet:
	@for m in $(MODULES); do (cd $$m && $(GO) vet ./...) || exit 1; done

fmt-check:
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

depcheck:
	./scripts/depcheck.sh

cover:
	$(GO) test -race -coverprofile=coverage.out ./...

check: fmt-check vet depcheck test
