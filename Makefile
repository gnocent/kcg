# Targets to run on Linux (WSL included): make check before every commit.
GO      ?= go
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)

.PHONY: check vet test build recette demo dist

check: vet test build

vet:
	$(GO) vet ./...
	$(GO) vet -tags bench ./...

test:
	$(GO) test -count=1 ./...

build:
	$(GO) build -trimpath -ldflags "$(LDFLAGS)" -o bin/kcg .

# Acceptance tests on the kafkabeat Docker bench (Kafka 3.9 on localhost:19092,
# 4.4 on localhost:19094), started beforehand:
#   docker compose -f bench/compose.yaml up -d --wait kafka39 kafka44
recette:
	$(GO) test -tags bench -count=1 -timeout 15m -v ./...

# Records demokcg.gif, the README demo, on the bench's Kafka 4.4 (needs vhs, ttyd, ffmpeg).
demo: build
	tools/demo/make-demo.sh bin/kcg demokcg.gif

# Release archives in dist/: static binary and documentation for each target;
# tar.gz, zip for Windows, and their SHA256SUMS.
DIST_TARGETS := linux/amd64 darwin/arm64 windows/amd64
DIST_FILES   := LICENSE README.md README_fr.md CHANGELOG.md CHANGELOG_fr.md

dist:
	rm -rf dist && mkdir -p dist
	set -e; for t in $(DIST_TARGETS); do \
	  os=$${t%/*}; arch=$${t#*/}; ext=; if [ $$os = windows ]; then ext=.exe; fi; \
	  stage=dist/stage/$$os-$$arch; d=$$stage/kcg-$(VERSION); mkdir -p $$d; \
	  CGO_ENABLED=0 GOOS=$$os GOARCH=$$arch $(GO) build -trimpath \
	    -ldflags "$(LDFLAGS)" -o $$d/kcg$$ext .; \
	  cp $(DIST_FILES) $$d/; \
	  if [ $$os = windows ]; then \
	    (cd $$stage && python3 -m zipfile -c ../../kcg-$(VERSION)-$$os-$$arch.zip kcg-$(VERSION)); \
	  else \
	    tar --owner=0 --group=0 --numeric-owner -C $$stage -czf dist/kcg-$(VERSION)-$$os-$$arch.tar.gz kcg-$(VERSION); \
	  fi; \
	done
	rm -rf dist/stage
	cd dist && sha256sum kcg-* > SHA256SUMS && cat SHA256SUMS
