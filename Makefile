TARGET ?= ...
TEST_FILTER ?= .
ifeq ($(TEST_FILTER),.)
	TEST_INTEGRATION_FILTER=TestIntegration
else
	TEST_INTEGRATION_FILTER=$(TEST_FILTER)
endif

SRCS=$(shell find . -name '*.go' -type f | grep -v -e ./protos -e /mocks -e '^./config/config.go')

.EXPORT_ALL_VARIABLES:
GO111MODULE ?= on

ifeq ($(CI),)
define docker_compose_up
	docker-compose -f docker-compose-testing.yml up -d --force-recreate
	sleep 20
endef
define docker_compose_down
	docker-compose -f docker-compose-testing.yml down
endef
else
define docker_compose_up
endef
define docker_compose_down
endef
endif

.PHONY: build
build: codegen fmt build-go
	@echo "--- build"

.PHONY: bootstrap
bootstrap:
	@echo "--- bootstrap"
	scripts/bootstrap.sh

.PHONY: bin
bin:
	@echo "--- bin"
	mkdir -p bin
	go build -o bin ./$(TARGET)

.PHONY: build-go
build-go:
	@echo "--- build-go"
	mkdir -p bin
	go build -o bin ./$(TARGET)

.PHONY: test
test: fmt lint
	@echo "--- test"
	TEST_TYPE=unit go test ./$(TARGET) -run=$(TEST_FILTER)

.PHONY: lint
lint:
	@echo "--- lint"
	go vet -printfuncs=wrapf,statusf,warnf,infof,debugf,failf,equalf,containsf,fprintf,sprintf ./...
	errcheck -ignoretests -ignoregenerated -ignore '.*[rR]ead|[wW]rite|[cC]lose|[dD]ecode|[aA]ctivate|[lL]isten|[rR]emove|[cC]hmod|[mM]arshal|[tT]oken|[rR]egister|[sS]ubscribe' ./internal/server/rpc ./...
	ineffassign ./...

.PHONY: integration
integration:
	@echo "--- integration"
	$(call docker_compose_up)
	TEST_TYPE=integration go test ./$(TARGET) -v -p=1 -parallel=1 -timeout=10m -failfast -run=$(TEST_INTEGRATION_FILTER)
	$(call docker_compose_down)

.PHONY: functional
functional:
	@echo "--- functional"
	$(call docker_compose_up)
	TEST_TYPE=functional go test ./$(TARGET) -v -p=1 -parallel=1 -timeout=45m -failfast -run=$(TEST_INTEGRATION_FILTER)
	$(call docker_compose_down)

.PHONY: codegen
codegen: tag
	@echo "--- codegen"
	./scripts/mockgen.sh

.PHONY: fmt
fmt:
	@echo "--- fmt"
	@goimports -l -w -local github.com/coinbase/chainnode $(SRCS)

.PHONY: server
server:
	go run ./cmd/server

.PHONY: worker
worker:
	go run ./cmd/worker

.PHONY: cron
cron:
	go run ./cmd/cron

.PHONY: localstack
localstack:
	docker-compose -f docker-compose-local.yml down && docker-compose -f docker-compose-local.yml up

.PHONY: tag
tag:
	go run internal/tag_generator/generator.go
	make fmt

.PHONY: docker-build
docker-build:
	@echo "--- docker-build"
	docker build -t coinbase/chainnode .

.PHONY: docker-run
docker-run:
	docker run --rm --network host --name chainnode coinbase/chainnode
