all: prepare clean get test-race build
	@echo "*** Done!"


prepare:
	@echo "*** Create bin & pkg dirs, if not exists..."
	@mkdir -p bin
	@mkdir -p pkg

get:
	@echo "*** Resolve dependencies..."
	@go get -v github.com/stretchr/testify

test:
	@echo "*** Run tests..."
	go test -v ./src/aof/...

test-race:
	@echo "*** Run tests with race condition..."
	@go test --race -v ./src/aof/...

test-cover:
	@go test -covermode=count -coverprofile=/tmp/coverage_aof.out ./src/aof/...

	@rm -f /tmp/aofcompactor_coverage.out
	@echo "mode: count" > /tmp/aofcompactor_coverage.out
	@cat /tmp/coverage_aof.out | tail -n +2 >> /tmp/aofcompactor_coverage.out
	@rm /tmp/coverage_aof.out

	@go tool cover -html=/tmp/aofcompactor_coverage.out

build:
	@echo "*** Build project..."
	@go build -v -o bin/aofcompactor src/main.go

build-race:
	@echo "*** Build project with race condition..."
	@go build --race -v -o bin/aofcompactor-race src/main.go

clean-bin:
	@echo "*** Clean up bin/ directory..."
	@rm -rf bin/*

clean-pkg:
	@echo "*** Clean up pkg/ directory..."
	@rm -rf pkg/*

clean: clean-bin clean-pkg
