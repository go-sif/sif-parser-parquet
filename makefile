version=0.1.0
export GOPROXY=direct

.PHONY: all dependencies clean test cover testall testvall

all:
	@echo "make <cmd>"
	@echo ""
	@echo "commands:"
	@echo "  dependencies  - install dependencies"
	@echo "  build         - build the source code"
	@echo "  docs          - build the documentation"
	@echo "  clean         - clean the source directory"
	@echo "  lint          - lint the source code"
	@echo "  fmt           - format the source code"
	@echo "  test          - test the source code"

dependencies:
	@go get -u golang.org/x/tools
	@go get -u golang.org/x/lint/golint
	@go get -u golang.org/x/tools/cmd/godoc
	@go get -u github.com/unchartedsoftware/witch
	@go get -u github.com/go-sif/sif
	@go get -d -v ./...

fmt:
	@go fmt ./...

clean:
	@rm -rf ./bin

lint:
	@echo "Running go vet"
	@go vet ./...
	@echo "Running golint"
	@go list ./... | grep -v /vendor/ | xargs -L1 golint --set_exit_status

test: build
	@echo "Running tests..."
	@go test -short -count=1 ./...

testall: build
	@echo "Running tests..."
	@go test -timeout 30m -count=1 ./...

testv: build
	@echo "Running tests..."
	@go test -short -v ./...

testvall: build
	@echo "Running tests..."
	@go test -timeout 30m -v -count=1 ./...

cover: build
	@echo "Running tests with coverage..."
	@go test -coverprofile=cover.out -coverpkg=./... ./...
	@go tool cover -html=cover.out -o cover.html

generate:
	@echo "Generating protobuf code..."
	@go generate ./...
	@echo "Finished generating protobuf code."

build: generate lint
	@echo "Building sif-parser-parquet..."
	@go build ./...
	@go mod tidy
	@echo "Finished building sif-parser-parquet."

serve-docs:
	@echo "Serving docs on http://localhost:6060"
	@witch --cmd="godoc -http=localhost:6060" --watch="**/*.go" --ignore="vendor,.git,**/*.pb.go" --no-spinner

watch:
	@witch --cmd="make build" --watch="**/*.go" --ignore="vendor,.git,**/*.pb.go" --no-spinner
