.PHONY: ci-tools
ci-tools:
	@echo "::> Installing tools..."
	CGO_ENABLED=0 go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.56.2
	@echo "::> Finished!"

.PHONY: lint
lint:
	@echo "::> Linting..."
	@CGO_ENABLED=0 $(GOPATH)/bin/golangci-lint run ./... --timeout 20m
	@echo "::> Finished!"

.PHONY: test
test:
	@echo "::> Running tests..."
	@go test -race -v ./... -cover -short -coverprofile=${COVER_FILE} $(go list ./...)
	@echo "::> Finished!"

