GO ?= go
MIGRATIONS_DIR ?= internal/infra/migrations

DB_HOST ?= localhost
DB_PORT ?= 5432
DB_USER ?= postgres
DB_PASSWORD ?= postgres
DB_NAME ?= challenge
DB_SSL_MODE ?= disable

DB_URL ?= postgres://$(DB_USER):$(DB_PASSWORD)@$(DB_HOST):$(DB_PORT)/$(DB_NAME)?sslmode=$(DB_SSL_MODE)

help:
	@echo "Usage: make <alvo>"
	@echo ""
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: test
test: ## Run tests
	$(GO) test ./... -count=1 -v ./... -race

migrate-up: ## Run migrations up
	goose -dir $(MIGRATIONS_DIR) postgres "$(DB_URL)" up

migrate-down: ## Run migrations down
	goose -dir $(MIGRATIONS_DIR) postgres "$(DB_URL)" down

migrate-reset: ## Reset migrations
	goose -dir $(MIGRATIONS_DIR) postgres "$(DB_URL)" reset

migrate-status: ## Show migration status
	goose -dir $(MIGRATIONS_DIR) postgres "$(DB_URL)" status

migrate-create: ## Create a new migration
	@read -p "Enter migration name: " name; \
	goose -dir $(MIGRATIONS_DIR) postgres "$(DB_URL)" create $$name