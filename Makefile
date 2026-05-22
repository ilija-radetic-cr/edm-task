.PHONY: build up down test logs clean status health

# Build and run
build:
	mvn clean package -DskipTests

up: build
	docker compose up --build -d

down:
	docker compose down -v

restart: down up

# Status and health
status:
	docker compose ps

health:
	@echo "=== Order API ===" && curl -s http://localhost:8080/health && echo
	@echo "=== Inventory ===" && curl -s http://localhost:8081/health && echo

ready:
	@echo "=== Order API ===" && curl -s http://localhost:8080/ready && echo
	@echo "=== Inventory ===" && curl -s http://localhost:8081/ready && echo

# Testing
test:
	@echo "Creating order..." && \
	curl -s -X POST http://localhost:8080/orders \
		-H "Content-Type: application/json" \
		-d '{"orderId":"ORD-TEST-1","itemId":"item-1","quantity":10}' | jq .

test-validation:
	@echo "Testing validation (should fail)..." && \
	curl -s -X POST http://localhost:8080/orders \
		-H "Content-Type: application/json" \
		-d '{"orderId":"","itemId":"","quantity":-1}' | jq .

test-idempotency:
	@echo "First request..." && \
	curl -s -X POST http://localhost:8080/orders \
		-H "Content-Type: application/json" \
		-d '{"orderId":"ORD-IDEMP-1","itemId":"item-1","quantity":5}' | jq .
	@sleep 2
	@echo "Duplicate request (check logs for 'Skipping duplicate')..." && \
	curl -s -X POST http://localhost:8080/orders \
		-H "Content-Type: application/json" \
		-d '{"orderId":"ORD-IDEMP-1","itemId":"item-1","quantity":5}' | jq .

test-rejection:
	@echo "Exhausting item-2 stock (50 units)..."
	@for i in 1 2 3 4 5 6; do \
		echo "Order $$i:"; \
		curl -s -X POST http://localhost:8080/orders \
			-H "Content-Type: application/json" \
			-d "{\"orderId\":\"ORD-REJ-$$i\",\"itemId\":\"item-2\",\"quantity\":10}" | jq -r '.status // .error'; \
	done
	@echo "Check logs: last order should be REJECTED"

test-all: test test-validation test-idempotency test-rejection

# Logs
logs:
	docker compose logs -f

logs-order:
	docker compose logs -f order-api

logs-inventory:
	docker compose logs -f inventory-service

# Cleanup
clean:
	mvn clean
	docker compose down -v --rmi local
