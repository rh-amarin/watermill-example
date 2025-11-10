.PHONY: test build clean docker-build docker-test-rabbitmq docker-test-googlepubsub

# Run all tests
test:
	@echo "Running tests..."
	go test ./pkg/pubsub/... -v

# Build the application
build:
	@echo "Building application..."
	go build -o bin/app ./cmd/app

# Clean build artifacts
clean:
	@echo "Cleaning..."
	rm -rf bin/
	docker-compose -f examples/rabbitmq/docker-compose.yml down -v
	docker-compose -f examples/googlepubsub/docker-compose.yml down -v

# Build Docker image
docker-build:
	@echo "Building Docker image..."
	docker build -t watermill-app:latest .

# Test RabbitMQ example
docker-test-rabbitmq: docker-build
	@echo "Testing RabbitMQ example..."
	cd examples/rabbitmq && docker-compose up --build -d
	@echo "Waiting for services to start..."
	sleep 10
	@echo "Checking logs..."
	docker-compose -f examples/rabbitmq/docker-compose.yml logs --tail=50
	@echo "Stopping services..."
	docker-compose -f examples/rabbitmq/docker-compose.yml down -v

# Test Google Pub/Sub example
docker-test-googlepubsub: docker-build
	@echo "Testing Google Pub/Sub example..."
	cd examples/googlepubsub && docker-compose up --build -d
	@echo "Waiting for services to start..."
	sleep 15
	@echo "Checking logs..."
	docker-compose -f examples/googlepubsub/docker-compose.yml logs --tail=50
	@echo "Stopping services..."
	docker-compose -f examples/googlepubsub/docker-compose.yml down -v

# Run all tests including Docker builds
test-all: test docker-build docker-test-rabbitmq docker-test-googlepubsub
	@echo "All tests completed!"

