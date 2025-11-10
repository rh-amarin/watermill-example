.PHONY: test build clean docker-build docker-test-rabbitmq docker-test-googlepubsub test-integration

# Run all tests (unit tests only, skips integration tests)
test:
	@echo "Running tests..."
	go test ./pkg/pubsub/... -v -short

# Run integration tests (requires Docker/Podman)
test-integration:
	@echo "Running integration tests..."
	go test ./pkg/pubsub/... -v -run Integration

# Run all tests including integration tests
test-all: test test-integration
	@echo "All tests completed!"

# Build the applications
build:
	@echo "Building applications..."
	go build -o bin/publisher ./cmd/publisher
	go build -o bin/subscriber ./cmd/subscriber

# Clean build artifacts
clean:
	@echo "Cleaning..."
	rm -rf bin/
	podman compose -f examples/rabbitmq/podman compose.yml down -v
	podman compose -f examples/googlepubsub/podman compose.yml down -v

# Build Docker images
docker-build:
	@echo "Building Docker images..."
	podman build -t watermill-publisher:latest -f Dockerfile.publisher .
	podman build -t watermill-subscriber:latest -f Dockerfile.subscriber .

# Test RabbitMQ example
docker-test-rabbitmq: docker-build
	@echo "Testing RabbitMQ example..."
	cd examples/rabbitmq && podman compose up --build -d
	@echo "Waiting for services to start..."
	sleep 10
	@echo "Checking logs..."
	podman compose -f examples/rabbitmq/podman compose.yml logs --tail=50
	@echo "Stopping services..."
	podman compose -f examples/rabbitmq/podman compose.yml down -v

# Test Google Pub/Sub example
docker-test-googlepubsub: docker-build
	@echo "Testing Google Pub/Sub example..."
	cd examples/googlepubsub && podman compose up --build -d
	@echo "Waiting for services to start..."
	sleep 15
	@echo "Checking logs..."
	podman compose -f examples/googlepubsub/podman compose.yml logs --tail=50
	@echo "Stopping services..."
	podman compose -f examples/googlepubsub/podman compose.yml down -v

# Run all tests including Docker builds
test-all-docker: test docker-build docker-test-rabbitmq docker-test-googlepubsub
	@echo "All tests completed!"

