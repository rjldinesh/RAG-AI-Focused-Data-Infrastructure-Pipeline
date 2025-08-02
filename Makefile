# Makefile

.PHONY: all clean prune build up reset

# Default target: run full pipeline
all: reset

# Stop and remove containers, images, and volumes
clean:
	@echo "Stopping and removing containers, images, and volumes..."
	docker-compose down --volumes --rmi all

# Prune unused Docker resources
prune:
	@echo "Pruning unused Docker resources (including volumes)..."
	docker system prune -a --volumes -f

# Rebuild Docker images
build:
	@echo "Rebuilding Docker images..."
	docker-compose build

# Start containers in detached mode
up:
	@echo "Starting containers in detached mode..."
	docker-compose up -d

# Run all tasks sequentially
reset: clean prune build up
	@echo "All tasks completed."
