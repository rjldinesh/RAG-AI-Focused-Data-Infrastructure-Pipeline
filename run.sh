#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

echo "Stopping and removing containers, images, and volumes..."
docker-compose down --volumes --rmi all

echo "Pruning unused Docker resources (including volumes)..."
docker system prune -a --volumes -f

echo "Rebuilding Docker images..."
docker-compose build

echo "Starting containers in detached mode..."
docker-compose up -d

echo "All tasks completed."
