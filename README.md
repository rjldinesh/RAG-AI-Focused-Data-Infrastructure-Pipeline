# 🚀 Makefile Commands for Docker Project

This project uses a `Makefile` to simplify Docker operations. Make sure you have Docker and Docker Compose installed.

## 📦 Available Commands

| Command         | Description                                                                 |
|-----------------|-----------------------------------------------------------------------------|
| `make` or `make all`  | Runs the full reset process: clean, prune, build, and start containers. |
| `make clean`    | Stops and removes all containers, images, and associated volumes.          |
| `make prune`    | Removes all unused Docker data including volumes and networks.             |
| `make build`    | Rebuilds Docker images defined in `docker-compose.yml`.                    |
| `make up`       | Starts the Docker containers in detached mode (`-d`).                      |
| `make reset`    | Equivalent to running: `clean` → `prune` → `build` → `up`.                 |

## ✅ Usage

From the root of the project (where the `Makefile` is located), open your terminal and run:

```bash
make             # full reset: clean + prune + build + up
make clean       # stop and remove containers, images, and volumes
make prune       # prune unused Docker resources
make build       # rebuild all Docker images
make up          # start containers in detached mode
