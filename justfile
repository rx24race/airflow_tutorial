# List available commands
default:
    just --list

# Docker compose up
up:
    docker compose up -d --build

# Docker compose down
down:
    docker compose down -v

# Restart docker containers
restart:
  just down 
  just up

# Run tests inside the Airflow container image
test:
    docker compose run --rm --build airflow-tutorial pytest
