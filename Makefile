SHELL := /bin/bash

VENV_DIR ?= venv
PYTHON := $(VENV_DIR)/bin/python
PIP := $(VENV_DIR)/bin/pip
UVICORN := $(VENV_DIR)/bin/uvicorn
PYTEST := $(VENV_DIR)/bin/pytest
COMPOSE ?= docker compose
APP_MODULE ?= app.main:app

.PHONY: help install run test clean docker-build docker-up docker-down docker-logs etl-install etl-run etl-clean

help:
	@printf "Targets:\n"
	@printf "  make install      Create the virtual environment and install dependencies\n"
	@printf "  make run          Start the FastAPI app locally with uvicorn\n"
	@printf "  make test         Run the test suite\n"
	@printf "  make clean        Remove generated caches and artifacts\n"
	@printf "  make docker-build Build the Docker image\n"
	@printf "  make docker-up    Start the full stack with Docker Compose\n"
	@printf "  make docker-down  Stop the Docker Compose stack\n"
	@printf "  make docker-logs  Follow API logs from Docker Compose\n"
	@printf "  make etl-install  Delegate dependency install to etl/Makefile\n"
	@printf "  make etl-run      Delegate ETL execution to etl/Makefile\n"
	@printf "  make etl-clean    Delegate cleanup to etl/Makefile\n"

install:
	@echo "Creating virtual environment and installing dependencies..."
	@python3 -m venv $(VENV_DIR)
	@$(PIP) install --upgrade pip
	@$(PIP) install -r requirements.txt

run: install
	@echo "Starting API on http://127.0.0.1:8000 ..."
	@$(UVICORN) $(APP_MODULE) --host 0.0.0.0 --port 8000 --reload

test: install
	@echo "Running tests..."
	@PYTHONPATH=. $(PYTEST) tests -v

clean:
	@echo "Removing generated files..."
	@rm -rf .pytest_cache __pycache__ app/__pycache__ tests/__pycache__ etl/__pycache__
	@rm -f *.html *.png dados_processados/*.png dados_processados/*.parquet

docker-build:
	@$(COMPOSE) build

docker-up:
	@$(COMPOSE) up -d --build

docker-down:
	@$(COMPOSE) down

docker-logs:
	@$(COMPOSE) logs -f api

etl-install:
	@$(MAKE) -C etl install

etl-run:
	@$(MAKE) -C etl run

etl-clean:
	@$(MAKE) -C etl clean