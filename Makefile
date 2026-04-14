SHELL := /bin/bash

VENV_DIR ?= venv
PYTHON   := $(VENV_DIR)/bin/python
PIP      := $(VENV_DIR)/bin/pip
UVICORN  := $(VENV_DIR)/bin/uvicorn
PYTEST   := $(VENV_DIR)/bin/pytest
COMPOSE  ?= docker compose
APP_MODULE ?= app.main:app

REDIS_CONTAINER := redis-geo
REDIS_PORT      := 6379

.PHONY: help install redis run test clean docker-build docker-up docker-down docker-logs etl-install etl-run etl-clean

help:
	@printf "Targets:\n"
	@printf "  make install      Cria o virtualenv e instala dependências\n"
	@printf "  make redis        Sobe o Redis em background via Docker\n"
	@printf "  make run          Sobe Redis + FastAPI com hot reload\n"
	@printf "  make test         Roda a suíte de testes\n"
	@printf "  make clean        Remove caches e artefatos gerados\n"
	@printf "  make docker-build Build da imagem Docker\n"
	@printf "  make docker-up    Sobe a stack completa via Docker Compose\n"
	@printf "  make docker-down  Para a stack Docker Compose\n"
	@printf "  make docker-logs  Acompanha logs da API no Docker\n"
	@printf "  make etl-install  Instala dependências do ETL\n"
	@printf "  make etl-run      Executa o pipeline ETL\n"
	@printf "  make etl-clean    Limpa artefatos do ETL\n"

install:
	@echo "Criando virtualenv e instalando dependências..."
	@python3 -m venv $(VENV_DIR)
	@$(PIP) install --upgrade pip -q
	@$(PIP) install -r requirements.txt -q

redis:
	@if docker ps --format '{{.Names}}' | grep -q "^$(REDIS_CONTAINER)$$"; then \
		echo "✅ Redis já está rodando."; \
	elif docker ps -a --format '{{.Names}}' | grep -q "^$(REDIS_CONTAINER)$$"; then \
		echo "▶️  Reiniciando container Redis existente..."; \
		docker start $(REDIS_CONTAINER); \
	else \
		echo "🚀 Subindo Redis via Docker..."; \
		docker run -d --name $(REDIS_CONTAINER) -p $(REDIS_PORT):6379 redis:7-alpine; \
	fi

run: install redis
	@echo "🚀 API disponível em http://127.0.0.1:8000"
	@echo "📖 Docs em http://127.0.0.1:8000/docs"
	@REDIS_URL=redis://localhost:$(REDIS_PORT) \
		$(UVICORN) $(APP_MODULE) --host 0.0.0.0 --port 8000 --reload

test: install
	@echo "Rodando testes..."
	@PYTHONPATH=. $(PYTEST) tests -v

clean:
	@echo "Removendo arquivos gerados..."
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