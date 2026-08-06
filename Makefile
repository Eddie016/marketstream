.PHONY: check down format install-dev lint lock migrate proto smoke test topic typecheck up verify-m2

check: lint typecheck test

format:
	python -m ruff format .
	python -m ruff check --fix .

install-dev:
	python -m pip install --require-hashes --requirement requirements-dev.lock
	python -m pip install --no-build-isolation --no-deps --editable .

lock:
	python -m piptools compile --allow-unsafe --strip-extras --generate-hashes --output-file=requirements.lock pyproject.toml
	python -m piptools compile --allow-unsafe --strip-extras --extra=dev --generate-hashes --output-file=requirements-dev.lock pyproject.toml

migrate:
	python -m alembic upgrade head

proto:
	python -m grpc_tools.protoc -I proto --python_out=src/marketstream/proto --pyi_out=src/marketstream/proto proto/market_event.proto

smoke:
	./scripts/smoke.sh

topic:
	docker compose run --rm kafka-init

lint:
	python -m ruff format --check .
	python -m ruff check .

typecheck:
	python -m mypy

test:
	python -m pytest

up:
	docker compose up --build --detach
	docker compose up --wait api

verify-m2:
	docker compose run --rm api python /app/scripts/verify-m2.py

down:
	docker compose down
