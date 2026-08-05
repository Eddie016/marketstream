.PHONY: check down format install-dev lint lock proto smoke test topic typecheck up

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

proto:
	python -m grpc_tools.protoc -I proto --python_out=src/marketstream/proto --pyi_out=src/marketstream/proto proto/market_event.proto

smoke:
	./scripts/smoke.sh

topic:
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic market-prices-v1 --partitions 4 --replication-factor 1

lint:
	python -m ruff format --check .
	python -m ruff check .

typecheck:
	python -m mypy

test:
	python -m pytest

up:
	docker compose up --build --wait

down:
	docker compose down
