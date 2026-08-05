.PHONY: check down format install-dev lint lock smoke test typecheck up

check: lint typecheck test

format:
	python -m ruff format .
	python -m ruff check --fix .

install-dev:
	python -m pip install --require-hashes --requirement requirements-dev.lock
	python -m pip install --no-deps --editable .

lock:
	python -m piptools compile --strip-extras --generate-hashes --output-file=requirements.lock pyproject.toml
	python -m piptools compile --strip-extras --extra=dev --generate-hashes --output-file=requirements-dev.lock pyproject.toml

smoke:
	./scripts/smoke.sh

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
