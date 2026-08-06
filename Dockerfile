FROM python:3.12.11-slim AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /build
COPY requirements.lock ./
RUN python -m pip install \
    --prefix=/install \
    --no-warn-script-location \
    --require-hashes \
    --requirement requirements.lock

COPY pyproject.toml README.md alembic.ini ./
COPY src ./src
COPY migrations ./migrations
COPY scripts/seed-m2-ci.py scripts/verify-m2.py ./scripts/
RUN python -m pip install --prefix=/install --no-deps .

FROM python:3.12.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN groupadd --system marketstream && \
    useradd --system --gid marketstream --create-home marketstream

COPY --from=builder /install /usr/local
COPY --from=builder /build/alembic.ini /app/alembic.ini
COPY --from=builder /build/migrations /app/migrations
COPY --from=builder /build/scripts /app/scripts

USER marketstream
WORKDIR /app
EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=5 \
  CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8000/health/live', timeout=2)"]

CMD ["uvicorn", "marketstream.main:app", "--host", "0.0.0.0", "--port", "8000"]
