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

COPY pyproject.toml README.md ./
COPY src ./src
RUN python -m pip install --prefix=/install --no-deps .

FROM python:3.12.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN groupadd --system marketstream && \
    useradd --system --gid marketstream --create-home marketstream

COPY --from=builder /install /usr/local

USER marketstream
WORKDIR /app
EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=5 \
  CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8000/health/live', timeout=2)"]

CMD ["uvicorn", "marketstream.main:app", "--host", "0.0.0.0", "--port", "8000"]
