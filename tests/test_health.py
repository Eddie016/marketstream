from collections.abc import Iterator

from fastapi.testclient import TestClient

from marketstream.config import Settings, get_settings
from marketstream.database import database_health
from marketstream.main import app


def override_settings() -> Settings:
    return Settings(environment="test")


def database_is_ready() -> Iterator[bool]:
    yield True


def database_is_unavailable() -> Iterator[bool]:
    yield False


def client_with_database(health_dependency: object) -> TestClient:
    app.dependency_overrides[get_settings] = override_settings
    app.dependency_overrides[database_health] = health_dependency  # type: ignore[assignment]
    return TestClient(app)


def test_liveness_does_not_require_infrastructure() -> None:
    with client_with_database(database_is_unavailable) as client:
        response = client.get("/health/live")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "service": "marketstream-api",
        "version": "0.1.0",
        "environment": "test",
    }
    app.dependency_overrides.clear()


def test_readiness_succeeds_when_database_is_ready() -> None:
    with client_with_database(database_is_ready) as client:
        response = client.get("/health/ready")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    app.dependency_overrides.clear()


def test_readiness_returns_503_when_database_is_unavailable() -> None:
    with client_with_database(database_is_unavailable) as client:
        response = client.get("/health/ready")

    assert response.status_code == 503
    assert response.json()["status"] == "unavailable"
    app.dependency_overrides.clear()
