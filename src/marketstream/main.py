from typing import Annotated

from fastapi import Depends, FastAPI, Response, status
from pydantic import BaseModel

from marketstream import __version__
from marketstream.config import Settings, get_settings
from marketstream.database import database_health


class HealthResponse(BaseModel):
    status: str
    service: str = "marketstream-api"
    version: str = __version__
    environment: str


app = FastAPI(
    title="MarketStream API",
    summary="Personal equity research and paper-portfolio platform",
    version=__version__,
)


@app.get("/health/live", tags=["health"], response_model=HealthResponse)
def liveness(settings: Annotated[Settings, Depends(get_settings)]) -> HealthResponse:
    """Report whether the API process can serve requests."""

    return HealthResponse(status="ok", environment=settings.environment)


@app.get("/health/ready", tags=["health"], response_model=HealthResponse)
def readiness(
    response: Response,
    settings: Annotated[Settings, Depends(get_settings)],
    database_ready: Annotated[bool, Depends(database_health)],
) -> HealthResponse:
    """Report whether required dependencies are available."""

    if not database_ready:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return HealthResponse(status="unavailable", environment=settings.environment)
    return HealthResponse(status="ok", environment=settings.environment)
