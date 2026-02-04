from fastapi import APIRouter, Response

from ..container import container
from ..schemas import EstatisticasResponse


router = APIRouter(prefix="/api", tags=["estatisticas"])


@router.get(
    "/estatisticas",
    response_model=EstatisticasResponse,
    summary="Estatisticas agregadas",
    description="Retorna estatisticas agregadas (total, media e top 5 operadoras).",
)
def estatisticas(response: Response) -> EstatisticasResponse:
    """
    Retorna estatisticas agregadas.
    """
    data, cache_hit = container.estatisticas_service.get_estatisticas()
    response.headers["X-Cache"] = "HIT" if cache_hit else "MISS"
    return data
