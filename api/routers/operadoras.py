import math

from fastapi import APIRouter, HTTPException, Path, Query

from ..container import container
from ..schemas import DespesaHistorico, OperadoraDetalhe, OperadorasResponse
from ..utils import normalize_cnpj


router = APIRouter(prefix="/api", tags=["operadoras"])


@router.get(
    "/operadoras",
    response_model=OperadorasResponse,
    summary="Lista operadoras",
    description="Lista operadoras com paginacao.",
)
def listar_operadoras(
    page: int = Query(1, ge=1, description="Pagina atual (1..N)."),
    limit: int = Query(10, ge=1, le=100, description="Itens por pagina."),
) -> OperadorasResponse:
    """
    Lista operadoras.
    """
    data, total = container.operadoras_service.list_operadoras(page, limit)
    total_pages = math.ceil(total / limit) if limit else 0
    return {
        "data": data,
        "meta": {
            "page": page,
            "limit": limit,
            "total": total,
            "total_pages": total_pages,
        },
    }


@router.get(
    "/operadoras/{cnpj}",
    response_model=OperadoraDetalhe,
    summary="Detalhe da operadora",
    description="Retorna os detalhes de uma operadora pelo CNPJ.",
)
def detalhe_operadora(
    cnpj: str = Path(..., description="CNPJ da operadora."),
) -> OperadoraDetalhe:
    """
    Retorna detalhes da operadora.
    """
    row = container.operadoras_service.get_operadora(cnpj)
    if not row:
        raise HTTPException(status_code=404, detail="Operadora nao encontrada.")
    return row


@router.get(
    "/operadoras/{cnpj}/despesas",
    response_model=list[DespesaHistorico],
    summary="Historico de despesas",
    description="Retorna o historico de despesas da operadora.",
)
def despesas_operadora(
    cnpj: str = Path(..., description="CNPJ da operadora."),
) -> list[DespesaHistorico]:
    """
    Retorna historico de despesas.
    """
    cnpj_digits = normalize_cnpj(cnpj)
    if not cnpj_digits:
        raise HTTPException(status_code=400, detail="CNPJ invalido.")
    return container.operadoras_service.get_despesas(cnpj_digits)
