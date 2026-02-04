from datetime import date
from typing import Optional

from pydantic import BaseModel


class Operadora(BaseModel):
    """
    Representa uma operadora para listagem.
    """

    cnpj: str
    razao_social: Optional[str] = None
    modalidade: Optional[str] = None
    uf: Optional[str] = None


class OperadoraDetalhe(BaseModel):
    """
    Detalhes completos de uma operadora.
    """

    cnpj: str
    registro_operadora: Optional[str] = None
    razao_social: Optional[str] = None
    nome_fantasia: Optional[str] = None
    modalidade: Optional[str] = None
    logradouro: Optional[str] = None
    numero: Optional[str] = None
    complemento: Optional[str] = None
    bairro: Optional[str] = None
    cidade: Optional[str] = None
    uf: Optional[str] = None
    cep: Optional[str] = None
    ddd: Optional[str] = None
    telefone: Optional[str] = None
    fax: Optional[str] = None
    endereco_eletronico: Optional[str] = None
    representante: Optional[str] = None
    cargo_representante: Optional[str] = None
    regiao_de_comercializacao: Optional[str] = None
    data_registro_ans: Optional[date] = None


class DespesaHistorico(BaseModel):
    """
    Registro de despesas por trimestre.
    """

    ano: int
    trimestre: int
    valor_despesas: float


class PaginationMeta(BaseModel):
    """
    Metadados de paginação.
    """

    page: int
    limit: int
    total: int
    total_pages: int


class OperadorasResponse(BaseModel):
    """
    Resposta paginada de operadoras.
    """

    data: list[Operadora]
    meta: PaginationMeta


class TopOperadora(BaseModel):
    """
    Operadora no ranking de despesas.
    """

    cnpj: str
    razao_social: Optional[str] = None
    total_despesas: float


class EstatisticasResponse(BaseModel):
    """
    Estatisticas agregadas para o endpoint /api/estatisticas.
    """

    total_despesas: float
    media_despesas: float
    top_operadoras: list[TopOperadora]
