from typing import Optional

from ..repositories.operadoras import OperadorasRepository


class OperadorasService:
    """
    Regras de negocio para operadoras.
    """

    def __init__(self, repo: OperadorasRepository) -> None:
        self._repo = repo

    def list_operadoras(self, page: int, limit: int) -> tuple[list[dict], int]:
        """
        Lista operadoras com paginacao.

        :param page: Pagina atual.
        :param limit: Itens por pagina.
        :return: Tupla (lista, total).
        """
        return self._repo.list_operadoras(page, limit)

    def get_operadora(self, cnpj: str) -> Optional[dict]:
        """
        Retorna detalhes da operadora.

        :param cnpj: CNPJ da operadora.
        :return: Dicionario com detalhes ou None.
        """
        return self._repo.get_operadora(cnpj)

    def get_despesas(self, cnpj: str) -> list[dict]:
        """
        Retorna historico de despesas.

        :param cnpj: CNPJ da operadora.
        :return: Lista de despesas por trimestre.
        """
        return self._repo.get_despesas(cnpj)
