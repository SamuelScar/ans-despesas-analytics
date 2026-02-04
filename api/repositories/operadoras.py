from typing import Any, Optional

from ..db import Database
from ..utils import normalize_cnpj


class OperadorasRepository:
    """
    Acesso a dados de operadoras e despesas.
    """

    def __init__(self, db: Database) -> None:
        self._db = db

    def _count_operadoras(self) -> int:
        """
        Retorna total de operadoras.

        :return: Total de operadoras.
        """
        sql = "SELECT COUNT(*) AS total FROM ans.operadoras_cadop"
        row = self._db.fetch_one(sql)
        return int(row["total"]) if row else 0

    def _list_operadoras(self, page: int, limit: int) -> list[dict]:
        """
        Retorna lista paginada de operadoras.

        :param page: Pagina atual.
        :param limit: Itens por pagina.
        :return: Lista de operadoras.
        """
        offset = (page - 1) * limit
        data_params: dict[str, Any] = {"limit": limit, "offset": offset}

        sql = (
            "SELECT cnpj, razao_social, modalidade, uf "
            "FROM ans.operadoras_cadop "
            "ORDER BY razao_social "
            "LIMIT %(limit)s OFFSET %(offset)s"
        )
        return self._db.fetch_all(sql, data_params)

    def list_operadoras(self, page: int, limit: int) -> tuple[list[dict], int]:
        """
        Lista operadoras com paginacao.

        :param page: Pagina atual.
        :param limit: Itens por pagina.
        :return: Tupla (lista, total).
        """
        total = self._count_operadoras()
        rows = self._list_operadoras(page, limit)
        return rows, total

    def get_operadora(self, cnpj: str) -> Optional[dict]:
        """
        Retorna detalhes de uma operadora pelo CNPJ.

        :param cnpj: CNPJ da operadora.
        :return: Dicionario com detalhes ou None.
        """
        cnpj_digits = normalize_cnpj(cnpj)
        sql = (
            "SELECT cnpj, registro_operadora, razao_social, nome_fantasia, "
            "modalidade, logradouro, numero, complemento, bairro, cidade, uf, "
            "cep, ddd, telefone, fax, endereco_eletronico, representante, "
            "cargo_representante, regiao_de_comercializacao, data_registro_ans "
            "FROM ans.operadoras_cadop "
            "WHERE cnpj = %(cnpj)s"
        )
        return self._db.fetch_one(sql, {"cnpj": cnpj_digits})

    def get_despesas(self, cnpj: str) -> list[dict]:
        """
        Retorna historico de despesas da operadora.

        :param cnpj: CNPJ da operadora.
        :return: Lista de despesas por trimestre.
        """
        cnpj_digits = normalize_cnpj(cnpj)
        sql = (
            "SELECT ano, trimestre, valor_despesas "
            "FROM ans.despesas_consolidadas "
            "WHERE cnpj = %(cnpj)s "
            "ORDER BY ano, trimestre"
        )
        return self._db.fetch_all(sql, {"cnpj": cnpj_digits})
