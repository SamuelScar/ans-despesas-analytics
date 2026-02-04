from ..db import Database


class EstatisticasRepository:
    """
    Acesso a dados agregados para estatisticas.
    """

    def __init__(self, db: Database) -> None:
        self._db = db

    def get_totais(self) -> dict:
        """
        Retorna total e media geral de despesas.

        :return: Dicionario com total e media.
        """
        sql = (
            "SELECT COALESCE(SUM(valor_despesas), 0) AS total, "
            "COALESCE(AVG(valor_despesas), 0) AS media "
            "FROM ans.despesas_consolidadas"
        )
        row = self._db.fetch_one(sql)
        return row or {"total": 0, "media": 0}

    def get_top_operadoras(self) -> list[dict]:
        """
        Retorna top 5 operadoras por total de despesas.

        :return: Lista com top operadoras.
        """
        sql = (
            "SELECT d.cnpj, "
            "COALESCE(c.razao_social, d.razao_social) AS razao_social, "
            "SUM(d.valor_despesas) AS total_despesas "
            "FROM ans.despesas_consolidadas d "
            "LEFT JOIN ans.operadoras_cadop c ON c.cnpj = d.cnpj "
            "GROUP BY d.cnpj, COALESCE(c.razao_social, d.razao_social) "
            "ORDER BY total_despesas DESC "
            "LIMIT 5"
        )
        return self._db.fetch_all(sql)
