import time

from ..repositories.estatisticas import EstatisticasRepository


class EstatisticasService:
    """
    Regras de negocio para estatisticas, com cache simples em memoria.
    """

    def __init__(self, repo: EstatisticasRepository, cache_ttl: int = 300) -> None:
        self._repo = repo
        self._cache_ttl = cache_ttl
        self._cache_data: dict | None = None
        self._cache_expires_at: float = 0.0

    def get_estatisticas(self) -> tuple[dict, bool]:
        """
        Retorna estatisticas agregadas com cache por TTL.

        :return: Tupla (estatisticas, cache_hit).
        """
        now = time.time()
        if self._cache_data and now < self._cache_expires_at:
            return self._cache_data, True

        totals = self._repo.get_totais()
        top = self._repo.get_top_operadoras()

        data = {
            "total_despesas": float(totals.get("total", 0)),
            "media_despesas": float(totals.get("media", 0)),
            "top_operadoras": [
                {
                    "cnpj": row["cnpj"],
                    "razao_social": row.get("razao_social"),
                    "total_despesas": float(row["total_despesas"]),
                }
                for row in top
            ],
        }

        self._cache_data = data
        self._cache_expires_at = now + self._cache_ttl
        return data, False
