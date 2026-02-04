from .config import Settings
from .db import Database
from .repositories.estatisticas import EstatisticasRepository
from .repositories.operadoras import OperadorasRepository
from .services.estatisticas_service import EstatisticasService
from .services.operadoras_service import OperadorasService


class Container:
    """
    Centraliza instancias de configuracao, banco e servicos.
    """

    def __init__(self) -> None:
        self.settings = Settings()
        self.db = Database(self.settings)
        self.operadoras_repo = OperadorasRepository(self.db)
        self.estatisticas_repo = EstatisticasRepository(self.db)
        self.operadoras_service = OperadorasService(self.operadoras_repo)
        self.estatisticas_service = EstatisticasService(
            self.estatisticas_repo, cache_ttl=self.settings.stats_cache_ttl
        )


container = Container()
