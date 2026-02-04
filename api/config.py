from dataclasses import dataclass
import os
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / ".env"

if not ENV_PATH.exists():
    raise RuntimeError("Arquivo .env obrigatorio nao encontrado na raiz do projeto.")

load_dotenv(ENV_PATH, override=True)


@dataclass(frozen=True)
class Settings:
    """
    Configuracoes basicas da aplicacao.
    """

    db_host: str = os.environ["DB_HOST"]
    db_port: int = int(os.environ["DB_PORT"])
    db_name: str = os.environ["DB_NAME"]
    db_user: str = os.environ["DB_USER"]
    db_password: str = os.environ["DB_PASSWORD"]
    stats_cache_ttl: int = int(os.environ["STATS_CACHE_TTL"])
