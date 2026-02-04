from contextlib import contextmanager
from typing import Any, Iterator, Mapping, Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool

from .config import Settings


class Database:
    """
    Gerencia conexoes com Postgres usando pool.
    """

    def __init__(self, settings: Settings) -> None:
        self._pool = SimpleConnectionPool(
            minconn=1,
            maxconn=5,
            host=settings.db_host,
            port=settings.db_port,
            dbname=settings.db_name,
            user=settings.db_user,
            password=settings.db_password,
        )

    @contextmanager
    def connection(self) -> Iterator["psycopg2.extensions.connection"]:
        """
        Fornece uma conexao do pool.

        :return: Conexao do pool.
        """
        conn = self._pool.getconn()
        try:
            yield conn
        finally:
            self._pool.putconn(conn)

    def fetch_all(
        self, query: str, params: Optional[Mapping[str, Any]] = None
    ) -> list[dict]:
        """
        Executa um SELECT e retorna todas as linhas.

        :param query: SQL a executar.
        :param params: Parametros do SQL.
        :return: Lista de linhas como dict.
        """
        with self.connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, params or {})
                rows = cur.fetchall()
        return [dict(row) for row in rows]

    def fetch_one(
        self, query: str, params: Optional[Mapping[str, Any]] = None
    ) -> Optional[dict]:
        """
        Executa um SELECT e retorna uma unica linha.

        :param query: SQL a executar.
        :param params: Parametros do SQL.
        :return: Linha unica ou None.
        """
        with self.connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, params or {})
                row = cur.fetchone()
        return dict(row) if row else None

    def execute(
        self, query: str, params: Optional[Mapping[str, Any]] = None
    ) -> None:
        """
        Executa um comando que altera dados.

        :param query: SQL a executar.
        :param params: Parametros do SQL.
        :return: None.
        """
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params or {})
            conn.commit()

    def close(self) -> None:
        """
        Encerra todas as conexoes do pool.

        :return: None.
        """
        self._pool.closeall()
