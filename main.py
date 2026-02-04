from fastapi import FastAPI

from api.container import container
from api.routers.estatisticas import router as estatisticas_router
from api.routers.operadoras import router as operadoras_router


app = FastAPI(
    title="ANS Despesas API",
    description="API para consulta de operadoras e despesas da ANS.",
    version="1.0.0",
)

app.include_router(operadoras_router)
app.include_router(estatisticas_router)


@app.get("/health")
def health_check() -> dict:
    """
    Endpoint simples de healthcheck.
    """
    return {"status": "ok"}


@app.on_event("shutdown")
def shutdown() -> None:
    """
    Encerra o pool de conexoes ao finalizar a aplicacao.

    :return: None.
    """
    container.db.close()
