import logging
import shutil
import sys
import zipfile
from pathlib import Path
from typing import Optional
import os
import stat
import time

import requests
from bs4 import BeautifulSoup
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from etl.fetch.ans_downloader import download_items, extract_downloaded
from etl.fetch.ans_indexer import get_last_trimesters
from etl.process.aggregate_despesas import aggregate
from etl.process.consolidate_despesas import consolidate
from etl.process.enrich_consolidado import enrich
from etl.process.validate_consolidado import validate


ZIP_NAME = "Teste_Samuel_de_Souza.zip"
DATA_DIR = Path("data")
TMP_DIR = DATA_DIR / "tmp"
RAW_DIR = TMP_DIR / "raw"
EXTRACT_DIR = TMP_DIR / "extracted"
INTER_DIR = TMP_DIR / "intermediate"
TMP_MARKER = TMP_DIR / ".pipeline_tmp"
LOG_DIR = Path("logs")
OUTPUT_DIR = DATA_DIR / "output"

CADOP_BASE_URL = (
    "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/"
)
CADOP_FILE_NAME = "Relatorio_cadop.csv"


def _setup_logger() -> logging.Logger:
    """
    Cria um logger em arquivo com timestamp para a pipeline.

    :return: Logger configurado para a pipeline.
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    logger = logging.getLogger("pipeline")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    file_handler = logging.FileHandler(
        LOG_DIR / f"pipeline_{timestamp}.log", encoding="utf-8"
    )
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger


def _list_links(url: str) -> list[str]:
    """
    Retorna todos os links href de uma listagem de diretorio.

    :param url: URL da listagem.
    :return: Lista de links encontrados.
    """
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    return [a.get("href") for a in soup.find_all("a") if a.get("href")]


def _resolve_cadop_url(logger: logging.Logger) -> Optional[str]:
    """
    Resolve a URL do CSV CADOP a partir da listagem da ANS.

    :param logger: Logger da pipeline.
    :return: URL do CSV CADOP ou None se nao encontrado.
    """
    try:
        links = _list_links(CADOP_BASE_URL)
    except Exception as exc:
        logger.error("Falha ao listar CADOP: %s", exc)
        return None
    for link in links:
        if link == CADOP_FILE_NAME:
            return f"{CADOP_BASE_URL}{link}"
    for link in links:
        if link.lower().endswith(".csv"):
            return f"{CADOP_BASE_URL}{link}"
    return None


def _download_file(url: str, dest: Path) -> None:
    """
    Baixa um arquivo para disco usando streaming.

    :param url: URL do arquivo.
    :param dest: Caminho de destino local.
    :return: None.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=60) as response:
        response.raise_for_status()
        with open(dest, "wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    handle.write(chunk)


def _zip_output(output_file: Path, zip_name: str) -> Path:
    """
    Compacta um unico arquivo dentro da pasta de saida.

    :param output_file: Arquivo a compactar.
    :param zip_name: Nome do ZIP de saida.
    :return: Caminho do ZIP gerado.
    """
    zip_path = OUTPUT_DIR / zip_name
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.write(output_file, arcname=output_file.name)
    return zip_path


def _handle_remove_readonly(func, path, _exc_info):
    """
    Trata arquivos somente leitura durante a limpeza temporaria.

    :param func: Funcao de remocao original.
    :param path: Caminho do arquivo/diretorio.
    :param _exc_info: Dados da excecao.
    :return: None.
    """
    try:
        os.chmod(path, stat.S_IWRITE)
        func(path)
    except Exception:
        pass


def _cleanup_tmp(logger: Optional[logging.Logger] = None) -> None:
    """
    Remove o diretorio temporario com tentativas para locks no Windows.

    :param logger: Logger opcional para avisos.
    :return: None.
    """
    if not TMP_DIR.exists():
        return
    for _ in range(3):
        try:
            shutil.rmtree(TMP_DIR, onerror=_handle_remove_readonly)
        except Exception as exc:
            if logger:
                logger.warning("Falha ao remover pasta temporaria: %s", exc)
        if not TMP_DIR.exists():
            return
        try:
            TMP_DIR.rmdir()
        except Exception:
            time.sleep(0.5)


def _ensure_cadop(logger: logging.Logger) -> Path:
    """
    Garante o CSV CADOP local, baixando se necessario.

    :param logger: Logger da pipeline.
    :return: Caminho local do CADOP.
    """
    cadop_path = INTER_DIR / CADOP_FILE_NAME
    if cadop_path.exists():
        return cadop_path
    cadop_url = _resolve_cadop_url(logger)
    if not cadop_url:
        raise RuntimeError("Nao foi possivel localizar o arquivo CADOP.")
    logger.info("Baixando CADOP: %s", cadop_url)
    _download_file(cadop_url, cadop_path)
    return cadop_path


def _prepare_directories(logger: logging.Logger) -> None:
    """
    Prepara pastas temporarias e de saida para uma execucao limpa.

    :param logger: Logger da pipeline.
    :return: None.
    """
    _cleanup_tmp(logger)
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    TMP_MARKER.write_text("temp files for pipeline", encoding="utf-8")
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    EXTRACT_DIR.mkdir(parents=True, exist_ok=True)
    INTER_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def _download_trimesters(logger: logging.Logger) -> list[dict]:
    """
    Identifica os ultimos trimestres a processar.

    :param logger: Logger da pipeline.
    :return: Lista de itens de trimestre.
    """
    logger.info("Buscando ultimos 3 trimestres")
    trimestre_items = get_last_trimesters(3)
    logger.info("Trimestres identificados: %s", len(trimestre_items))
    return trimestre_items


def _download_zips(logger: logging.Logger, trimestre_items: list[dict]) -> list[Path]:
    """
    Baixa os ZIPs dos trimestres selecionados.

    :param logger: Logger da pipeline.
    :param trimestre_items: Itens de trimestre.
    :return: Lista de caminhos baixados.
    """
    logger.info("Baixando ZIPs")
    downloaded_paths = download_items(trimestre_items, raw_dir=RAW_DIR)
    logger.info("ZIPs baixados: %s", len(downloaded_paths))
    return downloaded_paths


def _extract_zips(logger: logging.Logger, trimestre_items: list[dict]) -> list[Path]:
    """
    Extrai os ZIPs baixados para a pasta temporaria.

    :param logger: Logger da pipeline.
    :param trimestre_items: Itens de trimestre.
    :return: Lista de pastas extraidas.
    """
    logger.info("Extraindo ZIPs")
    extracted_dirs = extract_downloaded(
        trimestre_items, raw_dir=RAW_DIR, extract_dir=EXTRACT_DIR
    )
    logger.info("Pastas extraidas: %s", len(extracted_dirs))
    return extracted_dirs


def _consolidate(logger: logging.Logger, cadop_path: Path) -> Path:
    """
    Consolida os dados dos trimestres em um unico CSV.

    :param logger: Logger da pipeline.
    :param cadop_path: Caminho do CADOP local.
    :return: Caminho do CSV consolidado.
    """
    logger.info("Consolidando dados")
    consolidado_path = INTER_DIR / "consolidado_despesas.csv"
    consolidate(
        extract_dir=EXTRACT_DIR,
        cadop_path=cadop_path,
        output_file=consolidado_path,
        limit_quarters=3,
    )
    return consolidado_path


def _validate(logger: logging.Logger, consolidado_path: Path) -> tuple[Path, Path]:
    """
    Valida o consolidado e retorna os caminhos de saida.

    :param logger: Logger da pipeline.
    :param consolidado_path: Caminho do CSV consolidado.
    :return: Tupla (validos, inconsistencias).
    """
    logger.info("Validando dados")
    valid_path = INTER_DIR / "consolidado_validado.csv"
    invalid_path = INTER_DIR / "inconsistencias_2_1.csv"
    validate(
        input_file=consolidado_path,
        valid_file=valid_path,
        invalid_file=invalid_path,
    )
    if invalid_path.exists():
        invalid_count = max(
            sum(1 for _ in invalid_path.open(encoding="utf-8-sig")) - 1, 0
        )
        logger.info("Inconsistencias validacao: %s", invalid_count)
    return valid_path, invalid_path


def _enrich(
    logger: logging.Logger, valid_path: Path, cadop_path: Path
) -> tuple[Path, Path]:
    """
    Enriquece o consolidado validado com campos do CADOP.

    :param logger: Logger da pipeline.
    :param valid_path: Caminho do CSV validado.
    :param cadop_path: Caminho do CADOP local.
    :return: Tupla (enriquecido, inconsistencias).
    """
    logger.info("Enriquecendo dados")
    enriched_path = INTER_DIR / "consolidado_enriquecido.csv"
    missing_path = INTER_DIR / "inconsistencias_2_2.csv"
    enrich(
        input_file=valid_path,
        cadop_path=cadop_path,
        output_file=enriched_path,
        missing_file=missing_path,
    )
    if missing_path.exists():
        missing_count = max(
            sum(1 for _ in missing_path.open(encoding="utf-8-sig")) - 1, 0
        )
        logger.info("Inconsistencias cadastro: %s", missing_count)
    return enriched_path, missing_path


def _aggregate(logger: logging.Logger, enriched_path: Path) -> Path:
    """
    Agrega despesas por RazaoSocial e UF.

    :param logger: Logger da pipeline.
    :param enriched_path: Caminho do CSV enriquecido.
    :return: Caminho do CSV agregado.
    """
    logger.info("Agregando dados")
    aggregated_path = OUTPUT_DIR / "despesas_agregadas.csv"
    aggregate(input_file=enriched_path, output_file=aggregated_path)
    return aggregated_path


def _finalize_outputs(consolidado_path: Path, aggregated_path: Path) -> None:
    """
    Cria os ZIPs finais exigidos no desafio.

    :param consolidado_path: Caminho do CSV consolidado.
    :param aggregated_path: Caminho do CSV agregado.
    :return: None.
    """
    _zip_output(consolidado_path, "consolidado_despesas.zip")
    _zip_output(aggregated_path, ZIP_NAME)


def run_pipeline() -> None:
    """
    Executa a pipeline completa do download ate a agregacao.

    :return: None.
    """
    print("PROCESSANDO...")
    logger = _setup_logger()
    logger.info("Iniciando pipeline")

    _prepare_directories(logger)

    try:
        trimestre_items = _download_trimesters(logger)
        _download_zips(logger, trimestre_items)
        _extract_zips(logger, trimestre_items)

        cadop_path = _ensure_cadop(logger)
        consolidado_path = _consolidate(logger, cadop_path)
        valid_path, _ = _validate(logger, consolidado_path)
        enriched_path, _ = _enrich(logger, valid_path, cadop_path)
        aggregated_path = _aggregate(logger, enriched_path)
        logger.info("Gerando ZIP final")
        _finalize_outputs(consolidado_path, aggregated_path)
        logger.info("Pipeline finalizado com sucesso")
        print("FINALIZADO")
    finally:
        _cleanup_tmp(logger)


if __name__ == "__main__":
    run_pipeline()
