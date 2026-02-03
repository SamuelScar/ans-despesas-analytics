import re
import requests
from bs4 import BeautifulSoup
from typing import List, Dict

BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/"

ZIP_RE = re.compile(r"^([1-4])T(\d{4})\.zip$", re.IGNORECASE)
QUARTER_DIR_RE = re.compile(r"^([1-4])T(\d{4})/?$", re.IGNORECASE)
QUARTER_IN_NAME_RE = re.compile(r"([1-4])T(\d{4})", re.IGNORECASE)


def _list_links(url: str) -> List[str]:
    """
    Busca e retorna todos os links href de uma listagem.

    :param url: URL da listagem.
    :return: Lista de links encontrados.
    """
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    return [a.get("href") for a in soup.find_all("a") if a.get("href")]


def _parse_quarter_from_name(name: str):
    """
    Extrai ano e trimestre de um diretorio como 1T2025.

    :param name: Nome do diretorio.
    :return: Tupla (ano, trimestre) ou None.
    """
    match = QUARTER_DIR_RE.match(name)
    if not match:
        return None
    return int(match.group(2)), int(match.group(1))


def _parse_quarter_from_filename(name: str):
    """
    Extrai ano e trimestre de um arquivo contendo 1T2025.

    :param name: Nome do arquivo.
    :return: Tupla (ano, trimestre) ou None.
    """
    match = QUARTER_IN_NAME_RE.search(name)
    if not match:
        return None
    return int(match.group(2)), int(match.group(1))


def _is_parent_dir(name: str) -> bool:
    """
    Retorna True se for referencia de diretorio pai.

    :param name: Nome do item.
    :return: True se for pai, caso contrario False.
    """
    return name in ("../", "..", "Parent Directory")


def _list_years() -> List[str]:
    """
    Lista anos disponiveis em ordem decrescente.

    :return: Lista de anos.
    """
    root_links = _list_links(BASE_URL)
    years = [
        x.strip("/") for x in root_links if x.endswith("/") and x.strip("/").isdigit()
    ]
    return sorted(years, reverse=True)


def _split_year_links(year_links: List[str]):
    """
    Separa links do ano entre pastas de trimestre e ZIPs.

    :param year_links: Links do ano.
    :return: Tupla (dirs, zips) organizados por trimestre.
    """
    quarter_dirs: Dict[tuple, str] = {}
    quarter_zips: Dict[tuple, List[str]] = {}

    for name in year_links:
        if name.endswith("/"):
            quarter = _parse_quarter_from_name(name.strip("/"))
            if quarter:
                quarter_dirs[quarter] = name
            continue

        if name.lower().endswith(".zip"):
            quarter = _parse_quarter_from_filename(name)
            if quarter:
                quarter_zips.setdefault(quarter, []).append(name)

    return quarter_dirs, quarter_zips


def _sorted_quarters(
    quarter_dirs: Dict[tuple, str], quarter_zips: Dict[tuple, List[str]]
):
    """
    Ordena trimestres do mais recente para o mais antigo.

    :param quarter_dirs: Mapa de pastas por trimestre.
    :param quarter_zips: Mapa de ZIPs por trimestre.
    :return: Lista ordenada de trimestres.
    """
    quarters = set(quarter_dirs.keys()) | set(quarter_zips.keys())
    return sorted(quarters, reverse=True)


def _add_result(
    results: List[Dict],
    seen_urls: set,
    ano: int,
    tri: int,
    filename: str,
    url: str,
) -> bool:
    """
    Adiciona um arquivo ao resultado se ainda nao foi visto.

    :param results: Lista de resultados.
    :param seen_urls: Conjunto de URLs ja vistas.
    :param ano: Ano do arquivo.
    :param tri: Trimestre do arquivo.
    :param filename: Nome do arquivo.
    :param url: URL do arquivo.
    :return: True se adicionou, False caso contrario.
    """
    if url in seen_urls:
        return False
    seen_urls.add(url)
    results.append(
        {
            "ano": ano,
            "trimestre": tri,
            "filename": filename,
            "url": url,
        }
    )
    return True


def _collect_quarter_files(
    ano: int,
    tri: int,
    year_url: str,
    quarter_dirs: Dict[tuple, str],
    quarter_zips: Dict[tuple, List[str]],
    results: List[Dict],
    seen_urls: set,
) -> bool:
    """
    Coleta ZIPs de um trimestre e adiciona ao resultado.

    :param ano: Ano do trimestre.
    :param tri: Numero do trimestre.
    :param year_url: URL base do ano.
    :param quarter_dirs: Pastas por trimestre.
    :param quarter_zips: ZIPs por trimestre.
    :param results: Lista de resultados.
    :param seen_urls: Conjunto de URLs ja vistas.
    :return: True se encontrou arquivos, False caso contrario.
    """
    quarter_has_files = False

    for filename in quarter_zips.get((ano, tri), []):
        url = f"{year_url}{filename}"
        if _add_result(results, seen_urls, ano, tri, filename, url):
            quarter_has_files = True

    dir_name = quarter_dirs.get((ano, tri))
    if dir_name:
        quarter_url = f"{year_url}{dir_name}"
        for item in _list_links(quarter_url):
            if _is_parent_dir(item) or not item.lower().endswith(".zip"):
                continue
            url = f"{quarter_url}{item}"
            if _add_result(results, seen_urls, ano, tri, item, url):
                quarter_has_files = True

    return quarter_has_files


def get_last_trimesters(limit: int = 3) -> List[Dict]:
    """
    Descobre os ultimos N trimestres e retorna os ZIPs encontrados.

    :param limit: Quantidade de trimestres.
    :return: Lista de itens com ano, trimestre, arquivo e URL.
    """
    years = _list_years()
    results: List[Dict] = []
    seen_urls: set = set()
    found_quarters: set = set()

    for year in years:
        year_url = f"{BASE_URL}{year}/"
        year_links = [x for x in _list_links(year_url) if not _is_parent_dir(x)]
        quarter_dirs, quarter_zips = _split_year_links(year_links)

        for ano, tri in _sorted_quarters(quarter_dirs, quarter_zips):
            if (ano, tri) in found_quarters:
                continue

            if _collect_quarter_files(
                ano,
                tri,
                year_url,
                quarter_dirs,
                quarter_zips,
                results,
                seen_urls,
            ):
                found_quarters.add((ano, tri))
            if len(found_quarters) == limit:
                return results

    return results


if __name__ == "__main__":
    for item in get_last_trimesters(3):
        print(f"{item['ano']} - {item['trimestre']}T -> {item['url']}")
